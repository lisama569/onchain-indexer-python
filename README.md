# onchain-indexer-python

基于 FastAPI 的链上数据索引服务。使用本地固定 JSON 数据模拟链上数据源，
在**链重组、重复投递、乱序到达与并发写入**下保证索引结果一致且可解释。

近期新增面向**长期运行与排障**的三项能力：

- **规范链切换审计**：每次重组切换都落一条可倒序查询的记录，与状态切换
  同事务同生共死（切换失败不留痕、重复投递不重复记账）。
- **只读自检**：`GET /health` 分项给出自洽性结论，把问题定位到具体的
  块/交易；只读、且在一致性快照上执行，读不到改了一半的中间状态。
- **整批原子摄取**：`POST /ingest/blocks/atomic` 一批区块要么全部生效、
  要么整体回滚；等锁有上限，超时按可重试故障分类返回。

## 架构

```
main.py              FastAPI 入口与路由（create_app 工厂便于测试注入配置）
indexer/
  config.py          IndexerConfig：max_reorg_depth / genesis_hash /
                     db_path / lock_timeout
  models.py          Block / Transaction / BlockStatus / IngestResult /
                     ReorgAuditRecord / BatchIngestResult / StoreSnapshot
  errors.py          失败分类：malformed_payload / source_unavailable /
                     state_conflict / reorg_too_deep / lock_timeout /
                     storage_error
  store.py           SQLite 存储（区块、交易、状态、进度、ever_canonical
                     历史标记、重组审计），写锁 + 事务（保存点嵌套、
                     锁等待超时、一致性快照）
  topology.py        classify_blocks：顺序无关的确定性三态拓扑判定，
                     service/health/查询共用同一口径
  service.py         摄取、全量拓扑重算、规范链维护、重组、审计、整批原子摄取
  health.py          HealthChecker：只读分项自检（含三态口径与审计对账）
  source.py          LocalChainSource：本地固定 JSON 数据源
tests/               重组/重复/乱序/并发/畸形/回退 + 审计/自检/整批原子测试
```

## 摄取协议

### HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/ingest/block` | 摄取单个区块，成功返回 `{status, applied, reorg_depth}` |
| POST | `/ingest/blocks` | 批量摄取，**逐块独立事务**，逐块返回结果或分类错误（不因一块失败而回滚其它块） |
| POST | `/ingest/blocks/atomic` | **整批原子摄取**，一批全有或全无；批内重复按幂等处理 |
| POST | `/ingest/from-source` | 从本地数据源（`INDEXER_SOURCE_PATH`）按高度顺序拉取并摄取 |
| GET | `/blocks?status=canonical\|reorged\|staged` | 按状态查询区块（含交易） |
| GET | `/blocks/{block_hash}` | 区块详情 |
| GET | `/chain/canonical` | 当前规范链（哈希按高度升序） |
| GET | `/progress` | 摄取进度（规范链最高高度） |
| GET | `/reorgs/audits?limit=&offset=` | 规范链切换审计，**按时间倒序**，可分页 |
| GET | `/health` | 只读自检，返回分项结论与可定位的问题清单 |

区块负载：

```json
{
  "block_hash": "a1", "parent_hash": "g0", "height": 1, "timestamp": 1700000001,
  "transactions": [{"tx_hash": "t1", "sender": "alice", "recipient": "bob",
                    "amount": 5, "nonce": 0}]
}
```

### 摄取规则

- **幂等**：同一区块（哈希与内容完全一致）重复投递返回 `applied=false`，
  不产生重复记录；同哈希不同内容属于状态冲突，拒绝且不覆盖原记录。
- **乱序与暂存**：父块缺失（或与父块高度不连续）的区块以 `staged` 状态
  保留（不丢弃、不进入规范数据）；父块到位、整棵子树连通到创世块后自动
  参与统一拓扑判定。**已连通到创世块但输掉竞争的分叉块是 `reorged`**，
  不是 `staged`——三态是只取决于最终区块集合的结构结论，不随投递时机变化。
- **校验**：负高度/时间戳/金额/nonce、空哈希、区块内交易重复、交易归属
  与区块不一致、高度与父块不连续、创世块哈希与配置不符，均按畸形数据拒绝。

## 规范链与重组规则

- 规范链始终从**已连通到创世块**的链中，按确定性规则选出唯一一条：
  **链尖高度最高；同高时取链尖区块哈希字典序最小**。规则只依赖「最终
  已落库的区块集合」，与区块的投递顺序、并发度无关，因此同一份数据无论
  怎么乱序、重复、并发投递，最终结论完全一致、可复现。未连通到创世块的
  链不会被提升。
- 区块状态三分类，查询可区分，且**顺序无关、口径唯一**：
  - `canonical`：当前规范链上的区块；
  - `reorged`：祖先链已连通到创世块、但不在规范链上的分叉块。它内部有
    两个可复查的**历史**细分（当前三态相同，靠 `ever_canonical` 标记与
    重组审计区分，二者严格对账、永不矛盾）：
    - **曾被换下去**：曾经进入过规范链（`ever_canonical=1`），后在某次
      切换中被换下——必然能在某条审计的 `replaced_blocks` 里找到，且其
      最后一次审计事件是「被换下」；
    - **从未进入规范链**：连通到创世块时竞争已经落败，从未上过规范链
      （`ever_canonical=0`）——从不出现在任何 `replaced/readopted` 清单中；
  - `staged`：祖先链**无法**连通到创世块的块（父块缺失，或与父块高度不
    连续）。注意：「已连通但输掉竞争」的块一律是 `reorged` 而非 `staged`，
    这正是此前乱序/并发下归类时红时绿的根因，现已收敛为结构判定。
- 实现、查询、自检、审计共用同一份确定性拓扑判定
  （`indexer/topology.py::classify_blocks`）：每次写入后在全量区块集合上
  重算三态；自检的 `status_classification` 分项会把存储状态与这份口径
  逐项比对，任何不一致都会点名具体区块。
- 重组触发时，被替换的规范区块在**同一事务**内标记为 `reorged`，新链区块
  标记为 `canonical`，并推进进度；任一步失败整体回滚，无部分应用。
- **重组深度上限**：`max_reorg_depth`（默认 6）。胜出所需替换深度超过上限时，
  该次摄取被明确拒绝（`reorg_too_deep`），区块不落库、规范链与进度不变。

## 进度与并发

- 进度 = 规范链最高高度，持久化于 SQLite，**只能单调前进**；写回退进度
  返回 `state_conflict`；同值重放为幂等空操作。
- 所有多步写入包裹在 `BEGIN IMMEDIATE` 事务中，并由进程内写锁串行化；
  并发摄取下每个区块恰好应用一次，重复投递幂等返回。

## 规范链切换审计

每次**真正的**规范链切换（某条分叉取代当前链尖）都会在状态切换的**同一事务**
内写一条 `reorg_audits` 记录，字段含义：

| 字段 | 含义 |
| --- | --- |
| `id` / `created_at_ns` | 自增主键与纳秒产生时间；列表按 `created_at_ns DESC, id DESC` 倒序 |
| `trigger_block_hash` | 触发本次切换的区块 |
| `fork_height` / `depth` | 分叉点（共同祖先）高度与切换深度（旧链被替换的区块数） |
| `old_tip` / `new_tip` | 切换前/后的链尖哈希与高度 |
| `replaced_blocks` | 本次被换下去的块（切换前 CANONICAL、切换后 REORGED，高度升序） |
| `readopted_blocks` | 本次重新回到规范链的块（切换前非规范、切换后 CANONICAL，高度升序；触发块即新链尖不计入）。仅当获胜段建在**曾经上过规范链**的回归链上时非空；全新分叉首次胜出时为空 |

一致性保证：

- **与状态切换同生共死**：审计与区块状态翻转、进度推进在一个事务里；切换
  因重组过深或任一步失败而回滚时，审计也一并回滚，不会留下「无对应切换」的记录。
- **重复投递不凭空记账**：初始建链、规范链正常延伸、**落败分叉连通**、
  同一区块/同一数据集重复投递都不构成切换，不产生新记录（幂等返回
  `applied=false`）。只有「旧规范块被真正换下」才落一条记录。
- **与最终三态严格对账**：按时间正序回放每条审计的两份清单，每个块「最后
  一次被提到」的方向必须与其最终状态一致——最后是 `replaced` 的块最终必为
  `reorged`，最后是 `readopted` 的块最终必为 `canonical`；同一条记录里
  同一块不会既被换下又被接纳。自检的 `audit_reconciliation` 分项持续守护
  这套对账，任何「审计与最终状态各执一词」的块都会被点名。
- 「曾被换下去」的块一定在某条 `replaced_blocks` 中且 `ever_canonical=1`；
  「从未进入规范链」的连通落败块 `ever_canonical=0`、从不出现在任何清单中。
- 记录**只追加**，通过 `GET /reorgs/audits?limit=&offset=` 按时间倒序翻查。

```bash
curl -s http://127.0.0.1:8000/reorgs/audits?limit=20 | jq
```

## 只读自检

`GET /health` 对当前索引做分项一致性检查，返回：

```json
{
  "ok": false,
  "checks": [
    {"name": "canonical_continuity", "ok": false, "issues": [
      {"check": "canonical_continuity",
       "message": "规范链断链: a3 的 parent 是 a2，期望 a1",
       "block_hashes": ["a3"], "tx_hashes": []}]}
  ]
}
```

六个分项：

| 检查项 | 检查内容 |
| --- | --- |
| `canonical_continuity` | 规范链是否从创世起逐块 parent/height 连续、同高度是否有多个规范块 |
| `block_parent_consistency` | 每个块的状态是否与父链对得上（规范块父块必须规范、缺父块只能是 staged、高度须相邻） |
| `canonical_txs` | 规范链交易是否归属正确、有无重复、有无悬空（归属块不存在） |
| `progress` | 记录进度是否与当前规范链尖高度一致 |
| `status_classification` | 存储三态是否与顺序无关的统一拓扑口径逐项一致；`ever_canonical` 历史标记是否自洽 |
| `audit_reconciliation` | 审计 `replaced/readopted` 清单正序回放后是否与最终三态对得上，有无互相矛盾的块 |

有问题时不是只回一个对/错，而是在 `issues` 里点名**具体的 `block_hashes` /
`tx_hashes`** 及可读说明。检查全程**只读**：

- 不执行任何写入，重复调用不改变数据（有 `test_health_is_read_only` 守护）。
- 快照在与写入事务**互斥的存储锁**内一次性取齐区块、交易、进度、审计四张表，
  因此并发摄取过程中调用自检，只会看到写前或写后的完整状态，读不到改了一半
  的中间状态。`test_health_never_reads_half_written_state` 用**真实生效的
  连续链写入**（每块都提交成功、断言写入数量，杜绝「写入全被拒的空转」）与
  自检线程长时间并发，断言每份报告六项全过、观察到的进度只可能是某个已提交
  完整前缀（写前或写后），并确实观察到了写入后的最终状态。
- HTTP 始终返回 200，由响应体 `ok` 区分自洽与否（自检动作本身成功执行）。

## 整批原子摄取

`POST /ingest/blocks/atomic` 与原 `POST /ingest/blocks` 的语义区别：

| 维度 | `/ingest/blocks`（保留，逐块） | `/ingest/blocks/atomic`（新增，整批） |
| --- | --- | --- |
| 事务边界 | 每个区块各自一个事务 | 整批共用**一个事务** |
| 一块被拒 | 其它块照常落库，逐项返回 error | **整批回滚，不留任何数据**（含已暂存的前半批） |
| 返回 | 200 + 逐项成功/错误混合 | 成功返回 `{applied, duplicates, results}`；任一被拒则整批返回对应 4xx/5xx |
| 批内重复 | 幂等 | 同样幂等，计入 `duplicates`、`applied=false`，**不算失败** |
| 等锁 | 无显式上限 | 等待写锁超过 `lock_timeout` 抛 `lock_timeout`（503，可重试） |

适用场景：希望「一个投递单元整体生效」、不能接受部分应用的批量同步任务。
原逐块接口与逐块服务方法 `ingest_block` 行为不变，可继续用于容忍部分失败、
需要逐项结果的场景。

底层数据库异常统一包装为 `storage_error`（500），不会把 SQLite 原生异常直接
返回给调用方；等锁超时单独归类为 `lock_timeout`（503，可重试），二者可区分。

## 错误分类

| category | HTTP | 含义 | 处理方式 |
| --- | --- | --- | --- |
| `malformed_payload` | 400 | 负载畸形、校验失败 | 修复数据后重投 |
| `source_unavailable` | 503 | 本地数据源暂时不可读 | 可重试 |
| `state_conflict` | 409 | 与已持久化状态矛盾（内容冲突、进度回退） | 检查调用方状态 |
| `reorg_too_deep` | 422 | 重组深度超过 `max_reorg_depth` | 需人工介入确认 |
| `lock_timeout` | 503 | 整批提交等待写锁超过 `lock_timeout` | 可重试的临时故障 |
| `storage_error` | 500 | 底层存储故障（已包装，不外泄原生异常） | 排查存储后重试 |

错误响应体：`{"error": {"category", "message", "detail"}}`。

## 配置项

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `INDEXER_MAX_REORG_DEPTH` | `6` | 允许的最大重组深度 |
| `INDEXER_GENESIS_HASH` | `""`（不校验） | 创世块哈希，非空时校验高度 0 区块 |
| `INDEXER_DB_PATH` | `:memory:` | SQLite 路径，设为文件路径即持久化 |
| `INDEXER_SOURCE_PATH` | 未设置 | 本地数据源 JSON 文件路径 |
| `INDEXER_LOCK_TIMEOUT` | `5` | 整批提交等待写锁的上限（秒），超时返回 `lock_timeout` |

## 兼容性

- **已有数据**：`reorg_audits` 表与 `blocks.ever_canonical` 列均通过
  幂等 DDL（`CREATE TABLE IF NOT EXISTS` / `ALTER TABLE ... ADD COLUMN`，
  已存在则跳过）在打开旧库时自动补齐，不改动、不删除已有
  `blocks`/`txs`/`progress` 数据。升级前产生的历史切换没有审计记录，
  旧块的 `ever_canonical` 默认为 0；自检不会因此误报。
- **原有接口**：`/ingest/block`、`/ingest/blocks`、`/ingest/from-source`、
  `/blocks`、`/chain/canonical`、`/progress` 的路径、请求/响应结构与
  查询语义全部保持不变；`/health`、`/reorgs/audits` 的既有字段不变
  （`/health` 的 `checks` 为纯追加两个分项）。
- **三态语义收敛**：`canonical/reorged/staged` 三个枚举值与查询入口不变；
  变化的只是 `reorged` 与 `staged` 的**判定口径**——从「依赖投递时机」收敛
  为「只取决于最终区块集合」的结构判定：已连通到创世块但输掉竞争的分叉块，
  此前在某些投递顺序下会被记为 `staged`，此后一律确定为 `reorged`。其
  「曾被换下去 / 从未进入规范链」的历史区分由 `ever_canonical` 标记与
  审计清单承担，接口不新增字段即可复查。

## 运行与验证

```bash
uv sync                                   # 安装依赖
uv run uvicorn main:app --reload          # 启动服务
uv run pytest tests/ -v                   # 运行测试（日志打印每个用例的输入与判定依据）
```

本地手工验证：

```bash
# 1) 整批原子摄取主链
curl -s -X POST localhost:8000/ingest/blocks/atomic -H 'content-type: application/json' \
  -d '{"blocks":[ ... ]}'
# 2) 触发一次重组后，按时间倒序查看切换审计（replaced/readopted 两份清单）
curl -s localhost:8000/reorgs/audits | jq
# 3) 随时做只读自检（六个分项 ok 与问题清单）
curl -s localhost:8000/health | jq
```

### 本地复现稳定性结论的步骤

```bash
# A) 全套测试一次性运行（70 个用例），日志含输入与判定依据
uv run pytest tests/ -v

# B) 把既有重组/乱序/并发/进度回退用例连着多轮复跑（下面为 20 轮），
#    确认最终三态不再时好时坏：
for i in $(seq 1 20); do
  uv run pytest tests/test_concurrency.py tests/test_ingest.py \
                tests/test_errors.py tests/test_audit.py -q | tail -1
done

# C) 顺序无关 + 多随机种子/多并发度（10 种子 × 1/4/16 线程 = 30 个参数组合）：
uv run pytest tests/test_determinism.py -v

# D) 写入进行中自检始终自洽（真实生效写入并发，非空转）：
uv run pytest tests/test_health.py::test_health_never_reads_half_written_state -v
```

期望结果：A 全部通过；B 每轮都为「全部 passed」，并发用例中输掉的 a 链
稳定归类为 `reorged`；C 每种种子/线程数下规范链均为 `g0,b1..b4`、
`a1..a3` 均为 `reorged`；D 日志显示 40 块全部生效、期间每份自检报告
`ok=true`，且观察到的进度是从 3 到 43 的若干完整状态。

测试覆盖：在既有的乱序/重复投递、重组切换与状态区分、过深重组拒绝、畸形
负载、进度回退、并发摄取不重不漏、数据源不可用与畸形分类之外，新增/加固：

- `tests/test_determinism.py`：多种投递顺序下最终三态严格一致；10 随机
  种子 × 3 种并发度的乱序重复投递结论稳定可复现；「曾被换下去」与「从未
  进入规范链」在状态、`ever_canonical`、审计清单三处口径一致；人为制造
  审计/状态矛盾时自检 `audit_reconciliation` 点名具体块。
- `tests/test_audit.py`：切换审计字段如实、初始建链/正常延伸/落败分叉连通
  不记账、切换失败（过深拒绝 / 审计写入注入失败）整体回滚不留记录、重复
  投递不重复记账、分叉反超回归时 `readopted_blocks` 如实反映、倒序分页。
- `tests/test_health.py`：健康索引六项全过；人为破坏连续性、进度、交易
  归属时点名具体块/交易；自检只读；**真实生效写入并发**期间每份自检都自洽、
  只能看到写前/写后完整状态（断言写入数量，杜绝空转）。
- `tests/test_batch.py`：整批原子摄取在畸形块/重组过深时整体不留数据、
  整批合法一次生效、批内重复幂等计入 duplicates、等锁超时返回可区分可重试
  的 `lock_timeout` 且不卡死。
- `tests/test_source_api.py`：三个 HTTP 端点的端到端行为。

测试日志通过 `log_cli` 输出每个用例的输入与判定依据。

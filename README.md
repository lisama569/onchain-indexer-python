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
  classification.py  三态判定唯一口径：确定性参考重放（与投递顺序无关）+
                     单调补判收敛；实现、自检共用
  store.py           SQLite 存储（区块、交易、状态、进度、重组审计），
                     写锁 + 事务（支持保存点嵌套、锁等待超时、一致性快照）
  service.py         摄取、暂存连接、规范链维护、重组、审计、整批原子摄取、
                     摄取后按参考口径单调收敛三态
  health.py          HealthChecker：只读分项自检（含三态归类口径复查）
  source.py          LocalChainSource：本地固定 JSON 数据源
tests/               重组/重复/乱序/并发/畸形/回退 + 审计/自检/整批原子 +
                     稳定性与口径对账测试
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
- **乱序与暂存**：父块缺失的区块以 `staged` 状态保留（不丢弃、不进入规范
  数据）；父块到位后自动递归连接。已连通但高度未超过规范链尖端的分叉块
  同样保持 `staged`。
- **校验**：负高度/时间戳/金额/nonce、空哈希、区块内交易重复、交易归属
  与区块不一致、高度与父块不连续、创世块哈希与配置不符，均按畸形数据拒绝。

## 规范链与重组规则

- 规范链始终选择**已连通到创世块**的链中高度最高者；未连通的链不会被提升。
- 区块状态三分类，查询可区分（判定口径在实现、查询、自检、审计四处一致，
  详见下节「并发与乱序下的三态判定口径」）：
  - `canonical`：当前规范链上的区块；
  - `reorged`：**曾在规范链竞争中领先过**、最终却不在规范链上的块。它有两种
    成因：① 在真实投递顺序中被某条更长分叉实际换下去（有对应审计）；
    ② 实际顺序里从未登上规范链，但按统一参考口径它曾在某个高度领先、后被
    最终胜出的更长链反超（由摄取时的参考重放补判，不凭空写审计）；
  - `staged`：父块缺失未连通，或虽已连通但**从未领先**的分叉块。
- 重组触发时，被替换的规范区块在**同一事务**内标记为 `reorged`，新链区块
  标记为 `canonical`，并推进进度；任一步失败整体回滚，无部分应用。
- **重组深度上限**：`max_reorg_depth`（默认 6）。胜出所需替换深度超过上限时，
  该次摄取被明确拒绝（`reorg_too_deep`），区块不落库、规范链与进度不变。

## 并发与乱序下的三态判定口径

问题背景：早期实现里「曾被换下去」完全由**实际投递顺序**决定——同一份数据
（一条主链 + 一条更长分叉）乱序/并发投递时，若更长分叉先到位，落败分叉的块
随后才连通，它们在实际顺序里从未登上规范链，就一直停在 `staged`；而主链先到
时它们又是 `reorged`。最终归类随调度抖动，用例时红时绿。

现在三态由一份**与投递顺序、并发度无关**的确定性「参考重放」统一裁决
（`indexer/classification.py`，实现 / 查询 / 自检共用同一份代码）：

1. 只保留沿 parent 链**连通到高度 0 根块、且高度逐块连续**的块参与竞争；
   父块缺失、高度不连续的块直接归入 `staged`。
2. 参考投递顺序固定为 **`(height, block_hash)` 升序**，在此顺序上重放与实现
   完全相同的链竞争规则：链尖严格更高者胜出并触发翻转；
   **同高度并列**时哈希较大者在参考重放中先领先（它若随后被另一条严格延伸的
   链超过，一样转为 `reorged`）。
3. 重放过程中**曾经位于规范链**、但最终不在规范链上的块 → `reorged`；
   从未领先或从未连通 → `staged`；重放结束时的规范链 → `canonical`。

关键性质——**单调收敛**：随着新区块到达，「曾经领先」只会从 False 变 True
（领先者只可能被严格更长的链反超，不会因新到同高度块丢位）。因此每次摄取提交
后，服务在同一事务内把存储状态向参考口径做一次**只升不降**的补判
（`STAGED -> REORGED`），从不把真实的规范块换下来。于是：

- 同一份数据无论以什么顺序、多少线程投递，最终三态逐块相同、稳定可复现；
- 真实发生的链切换仍由状态机即时翻转并写审计（行为不变），参考补判只补齐
  「实际顺序里没赶上、但口径上确实领先过」的块，且**不凭空写审计**；
- 边角：最终图上存在多条「同最高高度、互不延伸」的并列链（永未分出长短）时
  没有唯一胜出者，保持「同高度不抢占、先到者保持规范」的既有行为，该情形不
  产生重组审计，自检也不对其规范身份做收敛复查。

`GET /health` 的 `block_parent_consistency` 分项会用**同一套参考口径**复查
每个块的归类，一旦实现落库的状态与口径不一致（例如库被外力改坏），会点名
具体区块：`块 a1(h=1) 归类为 staged，与统一口径期望的 reorged 不一致`。

### 审计名单与最终状态对账

审计只记录**真实发生过**的切换，因此「被换下去 / 重新接纳」两份名单与最终
三态之间满足下述可复查关系（`tests/test_stability.py` 中
`_reconcile_audits_with_final` 自动守护，多种乱序下逐块对账）：

- 同一条审计内 `replaced_blocks` 与 `readopted_blocks` 不相交、各自无重复；
- 最终 `canonical` 的块若在审计中出现，其**最后一次**角色必须是
  `readopted`（被换下去又回来了）；最终 `reorged` 的块最后一次角色必须是
  `replaced`，或它从未出现在审计中（即上文成因②的参考补判块）；
- 最终 `staged` / 未连通的块永远不会出现在任何审计名单里。

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
| `replaced_blocks` | 本次被换下去的块（原 CANONICAL → REORGED，高度升序） |
| `readopted_blocks` | 原本在分叉上、本次随胜出链**重新回到规范链**的块（含曾被换为 REORGED 的块，以及搭在回归链上的暂存延伸块；本次新投递的触发块不计入） |

一致性保证：

- **与状态切换同生共死**：审计与区块状态翻转、进度推进在一个事务里；切换
  因重组过深或任一步失败而回滚时，审计也一并回滚，不会留下「无对应切换」的记录。
- **重复投递不凭空记账**：初始建链、规范链正常延伸、同一区块/同一数据集
  重复投递都不构成切换，不产生新记录（幂等返回 `applied=false`）。
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

四个分项：

| 检查项 | 检查内容 |
| --- | --- |
| `canonical_continuity` | 规范链是否从创世起逐块 parent/height 连续、同高度是否有多个规范块 |
| `block_parent_consistency` | 每个块的状态是否与父链对得上（规范块父块必须规范、缺父块只能是 staged、高度须相邻） |
| `canonical_txs` | 规范链交易是否归属正确、有无重复、有无悬空（归属块不存在） |
| `progress` | 记录进度是否与当前规范链尖高度一致 |

有问题时不是只回一个对/错，而是在 `issues` 里点名**具体的 `block_hashes` /
`tx_hashes`** 及可读说明。检查全程**只读**：

- 不执行任何写入，重复调用不改变数据（有 `test_health_is_read_only` 守护）。
- 快照在与写入事务**互斥的存储锁**内一次性取齐区块、交易、进度三张表，因此
  并发摄取过程中调用自检，只会看到某次提交**写前或写后的完整状态**，读不到
  改了一半的中间状态（`test_health_never_reads_half_written_state` 守护）。
  该用例的写入端会**真正连续提交生效区块**（逐块相邻延伸规范链并推进进度），
  自检线程在写入窗口内取多份快照，并断言每份快照自检自洽、观测到的进度序列
  单调非降且起点/终点与写前/最终状态对齐——写入块被畸形拒绝、异常被吞导致
  空转通过的旧问题已修正。
- `block_parent_consistency` 除结构校验外，还用与摄取**同一套确定性参考口径**
  复查每个块的三态归类（见「并发与乱序下的三态判定口径」），保证自检结论能
  解释最终分类而不是各说各话。
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

- **已有数据**：新增的 `reorg_audits` 表通过 `CREATE TABLE IF NOT EXISTS`
  幂等建表，打开旧库会自动补齐，不影响已有的 `blocks`/`txs`/`progress`
  数据与查询结果。升级前产生的历史切换没有审计记录（表从升级后开始记录），
  不影响索引一致性。
- **原有接口**：`/ingest/block`、`/ingest/blocks`、`/ingest/from-source`、
  `/blocks`、`/chain/canonical`、`/progress` 的路径、请求/响应结构与语义
  全部保持不变；新增能力走**独立的新路径**（`/ingest/blocks/atomic`、
  `/reorgs/audits`、`/health`），不改动老调用方。
- 区块三状态（canonical/reorged/staged）的定义与原文档一致；新表、新错误
  分类（`lock_timeout`、`storage_error`）均为纯增量。

## 运行与验证

```bash
uv sync                                   # 安装依赖
uv run uvicorn main:app --reload          # 启动服务
uv run pytest tests/ -v                   # 运行测试（日志打印输入与判定依据）
```

本地手工验证新能力：

```bash
# 1) 整批原子摄取主链
curl -s -X POST localhost:8000/ingest/blocks/atomic -H 'content-type: application/json' \
  -d '{"blocks":[ ... ]}'
# 2) 触发一次重组后，按时间倒序查看切换审计
curl -s localhost:8000/reorgs/audits | jq
# 3) 随时做只读自检（分项 ok 与问题清单）
curl -s localhost:8000/health | jq
```

测试覆盖：在既有的乱序/重复投递、重组切换与状态区分、过深重组拒绝、畸形
负载、进度回退、并发摄取不重不漏、数据源不可用与畸形分类之外，新增：

- `tests/test_audit.py`：切换审计字段如实、初始建链/正常延伸不记账、切换
  失败（过深拒绝 / 审计写入注入失败）整体回滚不留记录、重复投递不重复记账、
  被换下去的块随分叉反超回归时 `readopted_blocks` 如实反映、审计倒序分页。
- `tests/test_health.py`：健康索引四项全过；人为破坏规范链连续性、进度、
  交易归属时给出点名具体块/交易的问题清单；自检只读；并发摄取期间读不到
  改了一半的中间状态。
- `tests/test_batch.py`：整批原子摄取在畸形块/重组过深时整体不留数据、
  整批合法一次生效、批内重复幂等计入 duplicates、等锁超时返回可区分可重试
  的 `lock_timeout` 且不卡死。
- `tests/test_source_api.py`：新增三个 HTTP 端点的端到端行为。
- `tests/test_stability.py`：**稳定性与口径一致性**专项——
  乱序（含重复）多轮投递与多线程并发乱序下，多组数据集（更长分叉胜出、
  反超后再回归、孤立块、正常延伸）的最终三态逐块一致且与统一口径相同；
  原并发数据集在 15 个不同并发调度下 a1..a3 恒为 `reorged`；多种乱序下
  审计「被换下去/重新接纳」名单与最终状态逐块对账无矛盾；落败分叉实际未
  曾规范时按口径补判 `reorged` 且不凭空写审计。

测试日志通过 `log_cli` 输出每个用例的输入与判定依据（含最终三类名单、
审计对账结论、并发自检的生效块数/快照数/进度观测序列）。

### 稳定性复现步骤

下列命令可在本地照着复现「并发 + 乱序下最终状态稳定可验收」的结论：

```bash
uv sync

# 1) 既有重组/乱序/并发/进度回退用例整包连跑 15 轮，确认不再时好时坏
for i in $(seq 1 15); do
  uv run pytest tests/ -q -p no:logging
done

# 2) 专盯原来时红时绿的那条并发摄取用例，连跑 100 轮
for i in $(seq 1 100); do
  uv run pytest tests/test_concurrency.py::test_concurrent_ingest_is_consistent \
    -q -p no:logging
done

# 3) 稳定性与口径对账专项（多轮乱序 + 多并发调度 + 审计对账），带详细日志
uv run pytest tests/test_stability.py -v

# 4) 自检并发保证（写入端真正连续提交生效块；查看日志里 applied 块数、
#    快照数与单调进度序列）
uv run pytest tests/test_health.py::test_health_never_reads_half_written_state -v
```

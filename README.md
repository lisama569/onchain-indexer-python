# onchain-indexer-python

基于 FastAPI 的链上数据索引服务。使用本地固定 JSON 数据模拟链上数据源，
在**链重组、重复投递、乱序到达与并发写入**下保证索引结果一致且可解释；
并额外提供面向长期运行与排障的三项能力：**重组审计、只读自检、整批原子摄取**。

## 架构

```
main.py              FastAPI 入口与路由（create_app 工厂便于测试注入配置）
indexer/
  config.py          IndexerConfig：max_reorg_depth / genesis_hash / db_path /
                     lock_timeout
  models.py          Block / Transaction / BlockStatus / IngestResult /
                     ReorgEvent（审计记录）/ CheckResult / HealthReport /
                     BatchIngestResult
  errors.py          失败分类：malformed_payload / source_unavailable /
                     state_conflict / reorg_too_deep / lock_busy /
                     storage_error
  store.py           SQLite 存储（区块、交易、状态、进度、重组审计表），
                     可超时写锁 + 顶层事务/嵌套 SAVEPOINT + 只读快照
  service.py         摄取、暂存连接、规范链维护、重组、进度推进、审计写入、
                     整批原子摄取
  healthcheck.py     只读自检：四个分项的一致性核对
  source.py          LocalChainSource：本地固定 JSON 数据源
tests/               基于固定数据的重组/重复/乱序/并发/畸形/回退/审计/
                     自检/整批原子测试
```

## 摄取协议

### HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/ingest/block` | 摄取单个区块，成功返回 `{status, applied, reorg_depth}` |
| POST | `/ingest/blocks` | 批量摄取（**旧语义**）：逐块独立事务，逐块返回结果或分类错误 |
| POST | `/ingest/batch` | **整批原子摄取（新）**：整批一个事务，任一块被拒则整批不落库 |
| POST | `/ingest/from-source` | 从本地数据源（`INDEXER_SOURCE_PATH`）按高度顺序拉取并摄取 |
| GET | `/blocks?status=canonical\|reorged\|staged` | 按状态查询区块（含交易） |
| GET | `/blocks/{block_hash}` | 区块详情 |
| GET | `/chain/canonical` | 当前规范链（哈希按高度升序） |
| GET | `/progress` | 摄取进度（规范链最高高度） |
| GET | `/reorgs?limit=50` | **规范链切换审计（新）**，按时间倒序 |
| GET | `/health/check` | **只读自检（新）**，分项返回 ok 与可定位的问题清单 |

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
- 区块状态三分类，查询可区分：
  - `canonical`：当前规范链上的区块；
  - `reorged`：曾被纳入规范链、后被重组替换（其交易随区块一并失去规范归属，
    记录保留可审计）；
  - `staged`：父块缺失未连通，或已连通但暂未胜出的分叉块。
- 重组触发时，被替换的规范区块在**同一事务**内标记为 `reorged`，新链区块
  标记为 `canonical`，并推进进度；任一步失败整体回滚，无部分应用。
- **重组深度上限**：`max_reorg_depth`（默认 6）。胜出所需替换深度超过上限时，
  该次摄取被明确拒绝（`reorg_too_deep`），区块不落库、规范链与进度不变。

## 进度与并发

- 进度 = 规范链最高高度，持久化于 SQLite，**只能单调前进**；写回退进度
  返回 `state_conflict`；同值重放为幂等空操作。
- 所有多步写入包裹在 `BEGIN IMMEDIATE` 事务中，并由进程内写锁串行化；
  并发摄取下每个区块恰好应用一次，重复投递幂等返回。

## 重组审计（新）

凡是规范链发生真实切换（有原规范块被换下去），就在**切换所在的同一个事务**
里写一条审计记录到 `reorg_events` 表，可经 `GET /reorgs` 按时间倒序翻阅。

- **同生共死**：审计 INSERT 与区块状态翻转、进度推进处于同一事务；切换失败
  （如重组过深）整体回滚，不会留下“状态没切、记录先有”的脏记录。
- **重复投递不重复记账**：重复的区块在判定前就走幂等分支返回 `applied=false`，
  不再次执行切换，因此不会凭空多出审计记录。
- **纯延长不记账**：只是把链尖往高延长（没有旧规范块被换下）不算“切换”，
  不产生记录；只有分叉胜出、真实换链才记录。

每条记录包含：

| 字段 | 含义 |
| --- | --- |
| `id` / `occurred_at` | 自增序号（即写入顺序）/ UTC ISO-8601 时间 |
| `depth` / `fork_height` | 切换深度 / 分叉点高度 |
| `old_tip` / `new_tip` | 切换前、后的链尖（哈希 + 高度） |
| `replaced_hashes` | 本次被换下去（canonical→reorged）的块 |
| `adopted_hashes` | 本次进入规范链的全部块 |
| `readopted_hashes` | `adopted` 的子集：曾被换下、本次又被重新接纳回规范链的块 |

```bash
curl -s http://127.0.0.1:8000/reorgs?limit=20
```

## 只读自检（新）

`GET /health/check` 对当前索引做**纯只读**一致性核对，返回一个总 `ok` 和四个
分项，任何一项有问题都会把**具体的块哈希 / 交易哈希 / 两个高度值**列在
`issues` 中，而不是只回一个对或错：

| 分项 | 核对内容 |
| --- | --- |
| `canonical_continuity` | 规范链是否从高度 0 起、逐块 parent 相连、高度连续 |
| `parent_status` | 每块的父块是否存在、高度是否连续；canonical 的父是否也是 canonical；reorged 的父不能是 staged |
| `canonical_txs` | 是否有孤儿交易、归属字段错乱，或同一交易哈希重复挂到多个（规范）块 |
| `progress_tip` | 记录的进度是否等于当前规范链最高高度 |

自检的全部读取都在 `store.read_snapshot()`（持有写锁的只读区间）内完成，
因此在并发摄取过程中调用也只会看到某个**已提交**的自洽状态，看不到改了一半的
中间状态；自检本身不执行任何写入，可随时反复调用。

```bash
curl -s http://127.0.0.1:8000/health/check
# {"ok": true, "checks": [{"name": "...", "ok": true, "issues": []}, ...]}
```

## 整批原子摄取（新）与旧逐块摄取的区别

| 维度 | 旧 `POST /ingest/blocks` | 新 `POST /ingest/batch` |
| --- | --- | --- |
| 事务边界 | 逐块独立事务 | 整批一个顶层事务（逐块复用同一套校验/暂存/重组逻辑） |
| 部分失败 | 合法块照样入库，非法块在该块结果里带 `error` | 任一块被拒（畸形/内容冲突/重组过深）**整批回滚**，不留下任何数据 |
| 重复投递 | 该块幂等成功 | 同样幂等：计入 `duplicates`，`applied=false`，**不算失败** |
| 返回 | `{results: [...]}`（每项可能是 error） | 成功 `{accepted, duplicates, results}`；失败直接走错误响应（4xx/5xx） |
| 并发撞锁 | 每块独立排队，无超时 | 等锁超过 `lock_timeout` 秒明确返回 `lock_busy`（503，可重试），不会无限卡死 |

语义示例：一批 5 个块中第 4 个畸形 —— 旧接口会落库前 3 个（加第 5 个），
新接口则保证回滚后区块表、交易表、进度、审计与调用前完全一致。

> 新接口刻意使用独立路径 `/ingest/batch`，**不改变** `/ingest/blocks` 的
> 既有逐块语义，旧调用方无需任何改动。

### 错误分类

| category | HTTP | 含义 | 处理方式 |
| --- | --- | --- | --- |
| `malformed_payload` | 400 | 负载畸形、校验失败 | 修复数据后重投 |
| `source_unavailable` | 503 | 本地数据源暂时不可读 | 可重试 |
| `state_conflict` | 409 | 与已持久化状态矛盾（内容冲突、进度回退） | 检查调用方状态 |
| `reorg_too_deep` | 422 | 重组深度超过 `max_reorg_depth` | 需人工介入确认 |
| `lock_busy` | 503 | 并发写锁等待超时（可区分的临时故障） | 稍后重试 |
| `storage_error` | 500 | 底层数据库异常的统一包装（原始异常不直接外泄） | 查看服务端日志 |

错误响应体：`{"error": {"category", "message", "detail"}}`。

## 配置项

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `INDEXER_MAX_REORG_DEPTH` | `6` | 允许的最大重组深度 |
| `INDEXER_GENESIS_HASH` | `""`（不校验） | 创世块哈希，非空时校验高度 0 区块 |
| `INDEXER_DB_PATH` | `:memory:` | SQLite 路径，设为文件路径即持久化 |
| `INDEXER_SOURCE_PATH` | 未设置 | 本地数据源 JSON 文件路径 |
| `INDEXER_LOCK_TIMEOUT` | `5.0` | 整批/单块摄取等待写锁的最长秒数，超时返回 `lock_busy` |

## 兼容性

- **数据兼容**：新增的 `reorg_events` 表通过 `CREATE TABLE IF NOT EXISTS`
  建立，对已有 SQLite 数据文件无需迁移，旧库启动后自动获得新表；
  `blocks` / `txs` / `progress` 表结构与既有数据格式未变。
- **接口兼容**：原有 `/ingest/block`、`/ingest/blocks`、`/ingest/from-source`、
  `/blocks*`、`/chain/canonical`、`/progress` 的路径、请求/响应结构完全保留；
  新能力全部走**新增**路径（`/ingest/batch`、`/reorgs`、`/health/check`）。
- **行为兼容**：单块摄取的幂等、暂存、重组、进度语义不变；新的错误分类
  `lock_busy` / `storage_error` 是对原有分类集合的追加，不改动旧分类码。

数据源文件格式：`{"blocks": [{block_hash, parent_hash, height, timestamp,
transactions: [...]}, ...]}`。

## 运行与验证

```bash
uv sync                                   # 安装依赖
uv run uvicorn main:app --reload          # 启动服务
uv run pytest tests/ -v                   # 运行全部测试（日志打印输入与判定依据）
```

只跑新增能力的测试：

```bash
uv run pytest tests/test_reorg_audit.py   # 审计：切换记录/失败回滚/重复不记账/重新接纳
uv run pytest tests/test_healthcheck.py   # 自检：健康全绿 + 人为破损可定位 + 并发只读
uv run pytest tests/test_batch_atomic.py  # 整批原子：全成全败/批内幂等/锁超时/异常包装
uv run pytest tests/test_new_api.py       # 新 HTTP 端点：/reorgs /health/check /ingest/batch
```

手工验证（服务启动后）：

```bash
# 1) 整批原子摄取
curl -s -XPOST localhost:8000/ingest/batch -H 'content-type: application/json' -d '{
  "blocks": [
    {"block_hash":"g0","parent_hash":"","height":0,"timestamp":1,"transactions":[]},
    {"block_hash":"a1","parent_hash":"g0","height":1,"timestamp":2,"transactions":[]}
  ]}'
# 2) 触发一次重组后翻阅审计（最新在前）
curl -s localhost:8000/reorgs | python -m json.tool
# 3) 随时只读自检
curl -s localhost:8000/health/check | python -m json.tool
# 4) 整批中夹一个畸形块：观察整批回滚（再 GET /blocks 应为空、/progress 为 -1）
curl -s -XPOST localhost:8000/ingest/batch -H 'content-type: application/json' -d '{
  "blocks": [
    {"block_hash":"g0","parent_hash":"","height":0,"timestamp":1,"transactions":[]},
    {"block_hash":"bad","parent_hash":"g0","height":-1,"timestamp":1,"transactions":[]}
  ]}'
```

测试覆盖：乱序/重复投递、重组切换与状态区分、过深重组拒绝、畸形负载、
进度回退、并发摄取不重不漏、数据源不可用与畸形分类，以及新增的——审计记录
在切换失败时整体回滚、重复投递不重复记账、被换下块重新回到规范链时如实记录；
自检出人为构造的各类破损并给出可定位清单、并发摄取中读到的始终是已提交状态；
整批原子摄取在部分块被拒时不留任何数据、批内重复按幂等处理、锁等待超时返回
可区分可重试的 `lock_busy`、底层数据库异常不外泄。测试日志通过 `log_cli`
输出每个用例的输入与判定依据。

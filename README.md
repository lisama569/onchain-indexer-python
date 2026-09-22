# onchain-indexer-python

基于 FastAPI 的链上数据索引服务。使用本地固定 JSON 数据模拟链上数据源，
在**链重组、重复投递、乱序到达与并发写入**下保证索引结果一致且可解释。

## 架构

```
main.py              FastAPI 入口与路由（create_app 工厂便于测试注入配置）
indexer/
  config.py          IndexerConfig：max_reorg_depth / genesis_hash / db_path
  models.py          Block / Transaction / BlockStatus / IngestResult
  errors.py          失败分类：malformed_payload / source_unavailable /
                     state_conflict / reorg_too_deep
  store.py           SQLite 存储（区块、交易、状态、进度），写锁 + 事务
  service.py         摄取、暂存连接、规范链维护、重组、进度推进
  source.py          LocalChainSource：本地固定 JSON 数据源
tests/               基于固定数据的重组/重复/乱序/并发/畸形/回退测试
```

## 摄取协议

### HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/ingest/block` | 摄取单个区块，成功返回 `{status, applied, reorg_depth}` |
| POST | `/ingest/blocks` | 批量摄取，逐块独立事务，逐块返回结果或分类错误 |
| POST | `/ingest/from-source` | 从本地数据源（`INDEXER_SOURCE_PATH`）按高度顺序拉取并摄取 |
| GET | `/blocks?status=canonical\|reorged\|staged` | 按状态查询区块（含交易） |
| GET | `/blocks/{block_hash}` | 区块详情 |
| GET | `/chain/canonical` | 当前规范链（哈希按高度升序） |
| GET | `/progress` | 摄取进度（规范链最高高度） |

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

## 错误分类

| category | HTTP | 含义 | 处理方式 |
| --- | --- | --- | --- |
| `malformed_payload` | 400 | 负载畸形、校验失败 | 修复数据后重投 |
| `source_unavailable` | 503 | 本地数据源暂时不可读 | 可重试 |
| `state_conflict` | 409 | 与已持久化状态矛盾（内容冲突、进度回退） | 检查调用方状态 |
| `reorg_too_deep` | 422 | 重组深度超过 `max_reorg_depth` | 需人工介入确认 |

错误响应体：`{"error": {"category", "message", "detail"}}`。

## 配置项

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `INDEXER_MAX_REORG_DEPTH` | `6` | 允许的最大重组深度 |
| `INDEXER_GENESIS_HASH` | `""`（不校验） | 创世块哈希，非空时校验高度 0 区块 |
| `INDEXER_DB_PATH` | `:memory:` | SQLite 路径，设为文件路径即持久化 |
| `INDEXER_SOURCE_PATH` | 未设置 | 本地数据源 JSON 文件路径 |

数据源文件格式：`{"blocks": [{block_hash, parent_hash, height, timestamp,
transactions: [...]}, ...]}`。

## 运行与验证

```bash
uv sync                                   # 安装依赖
uv run uvicorn main:app --reload          # 启动服务
uv run pytest tests/ -v                   # 运行测试（日志打印输入与判定依据）
```

测试覆盖：乱序/重复投递、重组切换与状态区分、过深重组拒绝、畸形负载、
进度回退、并发摄取不重不漏、数据源不可用与畸形分类。测试日志通过
`log_cli` 输出每个用例的输入与判定依据。

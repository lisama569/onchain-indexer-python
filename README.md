# onchain-indexer-python

基于 FastAPI + SQLite 的链上数据索引服务。用本地固定区块/交易数据模拟数据源，
保证在**链重组、重复投递、乱序到达与并发写入**下索引结果始终一致、可解释、可恢复。

## 快速开始

```bash
uv sync --group dev          # 安装运行与测试依赖
uv run uvicorn main:app      # 启动服务（默认 http://127.0.0.1:8000）
uv run pytest -s             # 运行全部测试（-s 打印输入与判定日志）
```

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/ingest` | 摄取一批区块并声明摄取进度 |
| GET  | `/blocks?status=canonical\|staged\|reorged` | 查询区块（可按状态过滤） |
| GET  | `/transactions?status=...` | 查询交易，状态跟随所属区块 |
| GET  | `/progress` | 已持久化的摄取进度与规范链末端 |

### 摄取协议（POST /ingest）

```json
{
  "progress": 3,
  "blocks": [
    {
      "block_hash": "A2",
      "parent_hash": "A1",
      "height": 2,
      "transactions": [
        {"tx_hash": "tx-A2", "block_hash": "A2", "tx_index": 0,
         "sender": "alice", "recipient": "bob", "amount": 100}
      ]
    }
  ]
}
```

* `progress`：生产者已交付到的最大区块高度，是**全局单调水位线**。
  成功摄取后只允许相等或增大；写回更小的值返回 `state_conflict`。
  每个区块的 `height` 不得超过本次声明的 `progress`。
* 区块靠 `(block_hash, parent_hash, height)` 连接：`height=0` 的创世块
  `parent_hash` 必须为 `null`，其余区块必须有父哈希；若父块尚未送达，
  区块进入 `staged`（暂存），**不会**被当成规范数据，父块到达后自动连通提升。
* 同一区块/交易重复投递且内容完全一致时为幂等空操作，不会产生重复记录；
  同一哈希但高度/父哈希/交易内容不一致，或交易被声明属于别的区块，
  返回 `malformed_data`，整批不落库。

## 三种数据状态

| 状态 | 含义 |
| --- | --- |
| `canonical` | 当前规范链上的区块及其交易 |
| `reorged` | 曾经在规范链上、后被重组替换的区块及其交易 |
| `staged` | 未连通的孤块、竞争叉和等父区块（从未成为规范） |

## 规范链与重组规则

每次摄取在**单个 `BEGIN IMMEDIATE` 事务 + 进程内写锁**内完成：加载快照 →
校验合并 → 重选规范链 → 重组深度检查 → 整体提交。中途任何失败全部回滚，
不会出现部分应用。

1. 只在"从创世块起父哈希一路连通且高度连续"的完整链之间挑选规范链。
2. **链高最高者胜**；等高时现任规范链优先（避免等长抖动）；再等高取
   tip 哈希字典序最小，保证结果确定且可解释。
3. 规范链切换时，被替换区块及其交易一并标记为 `reorged`，
   新链标记为 `canonical`；不连通的竞争块保持 `staged`。
   若旧叉后来延伸得更长而重新胜出，其区块会从 `reorged` 恢复为 `canonical`。
4. **重组深度** = 被替换的旧规范区块数（旧 tip 到两链最高公共祖先之间的区块数）。
   深度超过配置上限 `max_reorg_depth` 时整批摄取被拒绝（`reorg_too_deep`），
   规范链、进度均保持不变。

## 错误分类

所有摄取失败返回 `{"accepted": false, "reason": "<类别>", "detail": "..."}`：

| reason | HTTP | 触发场景 | 客户端建议 |
| --- | --- | --- | --- |
| `malformed_data` | 422 | 结构/语义非法：创世块带父、高度不连续、交易挂错块、同哈希内容冲突等 | 修正数据，勿重试 |
| `state_conflict` | 409 | 进度回退、并发下水位线已被他人推进 | 刷新状态后重算 |
| `reorg_too_deep` | 422 | 重组深度超过 `max_reorg_depth` | 人工介入/扩大确认窗口 |
| `temporary_unavailable` | 503 | 写锁等待超时、数据库临时忙 | 可安全重试 |

请求体连 Pydantic 校验都未通过时同样是 422（FastAPI 标准校验响应），
可凭 `reason` 字段与 `reorg_too_deep` 区分。

## 并发与持久化保证

* 所有摄取由一把进程内写锁串行化，SQLite 单连接上以一个事务提交；
  并发请求要么完整生效、要么按上表分类失败，互不覆盖。
* 进度、规范 tip 与区块/交易三态状态持久化在 SQLite 中（默认
  `./indexer.db`），进程重启后依旧可查，回退进度仍会被拒绝。
* 内存库 `sqlite:///:memory:` 仅用于测试。

## 配置项（`app/config.py` 的 `Settings`）

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `max_reorg_depth` | `64` | 允许的最大重组深度（区块数），超过则拒绝 |
| `lock_timeout_seconds` | `5.0` | 写锁/数据库忙等待秒数，超时返回 503 |
| `database_url` | `sqlite:///./indexer.db` | SQLite 数据库地址 |

## 验证方法

`tests/` 基于本地固定数据（`tests/conftest.py` 中的 G/A1/A2/A3 主链与
A2p/A3p/A4p 竞争叉）覆盖：

* 正常有序摄取、状态过滤查询；
* 重复投递（幂等不重）、单批内重复、乱序到达（暂存后提升，不漏）；
* 重组替换、三态可区分、等长竞争不抖动、旧叉反超、超深重组被原子拒绝；
* 12 线程并发摄取不同区块、并发相同负载、竞争叉乱序并发、并发进度竞争；
* 各类畸形负载、进度回退、锁争用 503、HTTP 状态码与 reason 可区分；
* 文件库重启后进度与三态状态不丢。

每个用例通过日志打印 **INPUT**（输入构造）与 **JUDGE**（判定依据），
用 `uv run pytest -s` 可查看完整决策过程。

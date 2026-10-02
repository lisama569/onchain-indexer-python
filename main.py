"""FastAPI 入口：摄取、查询与进度接口。"""

from __future__ import annotations

import os

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from indexer.config import IndexerConfig
from indexer.errors import IngestError
from indexer.health import HealthChecker
from indexer.models import Block, BlockStatus, Transaction
from indexer.service import IngestService
from indexer.source import LocalChainSource
from indexer.store import Store

# 失败原因分类 -> HTTP 状态码
_CATEGORY_STATUS = {
    "malformed_payload": 400,
    "source_unavailable": 503,
    "state_conflict": 409,
    "reorg_too_deep": 422,
    "lock_timeout": 503,       # 等锁超时：可重试的临时故障
    "storage_error": 500,
    "internal": 500,
}


class TxIn(BaseModel):
    tx_hash: str
    sender: str
    recipient: str
    amount: int
    nonce: int


class BlockIn(BaseModel):
    block_hash: str
    parent_hash: str
    height: int
    timestamp: int
    transactions: list[TxIn] = []

    def to_block(self) -> Block:
        return Block(
            block_hash=self.block_hash,
            parent_hash=self.parent_hash,
            height=self.height,
            timestamp=self.timestamp,
            transactions=tuple(
                Transaction(tx_hash=t.tx_hash, block_hash=self.block_hash,
                            sender=t.sender, recipient=t.recipient,
                            amount=t.amount, nonce=t.nonce)
                for t in self.transactions
            ),
        )


class BatchIn(BaseModel):
    blocks: list[BlockIn]


def _block_view(block: Block, status: BlockStatus) -> dict:
    return {
        "block_hash": block.block_hash,
        "parent_hash": block.parent_hash,
        "height": block.height,
        "timestamp": block.timestamp,
        "status": status.value,
        "transactions": [
            {"tx_hash": t.tx_hash, "sender": t.sender, "recipient": t.recipient,
             "amount": t.amount, "nonce": t.nonce}
            for t in block.transactions
        ],
    }


def create_app(config: IndexerConfig | None = None,
               source: LocalChainSource | None = None) -> FastAPI:
    config = config or IndexerConfig(
        max_reorg_depth=int(os.environ.get("INDEXER_MAX_REORG_DEPTH", "6")),
        genesis_hash=os.environ.get("INDEXER_GENESIS_HASH", ""),
        db_path=os.environ.get("INDEXER_DB_PATH", ":memory:"),
        lock_timeout=float(os.environ.get("INDEXER_LOCK_TIMEOUT", "5")),
    )
    store = Store(config.db_path, lock_timeout=config.lock_timeout)
    service = IngestService(store, config)
    health = HealthChecker(store, genesis_hash=config.genesis_hash)
    if source is None and os.environ.get("INDEXER_SOURCE_PATH"):
        source = LocalChainSource(os.environ["INDEXER_SOURCE_PATH"])

    app = FastAPI(title="onchain-indexer")
    app.state.service = service
    app.state.store = store
    app.state.source = source
    app.state.health = health
    app.state.config = config

    @app.exception_handler(IngestError)
    async def ingest_error_handler(_: Request, exc: IngestError) -> JSONResponse:
        return JSONResponse(
            status_code=_CATEGORY_STATUS.get(exc.category, 500),
            content={"error": {"category": exc.category,
                               "message": exc.message,
                               "detail": exc.detail}},
        )

    @app.get("/")
    def read_root():
        return {"service": "onchain-indexer", "status": "ok"}

    # ---- 摄取 ----
    @app.post("/ingest/block")
    def ingest_block(payload: BlockIn):
        result = service.ingest_block(payload.to_block())
        return {"block_hash": result.block_hash, "status": result.status.value,
                "applied": result.applied, "reorg_depth": result.reorg_depth}

    @app.post("/ingest/blocks")
    def ingest_blocks(payload: BatchIn):
        """批量摄取：逐块独立事务，逐块返回结果或分类错误。"""
        outcomes = []
        for item in payload.blocks:
            try:
                r = service.ingest_block(item.to_block())
                outcomes.append({"block_hash": r.block_hash,
                                 "status": r.status.value, "applied": r.applied,
                                 "reorg_depth": r.reorg_depth})
            except IngestError as exc:
                outcomes.append({"block_hash": item.block_hash,
                               "error": {"category": exc.category,
                                         "message": exc.message}})
        return {"results": outcomes}

    @app.post("/ingest/blocks/atomic")
    def ingest_blocks_atomic(payload: BatchIn):
        """整批原子摄取：一批要么全部生效，要么整体回滚不留任何数据。

        与 /ingest/blocks 的逐块独立事务不同；批次内重复投递按幂等处理、
        计入 duplicates 不算失败；任一区块被拒整批拒绝（错误分类直接返回，
        整批 4xx/5xx），不会逐块混入 outcomes。
        """
        blocks = [item.to_block() for item in payload.blocks]
        result = service.ingest_blocks_atomic(blocks)
        return {
            "applied": result.applied,
            "duplicates": result.duplicates,
            "results": [{"block_hash": r.block_hash, "status": r.status.value,
                         "applied": r.applied, "reorg_depth": r.reorg_depth}
                        for r in result.results],
        }

    @app.post("/ingest/from-source")
    def ingest_from_source():
        """从本地数据源拉取全部区块并按高度顺序摄取。"""
        if app.state.source is None:
            return JSONResponse(status_code=503, content={
                "error": {"category": "source_unavailable",
                          "message": "未配置本地数据源"}})
        blocks = app.state.source.fetch_all()
        outcomes = []
        for blk in sorted(blocks, key=lambda b: b.height):
            try:
                r = service.ingest_block(blk)
                outcomes.append({"block_hash": r.block_hash,
                                 "status": r.status.value, "applied": r.applied})
            except IngestError as exc:
                outcomes.append({"block_hash": blk.block_hash,
                                 "error": {"category": exc.category}})
        return {"ingested": len(outcomes), "results": outcomes}

    # ---- 查询 ----
    @app.get("/blocks")
    def list_blocks(status: str | None = None):
        wanted = BlockStatus(status) if status else None
        return {"blocks": [_block_view(b, s)
                           for b, s in store.list_blocks(wanted)]}

    @app.get("/blocks/{block_hash}")
    def get_block(block_hash: str):
        entry = store.get_block(block_hash)
        if entry is None:
            return JSONResponse(status_code=404,
                                content={"error": "block not found"})
        return _block_view(entry[0], entry[1])

    @app.get("/chain/canonical")
    def canonical_chain():
        return {"chain": [b.block_hash for b in service.canonical_chain()]}

    @app.get("/progress")
    def progress():
        return {"height": store.get_progress()}

    # ---- 重组审计 ----
    @app.get("/reorgs/audits")
    def reorg_audits(limit: int = 50, offset: int = 0):
        """按时间倒序翻查规范链切换审计记录。"""
        records = store.list_reorg_audits(limit=max(1, min(limit, 500)),
                                          offset=max(0, offset))
        return {"audits": [
            {"id": r.id, "created_at_ns": r.created_at_ns,
             "trigger_block_hash": r.trigger_block_hash,
             "fork_height": r.fork_height, "depth": r.depth,
             "old_tip": {"block_hash": r.old_tip_hash,
                         "height": r.old_tip_height},
             "new_tip": {"block_hash": r.new_tip_hash,
                         "height": r.new_tip_height},
             "replaced_blocks": list(r.replaced_blocks),
             "readopted_blocks": list(r.readopted_blocks)}
            for r in records]}

    # ---- 只读自检 ----
    @app.get("/health")
    def health_check():
        """只读自检：分项给出结论，问题清单定位到具体块/交易；不落任何数据。

        HTTP 200 表示全部检查通过；发现问题返回 200 但 ok=false（自检本身
        成功执行、索引存在破损），由调用方据 ok 字段判断。
        """
        report = health.run_checks()
        return {
            "ok": report.ok,
            "checks": [{"name": c.name, "ok": c.ok,
                        "issues": [{"check": i.check, "message": i.message,
                                    "block_hashes": list(i.block_hashes),
                                    "tx_hashes": list(i.tx_hashes)}
                                   for i in c.issues]}
                       for c in report.checks],
        }

    return app


app = create_app()

"""FastAPI 入口：摄取、查询与进度接口。"""

from __future__ import annotations

import os

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from indexer.config import IndexerConfig
from indexer.errors import IngestError
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
    )
    store = Store(config.db_path)
    service = IngestService(store, config)
    if source is None and os.environ.get("INDEXER_SOURCE_PATH"):
        source = LocalChainSource(os.environ["INDEXER_SOURCE_PATH"])

    app = FastAPI(title="onchain-indexer")
    app.state.service = service
    app.state.store = store
    app.state.source = source
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

    return app


app = create_app()

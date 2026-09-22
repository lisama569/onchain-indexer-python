"""FastAPI application wiring."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.config import Settings
from app.errors import IngestError
from app.models import (
    BlockStatus,
    IngestRequest,
    IngestResult,
    ProgressView,
    BlockView,
    TransactionView,
)
from app.service import IndexerService
from app.storage import Storage


def create_app(
    service: IndexerService | None = None,
    settings: Settings | None = None,
) -> FastAPI:
    app = FastAPI(title="onchain-indexer")

    if service is None:
        settings = settings or Settings()
        storage = Storage(settings.database_url, settings.lock_timeout_seconds)
        service = IndexerService(storage, settings)
    app.state.service = service

    @app.exception_handler(IngestError)
    async def ingest_error_handler(_: Request, exc: IngestError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.http_status,
            content={"accepted": False, "reason": exc.category,
                     "detail": str(exc)},
        )

    @app.post("/ingest", response_model=IngestResult)
    def ingest(request: IngestRequest) -> IngestResult:
        return service.ingest(request)

    @app.get("/blocks", response_model=list[BlockView])
    def list_blocks(status: BlockStatus | None = None) -> list[BlockView]:
        return service.list_blocks(status)

    @app.get("/transactions", response_model=list[TransactionView])
    def list_transactions(status: BlockStatus | None = None) -> list[TransactionView]:
        return service.list_transactions(status)

    @app.get("/progress", response_model=ProgressView)
    def get_progress() -> ProgressView:
        return service.get_progress()

    return app

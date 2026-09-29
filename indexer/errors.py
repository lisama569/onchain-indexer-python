"""摄取失败的错误分类。"""

from __future__ import annotations


class IngestError(Exception):
    """所有摄取错误的基类，category 用于 API 层的失败原因分类。"""

    category: str = "internal"

    def __init__(self, message: str, *, detail: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail or {}


class MalformedPayloadError(IngestError):
    """畸形数据：字段缺失、格式非法、哈希/高度不自洽等。"""

    category = "malformed_payload"


class SourceUnavailableError(IngestError):
    """临时不可用：本地数据源暂时无法读取，可重试。"""

    category = "source_unavailable"


class StateConflictError(IngestError):
    """状态冲突：与已持久化状态矛盾（如进度回退、并发写冲突）。"""

    category = "state_conflict"


class ReorgTooDeepError(IngestError):
    """重组深度超过允许上限，必须显式拒绝。"""

    category = "reorg_too_deep"


class LockBusyError(IngestError):
    """并发写锁等待超过时限：临时性故障，调用方可重试。"""

    category = "lock_busy"


class StorageError(IngestError):
    """底层存储异常的统一包装，避免把数据库原始错误抛给调用方。"""

    category = "storage_error"

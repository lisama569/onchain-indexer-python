"""Ingestion error classification.

Each ingest failure maps to exactly one category so callers can
distinguish malformed payloads, temporary unavailability, state
conflicts (regression / concurrent modification) and reorganizations
that are deeper than the allowed limit.
"""

from __future__ import annotations


class IngestError(Exception):
    """Base class for all ingestion failures."""

    category: str = "unknown"
    http_status: int = 400


class MalformedDataError(IngestError):
    """Payload cannot be parsed/normalized (bad fields, broken links)."""

    category = "malformed_data"
    http_status = 422


class TemporaryUnavailableError(IngestError):
    """Storage lock contention or another transient failure."""

    category = "temporary_unavailable"
    http_status = 503


class StateConflictError(IngestError):
    """Progress regression or conflicting concurrent application."""

    category = "state_conflict"
    http_status = 409


class ReorgTooDeepError(IngestError):
    """Reorganization depth exceeds the configured limit."""

    category = "reorg_too_deep"
    http_status = 422

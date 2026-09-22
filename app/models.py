"""API / domain models for blocks, transactions and progress."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class BlockStatus(str, Enum):
    CANONICAL = "canonical"
    STAGED = "staged"
    REORGED = "reorged"


class InTransaction(BaseModel):
    tx_hash: str = Field(min_length=1)
    block_hash: str
    tx_index: int = Field(ge=0)
    sender: str
    recipient: str
    amount: int = Field(ge=0)


class InBlock(BaseModel):
    block_hash: str = Field(min_length=1)
    parent_hash: str | None = None
    height: int = Field(ge=0)
    transactions: list[InTransaction] = Field(default_factory=list)


class IngestRequest(BaseModel):
    # Highest block height the producer considers delivered.
    # Must never regress across successful ingests.
    progress: int = Field(ge=0)
    blocks: list[InBlock] = Field(default_factory=list)


class BlockView(BaseModel):
    block_hash: str
    parent_hash: str | None
    height: int
    status: BlockStatus


class TransactionView(BaseModel):
    tx_hash: str
    block_hash: str
    tx_index: int
    sender: str
    recipient: str
    amount: int
    status: BlockStatus


class IngestResult(BaseModel):
    accepted: bool
    progress: int
    canonical_blocks: int
    staged_blocks: int
    reorged_blocks: int
    reason: str | None = None


class ProgressView(BaseModel):
    progress: int
    canonical_tip_hash: str | None
    canonical_tip_height: int | None

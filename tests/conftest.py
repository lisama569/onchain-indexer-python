"""测试公共夹具：本地固定链数据与服务工厂。"""

from __future__ import annotations

import json
import logging

import pytest

from indexer.config import IndexerConfig
from indexer.models import Block, Transaction
from indexer.service import IngestService
from indexer.store import Store

log = logging.getLogger("test")

GENESIS = "g0"


def make_block(block_hash: str, parent_hash: str, height: int,
               n_tx: int = 1) -> Block:
    """构造固定结构的测试区块，交易内容由区块哈希确定性派生。"""
    return Block(
        block_hash=block_hash,
        parent_hash=parent_hash,
        height=height,
        timestamp=1_700_000_000 + height,
        transactions=tuple(
            Transaction(tx_hash=f"tx-{block_hash}-{i}", block_hash=block_hash,
                        sender="alice", recipient="bob", amount=i + 1, nonce=i)
            for i in range(n_tx)
        ),
    )


# 固定链数据：
#   主链   g0 <- a1 <- a2 <- a3
#   分叉B  g0 <- b1 <- b2 <- b3 <- b4   （更长，触发深度 3 重组）
#   分叉C  g0 <- c1 <- ... <- c6        （触发深度 5 重组，用于超限拒绝）
def main_chain() -> list[Block]:
    return [make_block("g0", "", 0), make_block("a1", "g0", 1),
            make_block("a2", "a1", 2), make_block("a3", "a2", 3)]


def fork_b() -> list[Block]:
    return [make_block(f"b{i}", "g0" if i == 1 else f"b{i-1}", i)
            for i in range(1, 5)]


def fork_c() -> list[Block]:
    return [make_block(f"c{i}", "g0" if i == 1 else f"c{i-1}", i)
            for i in range(1, 7)]


@pytest.fixture()
def config() -> IndexerConfig:
    return IndexerConfig(max_reorg_depth=3, genesis_hash=GENESIS)


@pytest.fixture()
def service(config) -> IngestService:
    return IngestService(Store(":memory:"), config)


@pytest.fixture()
def source_file(tmp_path):
    """把固定链数据写入本地 JSON 文件，模拟数据源。"""
    def _write(blocks: list[Block]) -> str:
        payload = {"blocks": [
            {"block_hash": b.block_hash, "parent_hash": b.parent_hash,
             "height": b.height, "timestamp": b.timestamp,
             "transactions": [
                 {"tx_hash": t.tx_hash, "sender": t.sender,
                  "recipient": t.recipient, "amount": t.amount,
                  "nonce": t.nonce}
                 for t in b.transactions]}
            for b in blocks]}
        path = tmp_path / "chain_data.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return str(path)
    return _write

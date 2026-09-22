"""本地区块/交易数据源：从固定 JSON 数据读取，模拟链上数据源。"""

from __future__ import annotations

import json
import os
import threading

from .errors import MalformedPayloadError, SourceUnavailableError
from .models import Block, Transaction


def _parse_block(raw: dict) -> Block:
    """把 JSON 对象解析为规范化 Block；结构非法时抛 MalformedPayloadError。"""
    try:
        txs = tuple(
            Transaction(
                tx_hash=str(t["tx_hash"]),
                block_hash=str(raw["block_hash"]),
                sender=str(t["sender"]),
                recipient=str(t["recipient"]),
                amount=int(t["amount"]),
                nonce=int(t["nonce"]),
            )
            for t in raw.get("transactions", [])
        )
        return Block(
            block_hash=str(raw["block_hash"]),
            parent_hash=str(raw["parent_hash"]),
            height=int(raw["height"]),
            timestamp=int(raw["timestamp"]),
            transactions=txs,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise MalformedPayloadError(
            f"数据源中区块记录畸形: {exc}",
            detail={"raw_hash": raw.get("block_hash")},
        ) from exc


class LocalChainSource:
    """从本地固定 JSON 文件读取区块，可模拟临时不可用。

    数据文件格式: {"blocks": [ {block_hash, parent_hash, height,
    timestamp, transactions: [...]}, ... ]}
    """

    def __init__(self, data_path: str) -> None:
        self._data_path = data_path
        self._lock = threading.Lock()
        self._cache: list[Block] | None = None

    def _load(self) -> list[Block]:
        with self._lock:
            if self._cache is not None:
                return self._cache
            if not os.path.exists(self._data_path):
                raise SourceUnavailableError(
                    f"数据源文件不存在: {self._data_path}",
                    detail={"path": self._data_path},
                )
            try:
                with open(self._data_path, "r", encoding="utf-8") as fh:
                    payload = json.load(fh)
            except (OSError, json.JSONDecodeError) as exc:
                raise SourceUnavailableError(
                    f"数据源暂时不可读: {exc}",
                    detail={"path": self._data_path},
                ) from exc
            if not isinstance(payload, dict) or not isinstance(
                    payload.get("blocks"), list):
                raise MalformedPayloadError(
                    "数据源顶层结构非法：应为 {\"blocks\": [...]}",
                    detail={"path": self._data_path},
                )
            self._cache = [_parse_block(raw) for raw in payload["blocks"]]
            return self._cache

    def fetch_block(self, block_hash: str) -> Block:
        """按哈希读取区块；不存在抛 MalformedPayloadError。"""
        for blk in self._load():
            if blk.block_hash == block_hash:
                return blk
        raise MalformedPayloadError(
            f"数据源中不存在区块: {block_hash}",
            detail={"block_hash": block_hash},
        )

    def fetch_by_height(self, height: int) -> list[Block]:
        """按高度读取所有区块（含分叉）。"""
        return [b for b in self._load() if b.height == height]

    def fetch_all(self) -> list[Block]:
        """返回数据源中的全部区块。"""
        return list(self._load())

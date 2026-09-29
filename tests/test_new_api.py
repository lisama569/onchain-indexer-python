"""新增 HTTP 能力测试：审计翻阅、只读自检、整批原子摄取。"""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from indexer.config import IndexerConfig
from indexer.source import LocalChainSource
from main import create_app

from .conftest import fork_b, main_chain

log = logging.getLogger("test")


@pytest.fixture()
def client(source_file):
    path = source_file(main_chain() + fork_b())
    app = create_app(IndexerConfig(max_reorg_depth=3, genesis_hash="g0"),
                     source=LocalChainSource(path))
    return TestClient(app)


def _payload(b):
    return {"block_hash": b.block_hash, "parent_hash": b.parent_hash,
            "height": b.height, "timestamp": b.timestamp,
            "transactions": [
                {"tx_hash": t.tx_hash, "sender": t.sender,
                 "recipient": t.recipient, "amount": t.amount, "nonce": t.nonce}
                for t in b.transactions]}


def test_reorgs_endpoint_records_and_orders_desc(client):
    """/reorgs 返回倒序审计记录，无切换时为空数组。"""
    assert client.get("/reorgs").json() == {"reorgs": []}
    for b in main_chain():
        client.post("/ingest/block", json=_payload(b))
    assert client.get("/reorgs").json()["reorgs"] == []
    for b in fork_b():
        client.post("/ingest/block", json=_payload(b))
    body = client.get("/reorgs").json()
    log.info("输入: b 链重组后 GET /reorgs -> %s", body)
    assert len(body["reorgs"]) == 1
    e = body["reorgs"][0]
    assert e["old_tip"]["block_hash"] == "a3"
    assert e["new_tip"]["block_hash"] == "b4"
    assert e["replaced_hashes"] == ["a1", "a2", "a3"]
    assert set(e["adopted_hashes"]) == {"b1", "b2", "b3", "b4"}
    assert e["readopted_hashes"] == []
    assert "occurred_at" in e and "depth" in e


def test_health_check_endpoint(client):
    """/health/check 健康时全绿，结构含四个分项；失败的摄取不改其只读性。"""
    for b in main_chain():
        client.post("/ingest/block", json=_payload(b))
    r = client.get("/health/check")
    assert r.status_code == 200
    body = r.json()
    log.info("输入: GET /health/check（健康）-> ok=%s", body["ok"])
    assert body["ok"] is True
    names = {c["name"] for c in body["checks"]}
    assert names == {"canonical_continuity", "parent_status",
                     "canonical_txs", "progress_tip"}
    assert all(c["ok"] and c["issues"] == [] for c in body["checks"])


def test_batch_endpoint_atomic_success_and_duplicate(client):
    """/ingest/batch 成功返回 accepted/duplicates；重复整批 duplicates=N。"""
    chain = main_chain()
    r = client.post("/ingest/batch",
                    json={"blocks": [_payload(b) for b in chain]})
    assert r.status_code == 200
    body = r.json()
    log.info("输入: 整批 g0..a3 -> %s", body)
    assert body["accepted"] == 4 and body["duplicates"] == 0

    r2 = client.post("/ingest/batch",
                     json={"blocks": [_payload(b) for b in chain]})
    body2 = r2.json()
    assert body2["accepted"] == 0 and body2["duplicates"] == 4
    assert all(x["applied"] is False for x in body2["results"])


def test_batch_endpoint_all_or_nothing_on_rejection(client):
    """批次内一块畸形 -> 400 且整批无任何数据落库。"""
    chain = main_chain()
    batch = {"blocks": [_payload(chain[0]), _payload(chain[1]),
                        {"block_hash": "bad", "parent_hash": "a1",
                         "height": -1, "timestamp": 0, "transactions": []}]}
    r = client.post("/ingest/batch", json=batch)
    log.info("输入: 整批含负高度块 -> HTTP %d %s", r.status_code, r.json())
    assert r.status_code == 400
    assert r.json()["error"]["category"] == "malformed_payload"
    # 前两块也必须回滚
    assert client.get("/blocks").json()["blocks"] == []
    assert client.get("/progress").json()["height"] == -1
    # 审计也不应有记录
    assert client.get("/reorgs").json()["reorgs"] == []


def test_batch_and_legacy_blocks_endpoint_coexist(client):
    """新整批接口与旧逐块接口语义独立、可共存：旧接口仍逐块返回错误。"""
    chain = main_chain()
    legacy_batch = {"blocks": [_payload(chain[0]),
                               {"block_hash": "", "parent_hash": "g0",
                                "height": 1, "timestamp": 0,
                                "transactions": []}]}
    r = client.post("/ingest/blocks", json=legacy_batch)
    assert r.status_code == 200
    results = r.json()["results"]
    log.info("输入: 旧 /ingest/blocks 含畸形块 -> 逐块结果=%s", results)
    assert results[0]["status"] == "canonical"
    assert results[1]["error"]["category"] == "malformed_payload"
    # 旧接口的合法块已落库（非原子），新接口仍可继续整批工作
    r2 = client.post("/ingest/batch",
                     json={"blocks": [_payload(b) for b in chain[1:]]})
    assert r2.status_code == 200
    assert r2.json()["accepted"] == 3

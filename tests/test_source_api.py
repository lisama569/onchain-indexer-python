"""本地数据源与 HTTP API 测试。"""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from indexer.config import IndexerConfig
from indexer.errors import MalformedPayloadError, SourceUnavailableError
from indexer.source import LocalChainSource
from main import create_app

from .conftest import fork_b, main_chain

log = logging.getLogger("test")


# ---- 数据源 ----

def test_source_fetch_fixed_data(source_file):
    path = source_file(main_chain())
    src = LocalChainSource(path)
    blk = src.fetch_block("a2")
    log.info("输入: 从本地固定数据读取 a2")
    log.info("判定依据: height=%d parent=%s 与固定数据一致", blk.height,
             blk.parent_hash)
    assert blk.height == 2 and blk.parent_hash == "a1"
    assert len(src.fetch_by_height(1)) == 1


def test_source_unavailable_is_classified(tmp_path):
    src = LocalChainSource(str(tmp_path / "missing.json"))
    with pytest.raises(SourceUnavailableError) as exc_info:
        src.fetch_all()
    log.info("输入: 数据源文件缺失")
    log.info("判定依据: 分类=%s（临时不可用，可重试）", exc_info.value.category)
    assert exc_info.value.category == "source_unavailable"


def test_source_malformed_data_is_classified(source_file, tmp_path):
    path = tmp_path / "bad.json"
    path.write_text('{"blocks": [{"block_hash": "x"}]}', encoding="utf-8")
    src = LocalChainSource(str(path))
    with pytest.raises(MalformedPayloadError) as exc_info:
        src.fetch_all()
    log.info("输入: 数据源记录缺字段")
    log.info("判定依据: 分类=%s（畸形数据）", exc_info.value.category)
    assert exc_info.value.category == "malformed_payload"


# ---- API ----

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


def test_api_ingest_and_query(client):
    chain = main_chain()
    for b in chain:
        r = client.post("/ingest/block", json=_payload(b))
        assert r.status_code == 200
    # 重复投递 -> applied=false
    r = client.post("/ingest/block", json=_payload(chain[0]))
    log.info("输入: 重复投递 g0，响应=%s", r.json())
    assert r.json()["applied"] is False

    assert client.get("/chain/canonical").json()["chain"] == \
        ["g0", "a1", "a2", "a3"]
    assert client.get("/progress").json()["height"] == 3
    staged = client.get("/blocks?status=staged").json()["blocks"]
    canonical = client.get("/blocks?status=canonical").json()["blocks"]
    log.info("判定依据: canonical=%d staged=%d", len(canonical), len(staged))
    assert len(canonical) == 4 and staged == []
    detail = client.get("/blocks/a1").json()
    assert detail["status"] == "canonical" and len(detail["transactions"]) == 1


def test_api_error_classification(client):
    # 畸形负载 -> 400 malformed_payload
    bad = {"block_hash": "bad", "parent_hash": "g0", "height": 1,
           "timestamp": 0,
           "transactions": [{"tx_hash": "t", "sender": "a", "recipient": "b",
                             "amount": -1, "nonce": 0}]}
    r = client.post("/ingest/block", json=bad)
    log.info("输入: 负金额负载 -> HTTP %d %s", r.status_code, r.json())
    assert r.status_code == 400
    assert r.json()["error"]["category"] == "malformed_payload"

    # 状态冲突 -> 409 state_conflict
    g0 = _payload(main_chain()[0])
    client.post("/ingest/block", json=g0)
    tampered = dict(g0, timestamp=g0["timestamp"] + 99)
    r = client.post("/ingest/block", json=tampered)
    log.info("输入: 同哈希不同内容 -> HTTP %d %s", r.status_code, r.json())
    assert r.status_code == 409
    assert r.json()["error"]["category"] == "state_conflict"


def test_api_ingest_from_source_and_reorg(client):
    r = client.post("/ingest/from-source")
    assert r.status_code == 200
    body = r.json()
    log.info("输入: 从本地数据源摄取 %d 个区块", body["ingested"])
    chain = client.get("/chain/canonical").json()["chain"]
    log.info("判定依据: 更长分叉 b 链胜出，规范链=%s", chain)
    assert chain == ["g0", "b1", "b2", "b3", "b4"]
    reorged = client.get("/blocks?status=reorged").json()["blocks"]
    assert {b["block_hash"] for b in reorged} == {"a1", "a2", "a3"}
    assert client.get("/progress").json()["height"] == 4


def test_api_batch_ingest_mixed_outcomes(client):
    blocks = main_chain()
    batch = {"blocks": [_payload(blocks[2]), _payload(blocks[0]),
                        {"block_hash": "", "parent_hash": "g0", "height": 1,
                         "timestamp": 0, "transactions": []}]}
    r = client.post("/ingest/blocks", json=batch)
    assert r.status_code == 200
    results = r.json()["results"]
    log.info("输入: 批量 [乱序a2, g0, 畸形块]，结果=%s", results)
    assert results[0]["status"] == "staged"          # 父缺失暂存
    assert results[1]["status"] == "canonical"
    assert results[2]["error"]["category"] == "malformed_payload"

"""折映双正式切断：先复现，后修复。

红测目标：
1. 同新键第二次折入后，正式区必须只有一行，来源旁表必须两源可查；
2. 两路同时折同一新键，必须只留一行正式并给后到者记“争用”；
3. 正式列表、来源旁表、映射册、互证卡、折映回执五处同态；
4. 旁表写入失败时，正式与旁表同事务回滚；
5. 切断说明锁定“乙路径”，实现偏离时拒写并记“折映偏了”。
"""

import threading
from pathlib import Path

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

import app as appmod
from app import app, init_db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    init_db()
    yield


def _login(c: TestClient) -> dict:
    r = c.post("/api/login", json={"username": "admin", "password": "123456"})
    assert r.status_code == 200, r.text
    return {"Authorization": "Bearer " + r.json()["token"]}


def _items(c: TestClient, path: str, h: dict) -> list:
    r = c.get(path, headers=h)
    assert r.status_code == 200, (path, r.status_code, r.text)
    return r.json()["items"]


def test_same_new_key_keeps_single_formal_and_sources_queryable():
    c = TestClient(app)
    h = _login(c)
    assert c.post("/api/remap", headers=h, json={"old_key": "OLD-B1"}).status_code == 200
    assert c.post("/api/remap", headers=h, json={"old_key": "OLD-B2"}).status_code == 200

    rows = [x for x in _items(c, "/api/formal", h) if x["entity_key"] == "NEW-200"]
    assert len(rows) == 1, f"同新键留下 {len(rows)} 行正式：{rows}"
    assert {s["old_key"] for s in rows[0]["sources"]} == {"OLD-B1", "OLD-B2"}

    sources = _items(c, "/api/formal-sources?entity_key=NEW-200", h)
    assert {s["old_key"] for s in sources} == {"OLD-B1", "OLD-B2"}


def test_concurrent_same_new_key_holds_key_lock_and_records_contention(monkeypatch):
    """两路同时折同一新键：正式只留一行，后到只记争用。

    测试只在“正式区查重”这一读语句上暂停先到者，从而稳定制造两路重叠；
    不依赖修复后才有的内部符号，因此在原实现上同样会因“双正式、无争用”见红。
    """
    c = TestClient(app)
    h = _login(c)
    token = h["Authorization"].split(" ", 1)[1]

    entered = threading.Event()
    release = threading.Event()
    real_connect = appmod.connect
    seen = {"n": 0}

    class _Proxy:
        def __init__(self, real, blocking):
            self._real = real
            self._blocking = blocking

        def __enter__(self):
            self._real.__enter__()
            return self

        def __exit__(self, *exc):
            return self._real.__exit__(*exc)

        def execute(self, sql, *args):
            if self._blocking and "FROM formal_entities WHERE entity_key" in sql:
                self._blocking = False
                entered.set()
                assert release.wait(timeout=5), "抢锁窗口未打开"
            return self._real.execute(sql, *args)

        def __getattr__(self, name):
            return getattr(self._real, name)

    def fake_connect():
        seen["n"] += 1
        return _Proxy(real_connect(), blocking=(seen["n"] == 1))

    monkeypatch.setattr(appmod, "connect", fake_connect)
    results: list = []

    def worker(old_key: str):
        try:
            out = appmod.remap(appmod.RemapIn(old_key=old_key), authorization="Bearer " + token)
            results.append(("ok", out.get("status")))
        except HTTPException as e:
            results.append(("err", e.status_code))

    first = threading.Thread(target=worker, args=("OLD-B1",))
    first.start()
    entered.wait(timeout=2)  # 原实现没有正式区查重，不会停在这里；继续让结果断言暴露缺陷
    second = threading.Thread(target=worker, args=("OLD-B2",))
    second.start()
    second.join(timeout=10)
    release.set()
    first.join(timeout=10)
    assert not first.is_alive() and not second.is_alive(), "并发折入未收束"

    assert ("err", 409) in results, results
    assert ("ok", "落库") in results, results
    rows = [x for x in _items(c, "/api/formal", h) if x["entity_key"] == "NEW-200"]
    assert len(rows) == 1, f"并发折入留下 {len(rows)} 行正式：{rows} / {results}"
    assert any(x["reason"] == "争用" for x in _items(c, "/api/conflicts", h)), results
    sources = _items(c, "/api/formal-sources?entity_key=NEW-200", h)
    assert {s["old_key"] for s in sources} == {"OLD-B1"}, "后到争用不得写来源"
    receipts = _items(c, "/api/remap-receipts", h)
    assert [x["status"] for x in receipts if x["entity_key"] == "NEW-200"].count("争用") == 1
    assert [x["status"] for x in receipts if x["entity_key"] == "NEW-200"].count("落库") == 1


def test_five_surfaces_agree_after_fold():
    c = TestClient(app)
    h = _login(c)
    assert c.post("/api/remap", headers=h, json={"old_key": "OLD-B1"}).status_code == 200
    assert c.post("/api/remap", headers=h, json={"old_key": "OLD-B2"}).status_code == 200

    formal = [x for x in _items(c, "/api/formal", h) if x["entity_key"] == "NEW-200"]
    sources = [x for x in _items(c, "/api/formal-sources", h) if x["entity_key"] == "NEW-200"]
    receipts = [x for x in _items(c, "/api/remap-receipts", h) if x["entity_key"] == "NEW-200"]
    mappings = [x for x in _items(c, "/api/mappings", h) if x["new_key"] == "NEW-200"]

    assert len(formal) == 1
    assert len({x["old_key"] for x in sources}) == 2
    assert len(receipts) == 2
    assert {x["old_key"] for x in mappings} == {"OLD-B1", "OLD-B2"}

    card = c.get("/api/verification", headers=h)
    assert card.status_code == 200, card.text
    body = card.json()
    assert body["ok"] is True, body
    assert body["checks"]["single_formal_per_key"] is True
    assert body["checks"]["source_in_mapping_ledger"] is True
    assert body["counts"]["formal_keys_for_new_200"] == 1


def test_source_write_failure_rolls_back_formal(monkeypatch):
    c = TestClient(app, raise_server_exceptions=False)
    h = _login(c)

    def boom(*args, **kwargs):
        raise RuntimeError("injected source write failure")

    monkeypatch.setattr(appmod, "_record_source", boom)
    r = c.post("/api/remap", headers=h, json={"old_key": "OLD-B1"})
    assert r.status_code >= 500, r.text

    rows = [x for x in _items(c, "/api/formal", h) if x["entity_key"] == "NEW-200"]
    sources = [x for x in _items(c, "/api/formal-sources", h) if x["entity_key"] == "NEW-200"]
    assert rows == [], "旁表失败后正式区不得留下半笔"
    assert sources == [], "旁表失败后来源旁表不得留下半笔"


def test_cut_explanation_locks_path_b_and_guard_rejects_deviation(monkeypatch):
    doc = Path(__file__).resolve().parents[1] / "docs" / "折映切断说明.md"
    text = doc.read_text(encoding="utf-8")
    assert "2026-11-20" in text
    assert "乙路径" in text
    assert "甲路径" in text and "不采用" in text

    c = TestClient(app)
    h = _login(c)
    monkeypatch.setattr(appmod, "CUT_PATH", "甲")
    r = c.post("/api/remap", headers=h, json={"old_key": "OLD-A1"})
    assert r.status_code == 409, r.text
    assert any(x["reason"] == "折映偏了" for x in _items(c, "/api/conflicts", h))


def test_verification_card_fails_when_only_display_merged(monkeypatch):
    """只改展示合并、不落旁表时，互证卡必须判失败。"""
    c = TestClient(app)
    h = _login(c)
    monkeypatch.setattr(appmod, "_record_source", lambda *a, **k: None)
    assert c.post("/api/remap", headers=h, json={"old_key": "OLD-B1"}).status_code == 200

    card = c.get("/api/verification", headers=h).json()
    assert card["ok"] is False, card
    assert card["checks"]["formal_primary_in_sources"] is False, card
    sources = _items(c, "/api/formal-sources", h)
    assert [s for s in sources if s["entity_key"] == "NEW-200"] == []

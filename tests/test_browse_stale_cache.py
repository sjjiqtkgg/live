"""浏览缓存（2026-10-16）单测：StaleTTLCache 软/硬过期语义 + browse_api 统一包装（stale-if-error）。

背景：浏览三接口（分区/推荐/分区房间）原为「五家有 600s 适配器内缓存 / 斗鱼无缓存 / 列表全程无缓存」；
现统一在 browse_api._browse_cached 包装：软 TTL 到期照常抓新；上游失败（基类守卫把异常转成
空态：cats=[] / {list:[]}）时回退「上次成功数据」（硬保留 24h）。
本文件钉住：软/硬过期语义、深拷贝隔离、上限驱逐、清扫、空态才回退、异常才回退、
空结果不落缓存、cookie 指纹分键、全链路（TestClient → 路由 → 缓存命中不触适配器方法）。
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import browse_api
from browse_cache import StaleTTLCache

SAMPLE_LIST: dict[str, Any] = {"list": [{"roomId": "1"}], "hasMore": False}
EMPTY_LIST: dict[str, Any] = {"list": [], "hasMore": False}


def _fresh_cache(monkeypatch: pytest.MonkeyPatch) -> tuple[StaleTTLCache, dict[str, float]]:
    """注入独立缓存实例 + 假时钟，避免与模块级 BROWSE_CACHE / 其它测试文件串扰。"""
    cache = StaleTTLCache(default_ttl=180.0, max_items=500)
    clock = {"t": 1_000_000.0}
    cache._now = lambda: clock["t"]  # type: ignore[method-assign]
    monkeypatch.setattr(browse_api, "BROWSE_CACHE", cache)
    return cache, clock


# ---------- A. StaleTTLCache 语义 ----------

def test_fresh_hit_and_deepcopy_isolation():
    c = StaleTTLCache()
    t = [100.0]
    c._now = lambda: t[0]  # type: ignore[method-assign]
    value = {"list": [{"roomId": "1"}]}
    c.set("k", value, 10)
    got = c.get("k")
    assert got == value
    got["list"][0]["roomId"] = "9"  # 下游原地改不污染缓存（深拷贝）
    assert c.get("k")["list"][0]["roomId"] == "1"


def test_soft_expiry_keeps_stale_until_hard():
    c = StaleTTLCache(grace=100.0)
    t = [0.0]
    c._now = lambda: t[0]  # type: ignore[method-assign]
    c.set("k", "v", 10)
    t[0] = 5
    assert c.get("k") == "v"           # 未过期：正常命中
    t[0] = 10
    assert c.get("k") is None          # 软过期：不再命中…
    assert c.get_stale("k") == "v"     # …但保留给兜底
    t[0] = 110                         # 硬过期（软 10 + grace 100）
    assert c.get("k") is None
    assert c.get_stale("k") is None


def test_sweep_removes_hard_expired_only():
    c = StaleTTLCache(grace=50.0, sweep_interval=10_000.0)
    t = [0.0]
    c._now = lambda: t[0]  # type: ignore[method-assign]
    c.set("a", 1, 5)
    c.set("b", 2, 5)
    t[0] = 6
    assert c.sweep() == 0              # 软过期但未到硬过期（55）：不清
    t[0] = 56
    assert c.sweep() == 2              # 清扫只清硬过期
    assert c.get_stale("a") is None and c.get_stale("b") is None


def test_max_items_evicts_soonest_hard():
    c = StaleTTLCache(max_items=2, grace=1000.0)
    t = [0.0]
    c._now = lambda: t[0]  # type: ignore[method-assign]
    c.set("a", 1, 10)
    t[0] = 1
    c.set("b", 2, 10)
    t[0] = 2
    c.set("c", 3, 10)                  # 超限：丢硬过期最早的 a
    assert c.get("a") is None
    assert c.get("b") == 2 and c.get("c") == 3


# ---------- B. _browse_cached 包装（stale-if-error） ----------

def test_cached_hit_skips_factory(monkeypatch: pytest.MonkeyPatch):
    calls = {"n": 0}

    async def factory():
        calls["n"] += 1
        return SAMPLE_LIST

    _fresh_cache(monkeypatch)
    r1 = asyncio.run(browse_api._browse_cached("zzt", "getRecommendRooms", (1, 24), "", factory))
    r2 = asyncio.run(browse_api._browse_cached("zzt", "getRecommendRooms", (1, 24), "", factory))
    assert r1 == r2 == SAMPLE_LIST
    assert calls["n"] == 1             # 第二次命中缓存，未触 factory


def test_soft_expiry_refetches(monkeypatch: pytest.MonkeyPatch):
    calls = {"n": 0}

    async def factory():
        calls["n"] += 1
        return SAMPLE_LIST

    _, clock = _fresh_cache(monkeypatch)
    asyncio.run(browse_api._browse_cached("zzt", "getRecommendRooms", (1, 24), "", factory))
    clock["t"] += 181                  # 推荐榜软 TTL 180s
    asyncio.run(browse_api._browse_cached("zzt", "getRecommendRooms", (1, 24), "", factory))
    assert calls["n"] == 2             # 到期照常抓新


def test_empty_state_falls_back_to_stale(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture):
    calls = {"n": 0}
    mode = {"empty": False}

    async def factory():
        calls["n"] += 1
        return EMPTY_LIST if mode["empty"] else SAMPLE_LIST

    _, clock = _fresh_cache(monkeypatch)
    asyncio.run(browse_api._browse_cached("zzt", "getCategoryRooms", (1, 24), "", factory))
    clock["t"] += 301                  # 分区房间软 TTL 300s
    mode["empty"] = True               # 上游转为失败（空态）
    with caplog.at_level(logging.WARNING, logger="live.browse"):
        r = asyncio.run(browse_api._browse_cached("zzt", "getCategoryRooms", (1, 24), "", factory))
    assert r == SAMPLE_LIST            # 回退上次成功数据
    assert calls["n"] == 2
    assert "stale-if-error" in caplog.text


def test_empty_state_without_stale_returns_empty_and_not_cached(monkeypatch: pytest.MonkeyPatch):
    calls = {"n": 0}

    async def factory():
        calls["n"] += 1
        return EMPTY_LIST

    _fresh_cache(monkeypatch)
    r1 = asyncio.run(browse_api._browse_cached("zzt", "getRecommendRooms", (1, 24), "", factory))
    r2 = asyncio.run(browse_api._browse_cached("zzt", "getRecommendRooms", (1, 24), "", factory))
    assert r1 == r2 == EMPTY_LIST      # 首次就失败：与现状一致（空页，不 500）
    assert calls["n"] == 2             # 空态不落缓存（否则 24h 空页）


def test_empty_cats_not_cached(monkeypatch: pytest.MonkeyPatch):
    calls = {"n": 0}

    async def factory():
        calls["n"] += 1
        return []

    _fresh_cache(monkeypatch)
    asyncio.run(browse_api._browse_cached("zzt", "getCategories", (), "", factory))
    asyncio.run(browse_api._browse_cached("zzt", "getCategories", (), "", factory))
    assert calls["n"] == 2


def test_exception_falls_back_to_stale_or_raises(monkeypatch: pytest.MonkeyPatch):
    mode = {"boom": False}

    async def factory():
        if mode["boom"]:
            raise RuntimeError("upstream exploded")
        return SAMPLE_LIST

    _, clock = _fresh_cache(monkeypatch)
    asyncio.run(browse_api._browse_cached("zzt", "getRecommendRooms", (1, 24), "", factory))
    clock["t"] += 181
    mode["boom"] = True
    r = asyncio.run(browse_api._browse_cached("zzt", "getRecommendRooms", (1, 24), "", factory))
    assert r == SAMPLE_LIST            # 有旧值：异常也兜底
    with pytest.raises(RuntimeError):  # 无旧值（新 key）：异常原样上抛保持可观测
        asyncio.run(browse_api._browse_cached("zzt", "getRecommendRooms", (2, 24), "", factory))


def test_cookie_fingerprint_isolates_keys(monkeypatch: pytest.MonkeyPatch):
    calls = {"n": 0}

    async def factory():
        calls["n"] += 1
        return SAMPLE_LIST

    _fresh_cache(monkeypatch)
    asyncio.run(browse_api._browse_cached("zzt", "getRecommendRooms", (1, 24), "a=1", factory))
    asyncio.run(browse_api._browse_cached("zzt", "getRecommendRooms", (1, 24), "a=2", factory))
    asyncio.run(browse_api._browse_cached("zzt", "getRecommendRooms", (1, 24), "a=1", factory))
    assert calls["n"] == 2             # 不同 cookie 分键；同 cookie 命中


# ---------- C. 全链路（路由 → 缓存；命中不触适配器方法） ----------

class _CountingAdapter:
    name = "counting"

    def __init__(self) -> None:
        self.cat_calls = 0
        self.mode_empty = False

    async def get_categories(self) -> list[dict[str, Any]]:
        self.cat_calls += 1
        if self.mode_empty:
            return []
        return [{"id": "1", "name": "网游", "pic": "", "children": []}]

    async def close(self) -> None:
        pass


class _CountingRegistry:
    def __init__(self, adapter: _CountingAdapter) -> None:
        self.adapter = adapter

    def get(self, site: str, cookie: str = ""):
        return self.adapter if site.strip().lower() == "huya" else None


def test_route_level_cache_and_stale(monkeypatch: pytest.MonkeyPatch):
    _, clock = _fresh_cache(monkeypatch)
    adapter = _CountingAdapter()
    monkeypatch.setattr(browse_api, "registry", _CountingRegistry(adapter))
    app = FastAPI()
    app.include_router(browse_api.router)
    with TestClient(app) as tc:
        r1 = tc.get("/api/huya/getCategories")
        r2 = tc.get("/api/huya/getCategories")
        assert r1.status_code == r2.status_code == 200
        assert r1.json() == r2.json()
        assert adapter.cat_calls == 1  # 第二次命中缓存，未触适配器方法

        # 软过期（cats 24h）+ 上游转失败（空态）→ 全链路 stale-if-error 回退旧数据
        clock["t"] += 24 * 3600 + 1
        adapter.mode_empty = True
        r3 = tc.get("/api/huya/getCategories")
        assert r3.status_code == 200
        assert r3.json()["categories"][0]["name"] == "网游"
        assert adapter.cat_calls == 2

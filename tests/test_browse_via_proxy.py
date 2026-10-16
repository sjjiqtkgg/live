"""BROWSE_VIA_PROXY 试验开关回归：国内平台浏览请求可改走国内代理池（默认直连）。"""
from __future__ import annotations

import asyncio

import httpx

import main
import browse_platforms.common as common


def _resp(method: str, url: str, payload: dict):
    return httpx.Response(200, json=payload, request=httpx.Request(method, url))


def test_domain_and_switch_matching(monkeypatch):
    monkeypatch.setattr(common, "POOL_FETCH", object())  # 只需非空，表达“已装配”
    monkeypatch.setattr(common, "_BROWSE_VIA_PROXY", True)
    # 国内平台域（含子域）命中
    assert common._browse_via_proxy("https://www.huya.com/index")
    assert common._browse_via_proxy("https://m.douyu.com/room")
    assert common._browse_via_proxy("https://apic.douyucdn.cn/api")
    assert common._browse_via_proxy("https://api.live.bilibili.com/x")
    assert common._browse_via_proxy("https://live.douyin.com/123")
    # 海外平台 / 后缀伪装 → 不命中
    assert not common._browse_via_proxy("https://gql.twitch.tv/gql")
    assert not common._browse_via_proxy("https://sch.sooplive.co.kr/x")
    assert not common._browse_via_proxy("https://api.pandalive.co.kr/x")
    assert not common._browse_via_proxy("https://evilhuya.com/x")
    # 开关关闭 → 一律直连
    monkeypatch.setattr(common, "_BROWSE_VIA_PROXY", False)
    assert not common._browse_via_proxy("https://www.huya.com/x")


def test_not_configured_falls_back_to_direct(monkeypatch):
    # 开关开了但未装配池取函数（standalone 加载期）→ 退回直连，不炸
    monkeypatch.setattr(common, "_BROWSE_VIA_PROXY", True)
    monkeypatch.setattr(common, "POOL_FETCH", None)

    class FakeClient:
        async def get(self, url, params=None, headers=None):
            return _resp("GET", url, {"direct": True})

    h = common.Http()

    async def fake_get():
        return FakeClient()

    monkeypatch.setattr(h, "_get", fake_get)
    assert asyncio.run(h.get_json("https://www.huya.com/x")) == {"direct": True}


def test_get_json_via_pool_when_enabled(monkeypatch):
    calls = []

    async def fake_pool(method, url, **kwargs):
        calls.append((method, url, kwargs))
        return _resp(method, url, {"ok": True})

    monkeypatch.setattr(common, "_BROWSE_VIA_PROXY", True)
    monkeypatch.setattr(common, "POOL_FETCH", fake_pool)

    out = asyncio.run(common.Http(extra_headers={"a": "b"}).get_json(
        "https://www.douyu.com/japi/room", params={"x": 1}))
    assert out == {"ok": True}
    assert len(calls) == 1
    method, url, kwargs = calls[0]
    assert method == "GET"
    assert kwargs["params"] == {"x": 1}
    assert kwargs["log_tag"] == "browse"
    assert kwargs["headers"]["a"] == "b", "适配器 extra_headers 必须随请求透传"


def test_overseas_stays_direct_even_when_enabled(monkeypatch):
    async def boom(*a, **k):
        raise AssertionError("海外域名不应走代理池")

    monkeypatch.setattr(common, "_BROWSE_VIA_PROXY", True)
    monkeypatch.setattr(common, "POOL_FETCH", boom)

    class FakeClient:
        async def get(self, url, params=None, headers=None):
            return _resp("GET", url, {"direct": True})

    h = common.Http()

    async def fake_get():
        return FakeClient()

    monkeypatch.setattr(h, "_get", fake_get)
    assert asyncio.run(h.get_json("https://gql.twitch.tv/gql")) == {"direct": True}


def test_main_wires_pool_fetch_hook():
    # main 启动装配后 hook 必须已注入（否则开关形同虚设）
    assert common.POOL_FETCH is not None

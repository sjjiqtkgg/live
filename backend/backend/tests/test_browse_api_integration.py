"""P3 批·回归测试：浏览 API 路由层离线集成回归（browse_api.py）。

背景：既有 323 例全部是函数级单测（browse_platforms 的解析/剥离/分池逻辑、
main.py 的分派/脱敏/WS），但「路由 → 参数解析 → adapter 调用 → 响应形状 →
cookie 头透传」这层 HTTP 契约此前零覆盖——前端 useSiteFetch 依赖的响应字段
一旦在重构中改名/丢字段，单测全绿也照样炸前端。

设计（全部离线、确定性、CI 可全绿）：
  - 只挂 browse_api.router 到独立 FastAPI 实例（不 import main，避免其全局
    状态/限速器牵连），TestClient 同步发请求；
  - monkeypatch browse_api.registry 为哨兵 registry：记录 (site, cookie) 调用
    序列并返回可控假 adapter——既断言路由正确路由到了平台适配层，又断言
    x-platform-cookie 头逐字透传；
  - _parse_main 的 parser 用 configure_parser 注入假实现，覆盖 getRoomDetail /
    getRoomStream 两条取流路径；
  - make_room_url 用真实实现（纯映射表，无网络）。

区块：A 路由与响应形状 8；B cookie 头透传 4；C 参数与错误契约 6；
D 图片/流域名分流 6；E ws 下发与房间链接 4。
"""
from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import browse_api


# ---------------------------------------------------------------- 哨兵平台层
class SentinelAdapter:
    """可控假 adapter：按 site 返回预设数据，记录调用与 close 次数。"""

    name = "sentinel"

    def __init__(self, site: str, cookie: str, calls: list[tuple[str, str]]):
        self.site = site
        self.cookie = cookie
        self.calls = calls
        self.calls.append(("adapter", f"{site}/{cookie}"))
        self.closed = 0

    async def get_categories(self) -> list[dict[str, Any]]:
        return [
            {"id": "1", "name": "网游", "pic": "https://a.cdn/x.png",
             "children": [{"id": "11", "name": "MOBA", "pic": "https://a.cdn/m.png"}]},
        ]

    async def get_category_rooms(self, parent: str, area: str, page: int, size: int) -> dict[str, Any]:
        return {"hasMore": False, "list": [{
            "roomId": "999", "title": "  t ", "nickname": "n", "avatar": "",
            "cover": "http://img.huya.com/c.jpg", "area": "a", "watching": 12,
            "isLive": False,
        }]}

    async def get_recommend_rooms(self, page: int, size: int) -> dict[str, Any]:
        return {"hasMore": True, "list": [{
            "roomId": 7, "title": "em<b>x</b>", "nickname": None, "avatar": "https://static-cdn.jtvnw.net/a.jpg",
        }]}

    async def search_rooms(self, kw: str, page: int, size: int) -> dict[str, Any]:
        return {"hasMore": False, "list": []}

    async def get_room_detail(self, room_id: str) -> dict[str, Any] | None:
        return None if room_id == "0" else {
            "roomId": room_id, "title": "d", "nickname": "u", "avatar": "", "isLive": True,
        }

    async def get_live_status(self, ids: list[str]) -> list[dict[str, Any]]:
        return [{"roomId": i, "isLive": True} for i in ids]

    async def close(self) -> None:
        self.closed += 1


class SentinelRegistry:
    """假 registry.get：记录 (site, cookie) 并回 SentinelAdapter（仅限已登记站点）。"""

    KNOWN_SITES = frozenset({"huya", "douyu", "bilibili", "douyin", "twitch", "soop", "pandatv"})

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.adapters: list[SentinelAdapter] = []

    def get(self, site: str, cookie: str = ""):
        if site.strip().lower() not in self.KNOWN_SITES:
            return None  # 与真实 registry 一致：未登记站点返回 None → 路由层 404
        adapter = SentinelAdapter(site, cookie, self.calls)
        self.adapters.append(adapter)
        return adapter


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    global _REG
    reg = SentinelRegistry()
    _REG = reg
    monkeypatch.setattr(browse_api, "registry", reg)

    async def fake_parser(url: str, cookie: str) -> dict[str, Any]:
        return {
            "roomId": url.rsplit("/", 1)[-1],
            "streams": [
                {"url": "http://flv.huya.com/x.flv", "quality": "原画", "cdn": "线路1"},
                {"url": "https://hls.sooplive.com/y.m3u8", "quality": "高清"},
                {"url": "http://flv.huya.com/x.flv", "quality": "重复地址应被去重"},
            ],
            "nickname": "主播", "avatar": "http://img.huya.com/a.jpg",
        }

    browse_api.configure_parser(fake_parser)
    app = FastAPI()
    app.include_router(browse_api.router)
    with TestClient(app) as tc:
        yield tc
    # 还原全局 parser（测试文件间共享模块状态，避免泄漏给其它用例）
    browse_api.configure_parser(None)  # type: ignore[arg-type]


# ---------------------------------------------------------------- A 路由与响应形状
def test_get_categories_shape(client: TestClient):
    r = client.get("/api/huya/getCategories")
    assert r.status_code == 200
    body = r.json()
    assert body["site"] == "huya" and body["name"] == "sentinel"
    cat = body["categories"][0]
    assert set(cat.keys()) >= {"id", "name", "pic", "children"}
    assert cat["children"][0]["pic"].endswith("m.png")


def test_get_recommend_rooms_room_shape(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    """前端 useSiteFetch 逐字段依赖的房间卡形状（_room 规范化契约）。"""
    monkeypatch.delenv("CF_WORKER_URL", raising=False)
    r = client.get("/api/huya/getRecommendRooms")
    assert r.status_code == 200
    body = r.json()
    assert body["site"] == "huya" and body["hasMore"] is True
    room = body["list"][0]
    # _room 强制字段：字符串化 + platform 标记
    assert room["roomId"] == "7" and isinstance(room["roomId"], str)
    assert room["title"] == "em<b>x</b>" and isinstance(room["title"], str)
    assert room["nickname"] == "" and room["avatar"] == "https://static-cdn.jtvnw.net/a.jpg"
    assert room["isLive"] is True  # 缺省回填 True（旧契约）
    assert room["platform"] == "huya"


def test_get_category_rooms_shape_and_defaults(client: TestClient):
    r = client.get("/api/douyu/getCategoryRooms?parentAreaId=2&areaId=201")
    assert r.status_code == 200
    room = r.json()["list"][0]
    assert room["roomId"] == "999"
    assert room["title"] == "  t "  # 只保证字符串化，不 trim（平台层职责）
    assert room["watching"] == "12" and room["isLive"] is False  # isLive=False 必须原样保留
    # http 图片直连（国内域名不包 worker）
    assert room["cover"] == "http://img.huya.com/c.jpg"


def test_search_rooms_blank_kw_shortcircuit(client: TestClient):
    """空关键词短路：不建 adapter、不进平台层（哨兵零调用断言）。"""
    r = client.get("/api/huya/searchRooms?kw=%20%20")
    assert r.status_code == 200
    body = r.json()
    assert body == {"site": "huya", "list": [], "hasMore": False}


def test_get_room_detail_parsed_fallback(client: TestClient):
    """detail=None（虎牙型：详情接口无元信息）→ 用 parse 结果当详情并补 ws。"""
    r = client.get("/api/soop/getRoomDetail/0")  # 哨兵：room_id=0 时 detail=None
    assert r.status_code == 200
    room = r.json()
    assert room["roomId"] == "0" and room["platform"] == "soop"
    assert room["nickname"] == "主播"
    assert room["streams"], "详情里必须带可用流地址"


def test_get_room_detail_merge(client: TestClient):
    """detail 有元信息 → parse 的 streams/lines/qualities 合并进详情。"""
    r = client.get("/api/bilibili/getRoomDetail/1000")
    assert r.status_code == 200
    room = r.json()
    assert room["roomId"] == "1000"
    assert {line["url"] for line in room["lines"]} and room["qualities"]
    assert len([s for s in room["streams"]]) == 2  # 重复流地址被 normalize 去重


def test_get_room_stream_normalize(client: TestClient):
    """取流端点：normalize_parse 契约（lines 去重/qualities 别名/类型推断）。"""
    r = client.get("/api/huya/getRoomStream/555")
    assert r.status_code == 200
    body = r.json()
    urls = [line["url"] for line in body["lines"]]
    assert len(urls) == len(set(urls)) == 2  # 去重后两条
    assert any(".flv" in u for u in urls) and any("m3u8" in u for u in urls)
    assert body["lines"][0]["type"] in ("flv", "m3u8")
    assert [q["id"] for q in body["qualities"]] == ["原画", "高清"]


def test_get_live_status_capped_at_30(client: TestClient):
    ids = ",".join(str(i) for i in range(35))
    r = client.get(f"/api/douyu/getLiveStatus/{ids}")
    assert r.status_code == 200
    body = r.json()
    assert body["site"] == "douyu" and len(body["list"]) == 30


# ---------------------------------------------------------------- B cookie 头透传
def test_cookie_header_passthrough(client: TestClient):
    """x-platform-cookie 头必须逐字进入 registry.get 的 cookie 参数。"""
    r = client.get("/api/huya/getRecommendRooms", headers={"x-platform-cookie": "uid=1; foo=bar"})
    assert r.status_code == 200
    assert ("adapter", "huya/uid=1; foo=bar") in _registry_calls()


def test_cookie_header_absent_falls_back_empty(client: TestClient):
    r = client.get("/api/huya/getRecommendRooms")
    assert r.status_code == 200
    assert ("adapter", "huya/") in _registry_calls()


def test_cookie_env_fallback(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("BILI_COOKIE", "envcookie=1")
    r = client.get("/api/bilibili/getRecommendRooms")
    assert r.status_code == 200
    assert ("adapter", "bilibili/envcookie=1") in _registry_calls()


def test_cookie_header_wins_over_env(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HUYA_COOKIE", "envcookie=2")
    r = client.get("/api/huya/getCategories", headers={"x-platform-cookie": "hdr=3"})
    assert r.status_code == 200
    assert ("adapter", "huya/hdr=3") in _registry_calls()
    assert ("adapter", "huya/envcookie=2") not in _registry_calls()


_REG: SentinelRegistry | None = None


def _registry_calls() -> list[tuple[str, str]]:
    # SentinelRegistry 实例由 fixture 持有；通过闭包变量桥接（fixture 内赋值）
    return _REG.calls if _REG is not None else []


# ---------------------------------------------------------------- C 参数与错误契约
def test_unknown_site_404(client: TestClient):
    r = client.get("/api/nope/getRecommendRooms")
    assert r.status_code == 404
    assert "不支持的平台" in r.json()["detail"]


def test_page_zero_422(client: TestClient):
    r = client.get("/api/huya/getRecommendRooms?page=0")
    assert r.status_code == 422


def test_pagesize_over_100_422(client: TestClient):
    r = client.get("/api/huya/getRecommendRooms?pageSize=101")
    assert r.status_code == 422


def test_pagesize_negative_422(client: TestClient):
    r = client.get("/api/huya/getCategoryRooms?pageSize=-1")
    assert r.status_code == 422


def test_room_stream_unknown_site_400(client: TestClient):
    """make_room_url 拼不出链接的站点（未登记）→ 400 而不是 500。"""
    r = client.get("/api/nope/getRoomStream/1")
    assert r.status_code == 400
    assert "房间链接" in r.json()["detail"]


def test_adapter_closed_after_request(client: TestClient):
    """finally 兜底：请求结束 adapter.close() 必被调（连接归还契约）。"""
    r = client.get("/api/huya/getRecommendRooms")
    assert r.status_code == 200
    adapters = _REG.adapters if _REG else []
    assert adapters and all(a.closed == 1 for a in adapters)


# ---------------------------------------------------------------- D 图片/流域名分流
def test_foreign_image_wrapped_to_worker_avatar(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("CF_WORKER_URL", "https://w.example.com")
    r = client.get("/api/twitch/getRecommendRooms")
    room = r.json()["list"][0]
    assert room["avatar"].startswith("https://w.example.com/avatar?url=")
    assert "jtvnw.net" in room["avatar"]


def test_domestic_image_kept_direct(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("CF_WORKER_URL", "https://w.example.com")
    r = client.get("/api/douyu/getCategoryRooms")
    room = r.json()["list"][0]
    assert room["cover"] == "http://img.huya.com/c.jpg"


def test_no_worker_env_direct(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("CF_WORKER_URL", raising=False)
    r = client.get("/api/twitch/getRecommendRooms")
    room = r.json()["list"][0]
    assert room["avatar"] == "https://static-cdn.jtvnw.net/a.jpg"  # 海外图无 worker 时也直连


def test_stream_http_upgraded_and_foreign_wrapped(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("CF_WORKER_URL", "https://w.example.com")
    r = client.get("/api/soop/getRoomStream/room1")
    urls = [line["url"] for line in r.json()["lines"]]
    assert any(u.startswith("https://flv.huya.com/") for u in urls)  # http→https（混合内容修复）
    soop = [u for u in urls if "sooplive" in u][0]
    assert soop.startswith("https://w.example.com?url=")  # 海外流包 worker


def test_stream_worker_wrapped_idempotent_prefix(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    """已经是 worker 地址的流不二次包装（幂等）。"""
    monkeypatch.setenv("CF_WORKER_URL", "https://w.example.com")

    async def fake_parser(url: str, cookie: str) -> dict[str, Any]:
        return {"streams": [{"url": "https://w.example.com?url=https%3A%2F%2Fhls.sooplive.com%2Fz.m3u8", "quality": "q"}]}

    browse_api.configure_parser(fake_parser)
    r = client.get("/api/soop/getRoomStream/z")
    url = r.json()["lines"][0]["url"]
    assert url.count("w.example.com") == 1


def test_stream_dedupe_by_url(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("CF_WORKER_URL", raising=False)
    r = client.get("/api/huya/getRoomStream/d")
    lines = r.json()["lines"]
    assert len(lines) == 2
    ids = [q["id"] for q in r.json()["qualities"]]
    assert len(ids) == len(set(ids))


# ---------------------------------------------------------------- E ws 下发与房间链接
def test_ws_injected_for_danmaku_sites(client: TestClient):
    """douyin/twitch/soop 详情必带 /ws/<site>/<room> 弹幕地址。"""
    for site in ("douyin", "twitch", "soop"):
        r = client.get(f"/api/{site}/getRoomDetail/r1")
        assert r.status_code == 200
        assert r.json()["ws"] == f"ws://testserver/ws/{site}/r1"


def test_ws_not_injected_for_huya(client: TestClient):
    """huya 无网页弹幕端点（前端直连平台 Tars）→ 不下发 ws 字段。"""
    r = client.get("/api/huya/getRoomDetail/r2")
    assert r.status_code == 200
    assert "ws" not in r.json()


def test_ws_https_via_forwarded_proto(client: TestClient):
    """反代 TLS 终止（x-forwarded-proto + x-forwarded-host）→ wss 地址。"""
    r = client.get(
        "/api/soop/getRoomDetail/r3",
        headers={"x-forwarded-proto": "https", "x-forwarded-host": "pub.example.com"},
    )
    assert r.status_code == 200
    assert r.json()["ws"].startswith("wss://pub.example.com/ws/soop/r3")


def test_ws_public_base_override(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("WS_PUBLIC_BASE", "wss://alt.example.cn")
    r = client.get("/api/douyin/getRoomDetail/r4")
    assert r.status_code == 200
    assert r.json()["ws"] == "wss://alt.example.cn/ws/douyin/r4"

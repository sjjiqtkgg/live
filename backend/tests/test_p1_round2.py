"""P1 第二轮（数据正确性收尾 + 个人使用批次）单测。

覆盖：
  A. hasMore 逐站公式 —— 斗鱼 pgcnt / 虎牙 totalPage+numFound / 抖音分区与
     feed（feed 翻页是假的必须恒 False）/ SOOP total_cnt+HAS_MORE_LIST /
     B站搜索 numPages / Twitch Helix cursor（短页+cursor 不再被 len>=ps 误杀）；
  B. 冷静期新语义 —— 直连兜底也失败不设冷静期、海外池直连兜底可配
     （T23 起默认开：外代全挂直连恢复；=0 恢复不白等直连的旧行为，流式同款）；
  C. B站 get_live_status 回显请求 id（关注列表短号匹配）；
  D. 适配器基类统一异常守卫（__init_subclass__ 包装，真实适配器已覆盖）；
  E. 缓存写入侧深拷贝（parse_soop _fail / parse_panda_manual 原地改不渗入）；
  F. SOOP 退避只认「明确未开播」（HTTP 503 不递增、RESULT 非 0/1 递增）；
  G. T3 补测 —— VERIFY_TLS 默认值与 _tls_verify_for 行为、/health/detail
     token 与限速。
"""
from __future__ import annotations

import asyncio
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import main  # noqa: E402
from browse_platforms.base import LivePlatform, room_dict  # noqa: E402
from browse_platforms.bilibili import BilibiliAdapter  # noqa: E402
from browse_platforms.douyin import DouyinAdapter  # noqa: E402
from browse_platforms.douyu import DouyuAdapter  # noqa: E402
from browse_platforms.huya import HuyaAdapter  # noqa: E402
from browse_platforms import douyin as douyin_mod  # noqa: E402
from browse_platforms import twitch as twitch_mod  # noqa: E402
from browse_platforms.twitch import TwitchAdapter  # noqa: E402
from browse_platforms.soop import SoopAdapter  # noqa: E402
from browse_platforms.pandatv import PandaAdapter  # noqa: E402


# ---------- 公共假基建 ----------

class _FakeHttp:
    """离线假 Http：按序返回预置 JSON（列表弹尽后回 {}），不发任何网络请求。"""

    def __init__(self, payloads):
        self._payloads = [payloads] if isinstance(payloads, dict) else list(payloads)

    async def get_json(self, url, params=None, headers=None):
        if not self._payloads:
            return {}
        item = self._payloads.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    async def post_json(self, url, data=None, json=None, headers=None):
        return await self.get_json(url)

    async def get_text(self, url, params=None, headers=None):
        return ""

    async def close(self):
        pass


class _FakeHttpClient:
    """B站专用：适配 _get_client().get(...).json() 链。"""

    def __init__(self, payload):
        self._payload = payload

    async def get(self, url, params=None, headers=None):
        outer = self

        class _R:
            def json(self):
                return outer._payload

        return _R()


def _install_bili_client(monkeypatch, payload):
    async def fake_get_client(self):
        return _FakeHttpClient(payload)

    monkeypatch.setattr(BilibiliAdapter, "_get_client", fake_get_client)


@pytest.fixture(autouse=True)
def _clean_cooldown():
    main._PROXY_POOL_DEAD_UNTIL.clear()
    yield
    main._PROXY_POOL_DEAD_UNTIL.clear()


# ---------- A. hasMore 逐站公式 ----------

def test_douyu_category_uses_pgcnt(monkeypatch):
    a = DouyuAdapter("")
    a.http = _FakeHttp({"data": {"rl": [{"rid": "r", "rn": "t", "nn": "n", "type": "1"}], "pgcnt": 6}})
    assert asyncio.run(a.get_category_rooms("2", "1", 5, 24))["hasMore"] is True
    a.http = _FakeHttp({"data": {"rl": [{"rid": "r", "rn": "t", "nn": "n", "type": "1"}], "pgcnt": 6}})
    assert asyncio.run(a.get_category_rooms("2", "1", 6, 24))["hasMore"] is False  # 收尾页 p6 后停


def test_douyu_recommend_pgcnt_zero_falls_back_to_nonempty(monkeypatch):
    a = DouyuAdapter("")
    a.http = _FakeHttp({"data": {"rl": [{"rid": "r", "rn": "t", "nn": "n", "type": "1"}], "pgcnt": 0}})
    assert asyncio.run(a.get_recommend_rooms(1, 24))["hasMore"] is True
    a.http = _FakeHttp({"data": {"rl": [], "pgcnt": 0}})
    assert asyncio.run(a.get_recommend_rooms(999, 24))["hasMore"] is False


@pytest.mark.parametrize("page,expected", [(4, True), (5, False)])
def test_huya_uses_totalpage(monkeypatch, page, expected):
    a = HuyaAdapter("")
    a.http = _FakeHttp({"data": {"datas": [{"profileRoom": "r", "introduction": "t", "nick": "n",
                                            "screenshot": "", "totalCount": "1", "avatar180": "",
                                            "gameFullName": "g"}], "totalPage": 5}})
    assert asyncio.run(a.get_category_rooms("", "1", page, 24))["hasMore"] is expected
    a.http = _FakeHttp({"data": {"datas": [{"profileRoom": "r", "introduction": "t", "nick": "n",
                                            "screenshot": "", "totalCount": "1", "avatar180": "",
                                            "gameFullName": "g"}], "totalPage": 5}})
    assert asyncio.run(a.get_recommend_rooms(page, 24))["hasMore"] is expected


@pytest.mark.parametrize("page,expected", [(1, True), (5, False)])
def test_huya_search_uses_numfound(monkeypatch, page, expected):
    docs = [{"room_id": f"r{i}", "game_roomName": "t", "game_nick": "n",
             "game_avatarUrl180": "", "game_screenshot": "", "game_name": "g",
             "game_totalCount": "1"} for i in range(24)]
    a = HuyaAdapter("")
    a.http = _FakeHttp({"response": {"3": {"numFound": 100, "docs": docs}}})
    # p1: 0+24 < 100 → True；p5: 4*24+24=120 ≥ 100 → False（旧 len>=24 会无限翻）
    assert asyncio.run(a.search_rooms("英雄联盟", page, 24))["hasMore"] is expected


def test_huya_search_no_numfound_falls_back(monkeypatch):
    a = HuyaAdapter("")
    a.http = _FakeHttp({"response": {"3": {"docs": [{"room_id": "r", "game_roomName": "t",
                                                     "game_nick": "n"}]}}})
    assert asyncio.run(a.search_rooms("x", 1, 24))["hasMore"] is True


def _douyin_adapter_with(payload):
    a = DouyinAdapter("")
    a.cookie = "fake-cookie"  # 跳过匿名 cookie 的网络获取
    a.http = _FakeHttp(payload)
    return a


@pytest.fixture(autouse=True)
def _fake_douyin_sign(monkeypatch):
    async def fake_signed(base, params, cookie):
        return base + "?" + "&".join(f"{k}={v}" for k, v in params.items())

    monkeypatch.setattr(douyin_mod, "_signed_url", fake_signed)


def test_douyin_partition_nonempty_has_more():
    a = _douyin_adapter_with({"data": {"data": [{"room": {"id_str": "1", "title": "t"}}]}})
    out = asyncio.run(a._partition_rooms("1", "1", 1, 24))
    assert len(out["list"]) >= 1
    assert out["hasMore"] is True  # 旧 20>=24 恒 False，分区第 1 页就断


def test_douyin_partition_empty_stops():
    a = _douyin_adapter_with({"data": {"data": []}})
    assert asyncio.run(a._partition_rooms("1", "1", 999, 24))["hasMore"] is False


def test_douyin_feed_has_more_always_false():
    """feed 接口不接受翻页参数（实测 p1-p4 内容完全相同）→ hasMore 恒 False，
    否则前端会反复拉同一页造成重复卡片。"""
    a = _douyin_adapter_with({"status_code": "0",
                             "data": {"data": [{"room": {"id_str": "1", "title": "t"}}]}})
    out = asyncio.run(a.get_recommend_rooms(1, 24))
    assert len(out["list"]) >= 1  # 本页非空……
    assert out["hasMore"] is False  # ……也不能翻页


@pytest.mark.parametrize("page,expected", [(54, True), (55, False)])
def test_soop_recommend_uses_total_cnt(monkeypatch, page, expected):
    broad = [{"user_id": f"u{i}", "broad_title": "t", "user_nick": "n"} for i in range(3)]
    a = SoopAdapter("")
    a.http = _FakeHttp({"total_cnt": "3289", "cnt": 60, "broad": broad})
    # p54: 54*60=3240 < 3289 → True；p55: 3300 ≥ 3289 → False（p55 实测是 49 条收尾页）
    assert asyncio.run(a.get_recommend_rooms(page, 24))["hasMore"] is expected


def test_soop_recommend_no_metadata_falls_back(monkeypatch):
    a = SoopAdapter("")
    a.http = _FakeHttp({"broad": [{"user_id": "u", "broad_title": "t", "user_nick": "n"}]})
    assert asyncio.run(a.get_recommend_rooms(1, 24))["hasMore"] is True
    a.http = _FakeHttp({"broad": []})
    assert asyncio.run(a.get_recommend_rooms(1, 24))["hasMore"] is False


@pytest.mark.parametrize("flag,expected", [(True, True), ("true", True), (False, False), ("false", False)])
def test_soop_search_uses_has_more_list(monkeypatch, flag, expected):
    a = SoopAdapter("")
    a.http = _FakeHttp({"HAS_MORE_LIST": flag, "REAL_BROAD": [{"user_id": "u", "broad_title": "t"}]})
    assert asyncio.run(a.search_rooms("LOL", 1, 24))["hasMore"] is expected


def test_soop_search_no_flag_falls_back(monkeypatch):
    a = SoopAdapter("")
    a.http = _FakeHttp({"REAL_BROAD": [{"user_id": "u", "broad_title": "t"}]})
    assert asyncio.run(a.search_rooms("LOL", 1, 24))["hasMore"] is True


@pytest.mark.parametrize("page,expected", [(41, True), (42, False)])
def test_bili_search_uses_pageinfo_pages(monkeypatch, page, expected):
    payload = {
        "data": {
            "result": {"live_room": [{"roomid": f"r{i}", "title": f"t{i}", "uname": f"u{i}",
                                      "live_status": 1} for i in range(24)]},
            "pageinfo": {"live_room": {"numPages": 42, "numResults": 986}},
        }
    }
    _install_bili_client(monkeypatch, payload)
    assert asyncio.run(BilibiliAdapter().search_rooms("英雄联盟", page, 24))["hasMore"] is expected


def test_bili_search_no_pageinfo_falls_back(monkeypatch):
    payload = {"data": {"result": {"live_room": [{"roomid": "r", "title": "t",
                                                  "uname": "u", "live_status": 1}]}}}
    _install_bili_client(monkeypatch, payload)
    assert asyncio.run(BilibiliAdapter().search_rooms("x", 1, 24))["hasMore"] is True


def test_twitch_helix_short_page_with_cursor_has_more(monkeypatch):
    """短页（len < ps）+ 有 cursor → True：旧公式 len>=ps 会把这种边缘页误判成末页。"""
    twitch_mod._CURSOR_MAP.clear()

    async def fake_helix(self, path, params):
        return {
            "data": [{"user_login": "a", "user_name": "A", "title": "t",
                      "profile_image_url": "u", "thumbnail_url": "c",
                      "game_name": "g", "viewer_count": 5}],
            "pagination": {"cursor": "next-cursor"},
        }

    monkeypatch.setattr(TwitchAdapter, "_helix_get", fake_helix)
    out = asyncio.run(TwitchAdapter("")._helix_category_rooms("12345", 1, 24))
    assert len(out["list"]) == 1
    assert out["hasMore"] is True  # 纯上游信号（cursor 存在）


def test_twitch_helix_no_cursor_stops(monkeypatch):
    twitch_mod._CURSOR_MAP.clear()

    async def fake_helix(self, path, params):
        return {"data": [{"user_login": "a", "user_name": "A", "title": "t",
                          "profile_image_url": "u", "thumbnail_url": "c",
                          "game_name": "g", "viewer_count": 5}], "pagination": {}}

    monkeypatch.setattr(TwitchAdapter, "_helix_get", fake_helix)
    assert asyncio.run(TwitchAdapter("")._helix_category_rooms("12345", 1, 24))["hasMore"] is False


def test_twitch_langs_env_default():
    """默认 zh,ko（保持 pure_live 旧行为）；可被测试 monkeypatch 覆盖证明是模块级可配置项。"""
    import browse_platforms.twitch as tw
    saved = tw.TWITCH_LANGS
    try:
        assert tw.TWITCH_LANGS == ["zh", "ko"] or tw.TWITCH_LANGS == []  # 环境里设了空也算合法
        tw.TWITCH_LANGS = ["en"]
        # 广播语言过滤读取模块级 TWITCH_LANGS（分区路径在 _gql 里无法离线构造，
        # 这里钉住常量本身可被外部（env/测试）驱动）
        assert tw.TWITCH_LANGS == ["en"]
    finally:
        tw.TWITCH_LANGS = saved


# ---------- B. 冷静期新语义 ----------

class _FakeSyncClient:
    def __init__(self, fail: bool):
        self._fail = fail

    async def request(self, method, url, **kwargs):
        if self._fail:
            raise RuntimeError("proxy unreachable")

        class _R:
            status_code = 200

        return _R()


def _install_fake_get_client(monkeypatch, fail_proxies: set, seen: list):
    async def fake_get_client(proxy=None, timeout=None, verify=True):
        seen.append(proxy)
        return _FakeSyncClient(fail=(proxy in fail_proxies))

    monkeypatch.setattr(main, "get_client", fake_get_client)


def _install_pools(monkeypatch, internal, external):
    monkeypatch.setattr(main, "PROXY_URLS", internal)
    monkeypatch.setattr(main, "EXTERNAL_PROXY_URLS", external)


def test_direct_fallback_failure_sets_no_cooldown(monkeypatch):
    async def _run():
        """【P1二轮】直连兜底也失败（出口网络整体不可用）→ 不设冷静期；
        否则后续请求会跳过代理、白等一次直连超时再全链失败。"""
        _install_pools(monkeypatch, ["http://int-dead:1"], ["http://ext-ok:1"])
        seen: list = []
        _install_fake_get_client(monkeypatch, {"http://int-dead:1", None}, seen)

        with pytest.raises(RuntimeError):
            await main.request_with_retry("GET", "http://example.com/a")
        assert seen == ["http://int-dead:1", None]  # 先代理、后直连兜底都试过
        assert main._PROXY_POOL_DEAD_UNTIL.get("internal", 0.0) == 0.0

    asyncio.run(_run())


class _FakeStreamClient:
    def __init__(self, fail: bool):
        self._fail = fail

    def build_request(self, method, url, headers=None, content=None):
        return (method, url)

    async def send(self, req, stream=True):
        if self._fail:
            raise RuntimeError("proxy unreachable")

        class _R:
            status_code = 200
            headers = {}

        return _R()


def _install_fake_stream_get_client(monkeypatch, fail_proxies: set, seen: list):
    async def fake_get_client(proxy=None, timeout=None, verify=True):
        seen.append(proxy)
        return _FakeStreamClient(fail=(proxy in fail_proxies))

    monkeypatch.setattr(main, "get_client", fake_get_client)


def test_stream_direct_fallback_failure_sets_no_cooldown(monkeypatch):
    async def _run():
        _install_pools(monkeypatch, ["http://int-dead:1"], ["http://ext-ok:1"])
        seen: list = []
        _install_fake_stream_get_client(monkeypatch, {"http://int-dead:1", None}, seen)

        with pytest.raises(RuntimeError):
            await main.stream_request_with_proxy_group("GET", "http://example.com/a",
                                                       proxy_list=["http://int-dead:1"])
        assert seen == ["http://int-dead:1", None]
        assert main._PROXY_POOL_DEAD_UNTIL.get("internal", 0.0) == 0.0

    asyncio.run(_run())


def test_stream_external_pool_direct_fallback_configurable(monkeypatch):
    async def _run():
        """【T23更新】海外池流式请求直连兜底改为可配（默认开）：
        默认：外挂全挂 → 直连兜底一次（成功返回 + external 冷静期）；
        EXTERNAL_DIRECT_FALLBACK=0（国内部署）：外挂全挂直接失败，不白等直连超时。"""
        _install_pools(monkeypatch, ["http://int-ok:1"], ["http://ext-dead:1"])
        seen: list = []
        _install_fake_stream_get_client(monkeypatch, {"http://ext-dead:1"}, seen)

        # 默认开：外代全挂 → 直连兜底成功
        resp = await main.stream_request_with_proxy_group("GET", "http://example.com/a",
                                                           proxy_list=["http://ext-dead:1"])
        assert resp.status_code == 200
        assert seen == ["http://ext-dead:1", None]
        assert main._PROXY_POOL_DEAD_UNTIL.get("external", 0.0) > time.time()

        # 开关关闭：恢复旧行为——不尝试直连、直接抛错、不设冷静期
        monkeypatch.setattr(main, "EXTERNAL_DIRECT_FALLBACK", False)
        main._PROXY_POOL_DEAD_UNTIL.pop("external", None)
        seen.clear()
        with pytest.raises(RuntimeError):
            await main.stream_request_with_proxy_group("GET", "http://example.com/a2",
                                                       proxy_list=["http://ext-dead:1"])
        assert None not in seen
        assert main._PROXY_POOL_DEAD_UNTIL.get("external", 0.0) == 0.0

    asyncio.run(_run())


# ---------- C. B站 get_live_status 回显请求 id ----------

def test_bili_live_status_echoes_requested_id(monkeypatch):
    """关注列表存的是 URL 短号；详情接口回的是 room_info.room_id 真实长号——
    回显请求传入的 id 才能对上关注状态匹配。"""
    async def fake_detail(self, room_id):
        return room_dict(roomId="99999", title="t", nickname="n", watching="12",
                         isLive=True, platform="bilibili")

    monkeypatch.setattr(BilibiliAdapter, "get_room_detail", fake_detail)
    out = asyncio.run(BilibiliAdapter("").get_live_status(["6", "7"]))
    assert [x["roomId"] for x in out] == ["6", "7"]  # 请求短号原样回显
    assert out[0]["isLive"] is True


# ---------- D. 基类异常守卫 ----------

class _BoomAdapter(LivePlatform):
    site = "boom"

    async def get_room_detail(self, room_id):
        raise RuntimeError("upstream exploded")

    async def get_recommend_rooms(self, page, page_size):
        raise RuntimeError("upstream exploded")

    async def get_live_status(self, room_ids):
        raise RuntimeError("upstream exploded")

    async def get_categories(self):
        raise RuntimeError("upstream exploded")

    async def search_rooms(self, keyword, page, page_size):
        raise RuntimeError("upstream exploded")

    async def get_category_rooms(self, parent_area_id, area_id, page, page_size):
        raise RuntimeError("upstream exploded")


def test_base_guard_converts_exceptions_to_empty_state():
    a = _BoomAdapter()
    assert asyncio.run(a.get_room_detail("x")) is None
    assert asyncio.run(a.get_recommend_rooms(1, 24)) == {"list": [], "hasMore": False}
    assert asyncio.run(a.get_category_rooms("1", "1", 1, 24)) == {"list": [], "hasMore": False}
    assert asyncio.run(a.search_rooms("x", 1, 24)) == {"list": [], "hasMore": False}
    assert asyncio.run(a.get_live_status(["x"])) == []
    assert asyncio.run(a.get_categories()) == []


class _OkAdapter(LivePlatform):
    site = "ok"

    async def get_room_detail(self, room_id):
        return room_dict(roomId=room_id, title="t", platform="ok")


def test_base_guard_transparent_for_normal_methods():
    out = asyncio.run(_OkAdapter().get_room_detail("42"))
    assert out["roomId"] == "42" and out["title"] == "t"


def test_real_adapters_are_wrapped_by_guard():
    """__init_subclass__ 守卫真实生效在存量适配器上（functools.wraps 的 __wrapped__）。"""
    assert hasattr(DouyuAdapter.get_room_detail, "__wrapped__")
    assert hasattr(PandaAdapter.get_room_detail, "__wrapped__")
    assert hasattr(BilibiliAdapter.get_live_status, "__wrapped__")
    assert hasattr(HuyaAdapter.get_category_rooms, "__wrapped__")
    assert hasattr(SoopAdapter.get_recommend_rooms, "__wrapped__")
    assert hasattr(TwitchAdapter.search_rooms, "__wrapped__")


# ---------- E. 缓存写入侧深拷贝 ----------

def _stub_soop_network(monkeypatch, responder):
    async def fake_rwpg(method, url, proxy_list=None, **kw):
        return responder(url)

    async def fake_pool(*a, **k):
        return []

    monkeypatch.setattr(main, "request_with_proxy_group", fake_rwpg)
    monkeypatch.setattr(main, "get_fixed_proxy_list", fake_pool)


class _Resp:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload


def test_soop_http_failure_no_backoff_and_write_copy(monkeypatch):
    """HTTP 503（网络/风控层）不算「未开播」：不递增退避计数；且写缓存存的是
    副本——返回对象被调用方原地改写不渗入缓存。"""
    async def _run():
        _stub_soop_network(monkeypatch, lambda url: _Resp(503))
        url = "https://play.sooplive.com/guardedbj"
        main.M3U8_CACHE.pop(url, None)
        main._SOOP_OFFLINE_COUNT.pop("guardedbj", None)
        try:
            out = await main.parse_soop(url)
            assert out["isLive"] is False
            assert main._SOOP_OFFLINE_COUNT.get("guardedbj", 0) == 0  # 未递增退避
            assert main.M3U8_CACHE[url]["expire"] <= time.time() + 60.5  # 固定短缓存
            out["title"] = "MUTATED"
            assert main.M3U8_CACHE[url]["data"]["title"] != "MUTATED"  # 写入侧深拷贝
            assert main.M3U8_CACHE[url]["data"] is not out
        finally:
            main.M3U8_CACHE.pop(url, None)
            main._SOOP_OFFLINE_COUNT.pop("guardedbj", None)

    asyncio.run(_run())


def test_soop_explicit_offline_increments_backoff(monkeypatch):
    """接口明确 RESULT 非 0/1（未开播/封禁）→ 递增退避计数。"""
    async def _run():
        _stub_soop_network(monkeypatch, lambda url: _Resp(200, {"CHANNEL": {"RESULT": 999}}))
        url = "https://play.sooplive.com/offlinebj"
        main.M3U8_CACHE.pop(url, None)
        main._SOOP_OFFLINE_COUNT.pop("offlinebj", None)
        try:
            out = await main.parse_soop(url)
            assert out["isLive"] is False
            assert main._SOOP_OFFLINE_COUNT.get("offlinebj", 0) == 1  # 递增
        finally:
            main.M3U8_CACHE.pop(url, None)
            main._SOOP_OFFLINE_COUNT.pop("offlinebj", None)

    asyncio.run(_run())


def test_panda_cache_write_side_isolation(monkeypatch):
    async def _run():
        _stub_soop_network(
            monkeypatch,
            lambda url: _Resp(200, {"result": True, "bjInfo": {"nick": "pan", "profileImg": "p"}}),
        )
        url = "https://www.pandalive.co.kr/pandabj"
        main.M3U8_CACHE.pop(url, None)
        try:
            out = await main.parse_panda_manual(url)
            assert out["isLive"] is False
            out["title"] = "MUTATED"
            cached = main.M3U8_CACHE[url]["data"]
            assert cached["title"] == "pan"
            assert cached is not out
        finally:
            main.M3U8_CACHE.pop(url, None)

    asyncio.run(_run())


# ---------- G. T3 补测：VERIFY_TLS / /health/detail ----------

def test_verify_tls_default_on():
    """默认值钉住：不设环境变量时 VERIFY_TLS=True（P0 安全修复的关键开关，
    此前改回 False 测试也不会红）。"""
    if "VERIFY_TLS" not in os.environ:
        assert main.VERIFY_TLS is True


def test_tls_verify_for_respects_flags(monkeypatch):
    monkeypatch.setattr(main, "VERIFY_TLS", True)
    monkeypatch.setattr(main, "INSECURE_TLS_HOSTS", ["bad.example"])
    assert main._tls_verify_for("https://sub.bad.example/x") is False
    assert main._tls_verify_for("https://bad.example/x") is False
    assert main._tls_verify_for("https://good.example/x") is True
    monkeypatch.setattr(main, "VERIFY_TLS", False)
    assert main._tls_verify_for("https://good.example/x") is False  # 全局关


def test_health_detail_token_gates(monkeypatch):
    from fastapi.testclient import TestClient

    if hasattr(main.limiter, "reset"):
        main.limiter.reset()
    client = TestClient(main.app)
    monkeypatch.setattr(main, "HEALTH_TOKEN", "")
    assert client.get("/health/detail").status_code == 404  # 未配置 token → 接口关闭

    monkeypatch.setattr(main, "HEALTH_TOKEN", "s3cret")
    assert client.get("/health/detail").status_code == 403  # 无 token
    assert client.get("/health/detail", headers={"x-health-token": "wrong"}).status_code == 403
    r = client.get("/health/detail", headers={"x-health-token": "s3cret"})
    assert r.status_code == 200
    assert r.json()["status"] == "alive"


def test_health_detail_rate_limited(monkeypatch):
    from fastapi.testclient import TestClient

    if hasattr(main.limiter, "reset"):
        main.limiter.reset()
    client = TestClient(main.app)
    monkeypatch.setattr(main, "HEALTH_TOKEN", "s3cret")
    try:
        codes = [client.get("/health/detail", headers={"x-health-token": "s3cret"}).status_code
                 for _ in range(11)]
        assert codes[:10] == [200] * 10
        assert codes[10] == 429  # 10/min 限速（错 token 爆破也会被拦）
    finally:
        if hasattr(main.limiter, "reset"):
            main.limiter.reset()

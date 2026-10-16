"""P1 数据正确性单测：冷静期分池 / B站 or 1 / strip_tags /
hasMore / 缓存深拷贝 / Twitch GraphQL 变量化。

背景（backend/main.py + browse_platforms/*）：
  - 代理池冷静期此前是单一全局量：外代池全挂（部署环境无外网代理是常态）会误让
    国内请求也直连优先，反之亦然 → _proxy_pool_key 按池分桶；
  - bilibili _parse_list 的 int(get("live_status", 1) or 1)：0 是 falsy，`0 or 1` → 1
    → 未开播房间被标成开播；search 的 == 1 又与 main.py A6（1/2 同为开播）不一致
    → _live_status 统一语义；
  - strip_tags 名为全标签剥离、实际只删 <em…> → 泛化为真标签剥离；
  - getMoreRecList 服务端固定 12 条/页（无分页元数据、不接受 pageSize），旧判定
    len(rooms) >= page_size（前端恒 24）→ 12 >= 24 恒 False → 首页推荐翻页第 1 页就死
    → 改 fetch-until-empty；
  - TTLCache / M3U8_CACHE 返回共享可变对象：browse 的 normalize_parse 原地改
    streams 的 url → 缓存被单路径污染 + 并发撕裂读 → 读出即深拷贝；
  - Twitch get_room_detail 的 user(login) GQL 查询字符串 % 插值 → $login 变量化。

运行：python -m pytest backend/tests/test_p1_data_correctness.py -q（见 README「本地测试」）。
"""
import asyncio
import time

import httpx
import pytest
import uvicorn

import main
from browse_platforms.bilibili import BilibiliAdapter, _live_status
from browse_platforms.common import strip_tags
from browse_platforms.twitch import TwitchAdapter
from browse_cache import TTLCache


@pytest.fixture(autouse=True)
def _clean_cooldown():
    """每个用例前后清空冷静期，避免跨用例串扰。"""
    main._PROXY_POOL_DEAD_UNTIL.clear()
    yield
    main._PROXY_POOL_DEAD_UNTIL.clear()


# ---------- A. 冷静期分池 ----------

def test_pool_key_internal():
    saved_i, saved_e = main.PROXY_URLS, main.EXTERNAL_PROXY_URLS
    try:
        main.PROXY_URLS = ["http://int1:1", "http://int2:2"]
        main.EXTERNAL_PROXY_URLS = ["http://ext1:1", "http://ext2:2"]
        assert main._proxy_pool_key(["http://int1:1"]) == "internal"
        assert main._proxy_pool_key(["http://int1:1", "http://int2:2"]) == "internal"
        # 轮转子集/乱序（get_fixed_proxy_list 的产物形态）
        assert main._proxy_pool_key(["http://int2:2", "http://int1:1"]) == "internal"
        assert main._proxy_pool_key(["http://ext1:1"]) == "external"
        assert main._proxy_pool_key(["http://ext2:2", "http://ext1:1"]) == "external"
        # TS 粘性代理：单代理从哪个池来就归哪个桶
        assert main._proxy_pool_key(["http://ext1:1"]) == "external"
        # 混合/未知代理 → misc；纯直连/空 → direct
        assert main._proxy_pool_key(["http://int1:1", "http://ext1:1"]) == "misc"
        assert main._proxy_pool_key(["http://nowhere:9"]) == "misc"
        assert main._proxy_pool_key([None]) == "direct"
        assert main._proxy_pool_key([]) == "direct"
        # 两池配了同一个代理 → 归内桶（保守）
        main.PROXY_URLS = ["http://both:1"]
        main.EXTERNAL_PROXY_URLS = ["http://both:1"]
        assert main._proxy_pool_key(["http://both:1"]) == "internal"
    finally:
        main.PROXY_URLS, main.EXTERNAL_PROXY_URLS = saved_i, saved_e


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


def test_internal_failure_sets_only_internal_cooldown(monkeypatch):
    async def _run():
        """内池全挂 + 直连兜底成功 → 只有 internal 桶进入冷静期，external 桶不受影响。"""
        _install_pools(monkeypatch, ["http://int-dead:1"], ["http://ext-ok:1"])
        seen: list = []
        _install_fake_get_client(monkeypatch, {"http://int-dead:1"}, seen)

        resp = await main.request_with_retry("GET", "http://example.com/a")
        assert resp.status_code == 200
        assert main._PROXY_POOL_DEAD_UNTIL.get("internal", 0.0) > time.time()
        assert main._PROXY_POOL_DEAD_UNTIL.get("external", 0.0) == 0.0

        # 关键回归：内池冷静期内，外池请求不受带偏——仍先走外池代理（不是直连优先）
        seen.clear()
        resp = await main.request_with_proxy_group("GET", "http://example.com/b",
                                                   proxy_list=["http://ext-ok:1"])
        assert resp.status_code == 200
        assert seen == ["http://ext-ok:1"]

    asyncio.run(_run())


def test_external_failure_sets_only_external_cooldown(monkeypatch):
    async def _run():
        """【T23更新】海外池全挂 → 默认（EXTERNAL_DIRECT_FALLBACK=1）直连兜底：
        直连成功 → 请求成功且 external 桶进入冷静期（后续直连优先，不再白等全池）；
        开关关闭（模拟纯国内部署）则恢复旧「不直连」行为（抛错、不尝试直连、
        不设冷静期）。内池两形态下都不受影响。"""
        _install_pools(monkeypatch, ["http://int-ok:1"], ["http://ext-dead:1"])
        seen: list = []
        _install_fake_get_client(monkeypatch, {"http://ext-dead:1"}, seen)

        # 默认开：外代全挂 → 直连兜底成功 → 返回响应 + external 冷静期置位
        resp = await main.request_with_proxy_group("GET", "http://example.com/a",
                                                    proxy_list=["http://ext-dead:1"])
        assert resp.status_code == 200
        assert seen == ["http://ext-dead:1", None]  # 先外代、后直连兜底
        assert main._PROXY_POOL_DEAD_UNTIL.get("external", 0.0) > time.time()
        assert main._PROXY_POOL_DEAD_UNTIL.get("internal", 0.0) == 0.0

        # 开关关闭（模拟纯国内部署）：外代全挂 → 不直连、直接抛错、不设冷静期
        monkeypatch.setattr(main, "EXTERNAL_DIRECT_FALLBACK", False)
        main._PROXY_POOL_DEAD_UNTIL.pop("external", None)
        seen.clear()
        with pytest.raises(RuntimeError):
            await main.request_with_proxy_group("GET", "http://example.com/a2",
                                                proxy_list=["http://ext-dead:1"])
        assert None not in seen  # 没有尝试直连
        assert main._PROXY_POOL_DEAD_UNTIL.get("external", 0.0) == 0.0

        seen.clear()
        resp = await main.request_with_retry("GET", "http://example.com/b")
        assert resp.status_code == 200
        # request_with_retry 读模块级 PROXY_URLS（已被 monkeypatch 成 int-ok）
        assert seen == ["http://int-ok:1"]

    asyncio.run(_run())


def test_cooldown_same_pool_skips_to_direct(monkeypatch):
    async def _run():
        """冷静期对同池生效：内池冷静期内，内池请求直连优先（旧行为保留）。"""
        _install_pools(monkeypatch, ["http://int-dead:1"], ["http://ext-ok:1"])
        seen: list = []
        _install_fake_get_client(monkeypatch, {"http://int-dead:1"}, seen)

        await main.request_with_retry("GET", "http://example.com/a")  # 触发内池冷静期
        seen.clear()
        resp = await main.request_with_retry("GET", "http://example.com/b")
        assert resp.status_code == 200
        assert seen == [None]  # 直连优先，没再撞死代理

    asyncio.run(_run())


def test_expired_cooldown_not_applied(monkeypatch):
    async def _run():
        """过期的冷静期不再生效（代理重新先试）。"""
        _install_pools(monkeypatch, ["http://int-ok:1"], ["http://ext-ok:1"])
        seen: list = []
        _install_fake_get_client(monkeypatch, set(), seen)
        main._PROXY_POOL_DEAD_UNTIL["internal"] = time.time() - 1  # 已过期

        resp = await main.request_with_retry("GET", "http://example.com/a")
        assert resp.status_code == 200
        assert seen == ["http://int-ok:1"]

    asyncio.run(_run())


# ---------- B. 活体 e2e 基建（真实 in-process uvicorn） ----------

_E2E_PORT = 39904


@pytest.fixture(scope="module")
def live_server():
    config = uvicorn.Config(main.app, host="127.0.0.1", port=_E2E_PORT, log_level="warning")
    server = uvicorn.Server(config)
    import threading
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(100):
        if server.started:
            break
        time.sleep(0.1)
    assert server.started, "uvicorn 未启动"
    yield f"127.0.0.1:{_E2E_PORT}"
    server.should_exit = True
    thread.join(timeout=5)


# ---------- B2. Host 守卫纯 ASGI 化回归 ----------

def test_host_guard_rejects_invalid_host(live_server, monkeypatch):
    """守卫语义不变：非法 Host 头 → 400 JSON（从 BaseHTTPMiddleware 换成纯 ASGI 后钉住）。"""
    monkeypatch.setattr(main, "ALLOWED_HOSTS", ["good.example"])
    with httpx.Client(timeout=10) as c:
        r = c.get(f"http://127.0.0.1:{_E2E_PORT}/health", headers={"host": "evil.example"})
        assert r.status_code == 400
        assert r.json() == {"error": "invalid host header"}


def test_host_guard_allows_whitelisted_host(live_server, monkeypatch):
    monkeypatch.setattr(main, "ALLOWED_HOSTS", ["good.example"])
    with httpx.Client(timeout=10) as c:
        # Host 合法 → 放行（到达 /health 正常响应，而不是 400）
        r = c.get(f"http://127.0.0.1:{_E2E_PORT}/health", headers={"host": "good.example"})
        assert r.status_code != 400


# ---------- C. B站 live_status（or 1 修复 + 轮播对齐） ----------

@pytest.mark.parametrize("raw,expected", [
    (0, False), (1, True), (2, True),            # 0=未开播 1=直播中 2=轮播（对齐 A6）
    ("0", False), ("1", True), ("2", True),      # 字符串形态（搜索接口实测形态）
    (True, True), (False, False),
])
def test_live_status_values(raw, expected):
    assert _live_status(raw, default=True) is expected


@pytest.mark.parametrize("raw,default,expected", [
    (None, True, True), (None, False, False),    # 字段缺失按各自旧默认
    ("", True, True), ("", False, False),        # 空串（旧实现 int("") 会抛）
    ("abc", True, True), ("abc", False, False),  # 非法值安全回落
])
def test_live_status_missing_or_invalid(raw, default, expected):
    assert _live_status(raw, default=default) is expected


def test_parse_list_or1_bug_fixed():
    """旧实现 `int(get("live_status", 1) or 1) != 0`：0 or 1 → 1 → 未开播被标开播。"""
    a = BilibiliAdapter()
    rooms = a._parse_list([
        {"roomid": "r1", "title": "t1", "live_status": 0},      # 未开播 → False（旧版 True）
        {"roomid": "r2", "title": "t2", "live_status": 2},      # 轮播 → True（有流可播）
        {"roomid": "r3", "title": "t3"},                        # 缺失 → True（列表默认在播）
        {"roomid": "r4", "title": "t4", "live_status": "0"},    # 字符串 0 → False
    ])
    assert [r["isLive"] for r in rooms] == [False, True, True, False]


def test_parse_list_strips_tags_in_title():
    a = BilibiliAdapter()
    rooms = a._parse_list([
        {"roomid": "r1", "title": '<em class="k">高亮</em>正常', "live_status": 1},
    ])
    assert rooms[0]["title"] == "高亮正常"


# ---------- D. strip_tags 泛化 ----------

@pytest.mark.parametrize("raw,expected", [
    ('<em class="keyword">王者</em>荣耀', "王者荣耀"),      # 高亮 em（旧实现唯一覆盖形态）
    ("<b>加粗</b>普通", "加粗普通"),                         # 真标签（旧实现删不掉）
    ("第一行<br/>第二行", "第一行第二行"),
    ("<a href='x'>链接</a>", "链接"),
    ("</closed>", ""),
    ("分数<100分>更多", "分数<100分>更多"),                  # 字面角括号：不是标签，不误吞
    ("a < b 且 c > d", "a < b 且 c > d"),
    ("", ""),
    (None, ""),
    (0, ""),
])
def test_strip_tags_matrix(raw, expected):
    assert strip_tags(raw) == expected


def test_strip_tags_regression_for_search_highlight():
    """B站搜索 title 的关键词高亮形态（原 search_rooms 内联正则的回归）。"""
    assert strip_tags('这局<em class="keyword">王者</em>教学') == "这局王者教学"


# ---------- E. hasMore：B站推荐固定 12 条/页 ----------

class _FakeHttpClient:
    def __init__(self, payload):
        self._payload = payload

    async def get(self, url, params=None, headers=None):
        class _R:
            def __init__(self, payload):
                self._payload = payload

            def json(self):
                return self._payload

        return _R(self._payload)


def _install_bili_client(monkeypatch, payload):
    async def fake_get_client(self):
        return _FakeHttpClient(payload)

    monkeypatch.setattr(BilibiliAdapter, "_get_client", fake_get_client)


def test_recommend_has_more_true_when_partial_page(monkeypatch):
    """getMoreRecList 实测每页固定 12 条且无尽（page=999 仍 12 条）；前端恒请求 24。
    旧判定 12 >= 24 → False → 推荐翻页第 1 页就死；新判定 12 > 0 → True。"""
    payload = {"data": {"recommend_room_list": [
        {"roomid": f"r{i}", "title": f"t{i}", "uname": f"u{i}", "live_status": 1}
        for i in range(12)
    ]}}
    _install_bili_client(monkeypatch, payload)
    result = asyncio.run(BilibiliAdapter().get_recommend_rooms(1, 24))
    assert len(result["list"]) == 12
    assert 12 < 24  # 钉住「旧公式在此页必为 False」的事实前提
    assert result["hasMore"] is True


def test_recommend_has_more_false_when_empty(monkeypatch):
    _install_bili_client(monkeypatch, {"data": {"recommend_room_list": []}})
    result = asyncio.run(BilibiliAdapter().get_recommend_rooms(999, 24))
    assert result["hasMore"] is False


def test_category_rooms_list_shape_and_fetch_until_empty(monkeypatch):
    """【P1二轮更新】实测 getListByAreaID 的 data 直接是房间列表（非旧代码假设的
    dict.recommend_room_list——旧代码对真实形状必抛 'list' object has no attribute
    'get' 被 except 吞掉，分区房间一直返回空）；响应无分页元数据、page=999 返回
    空列表 → hasMore 用 fetch-until-empty：本页非空 True（哪怕只有 10 条收尾页），
    空页 False；旧 dict 形状保持兼容；areaName 字段映射。"""
    # 真实形状：data 为裸列表（含收尾短页 10 条）
    as_list = {"data": [
        {"roomid": f"r{i}", "title": f"t{i}", "live_status": 1,
         "uname": f"u{i}", "face": "f", "areaName": "网游"} for i in range(10)
    ]}
    _install_bili_client(monkeypatch, as_list)
    result = asyncio.run(BilibiliAdapter().get_category_rooms("2", "36", 5, 24))
    assert len(result["list"]) == 10
    assert result["list"][0]["area"] == "网游"
    assert result["hasMore"] is True  # 10 > 0（旧 10>=24 恒 False，收尾页丢数据）

    # 旧形状（dict.recommend_room_list）保持兼容
    as_dict = {"data": {"recommend_room_list": [
        {"roomid": f"r{i}", "title": f"t{i}", "live_status": 1} for i in range(24)
    ]}}
    _install_bili_client(monkeypatch, as_dict)
    result = asyncio.run(BilibiliAdapter().get_category_rooms("2", "36", 1, 24))
    assert len(result["list"]) == 24
    assert result["hasMore"] is True

    # 末页之后：空列表自然停
    _install_bili_client(monkeypatch, {"data": []})
    result = asyncio.run(BilibiliAdapter().get_category_rooms("2", "36", 999, 24))
    assert result["list"] == []
    assert result["hasMore"] is False


# ---------- F. 缓存深拷贝 ----------

def test_ttl_cache_returns_deep_copy():
    c = TTLCache(default_ttl=60)
    value = {"list": [{"id": 1, "url": "http://a/1"}]}
    c.set("k", value)
    got = c.get("k")
    assert got == value
    assert got is not value
    got["list"][0]["url"] = "MUTATED"
    got["list"].append({"id": 2})
    # 再读：缓存原值未被污染
    again = c.get("k")
    assert again["list"] == [{"id": 1, "url": "http://a/1"}]


def test_ttl_cache_string_roundtrip_zero_cost():
    c = TTLCache(default_ttl=60)
    c.set("ck", "cookie-string")
    assert c.get("ck") == "cookie-string"


def test_soop_cache_hit_returns_copy_not_shared_object(monkeypatch):
    async def _run():
        """parse_soop 缓存命中返回深拷贝：下游 normalize_parse 原地改 url 不会再污染缓存。"""
        url = "https://play.sooplive.com/leesh2148"
        cached_data = {
            "streams": [{"url": "http://live-s.example/a.flv", "quality": "原画", "cdn": "line1"}],
            "isLive": True, "title": "t", "avatar": "https://a/1.jpg",
        }
        main.M3U8_CACHE[url] = {"data": cached_data, "expire": time.time() + 60}

        async def no_network(*a, **k):
            raise AssertionError("缓存命中路径不应触网")

        monkeypatch.setattr(main, "request_with_proxy_group", no_network)
        monkeypatch.setattr(main, "get_fixed_proxy_list", no_network)

        first = await main.parse_soop(url)
        assert first is not cached_data
        assert first["streams"][0]["url"] == "http://live-s.example/a.flv"

        # 模拟 browse 路由的下游变异（normalize_parse 原地改 source["url"]：http→https）
        first["streams"][0]["url"] = "https://live-s.example/a.flv"
        second = await main.parse_soop(url)
        assert second["streams"][0]["url"] == "http://live-s.example/a.flv"  # 缓存未被污染
        assert second is not first

        main.M3U8_CACHE.pop(url, None)

    asyncio.run(_run())


def test_normalize_parse_mutation_does_not_leak_into_cache(monkeypatch):
    async def _run():
        """端到端场景：browse 的 normalize_parse 会原地改 streams——经缓存的深拷贝隔离。"""
        from browse_api import normalize_parse

        url = "https://play.sooplive.com/khm11903"
        cached_data = {
            "streams": [{"url": "http://cdn.example/s.flv", "quality": "蓝光", "cdn": "k"}],
            "isLive": True, "title": "t", "avatar": "https://a/2.jpg",
        }
        main.M3U8_CACHE[url] = {"data": cached_data, "expire": time.time() + 60}

        async def no_network(*a, **k):
            raise AssertionError("缓存命中路径不应触网")

        monkeypatch.setattr(main, "request_with_proxy_group", no_network)
        monkeypatch.setattr(main, "get_fixed_proxy_list", no_network)

        raw = await main.parse_soop(url)
        normalized = normalize_parse(raw, "soop", "khm11903")
        # normalize_parse 把 http 流升级 https（原地改 source["url"]）
        assert normalized["streams"][0]["url"].startswith("https://")

        again = await main.parse_soop(url)
        assert again["streams"][0]["url"].startswith("http://")  # 缓存里还是原始形态

        main.M3U8_CACHE.pop(url, None)

    asyncio.run(_run())


# ---------- G. Twitch GraphQL 变量化 ----------

_TWITCH_QUERY_CONST = (
    "query User($login: String!) { user(login: $login) "
    "{ displayName profileImageURL(width: 150) stream { title viewersCount previewImageURL } } }"
)


def test_twitch_room_detail_uses_graphql_variables(monkeypatch):
    async def _run():
        captured = {}

        async def fake_gql(self, payload):
            captured["payload"] = payload
            return {"data": {"user": {
                "displayName": "测试频道", "profileImageURL": "https://a/1.jpg",
                "stream": {"title": "直播标题", "viewersCount": 123,
                           "previewImageURL": "https://p/{width}x{height}.jpg"},
            }}}

        monkeypatch.setattr(TwitchAdapter, "_gql", fake_gql)
        d = await TwitchAdapter().get_room_detail("kato_junichi0817")
        p = captured["payload"][0]
        # 查询串是常量（$login 变量位），不含任何用户输入
        assert p["query"] == _TWITCH_QUERY_CONST
        assert p["variables"] == {"login": "kato_junichi0817"}
        assert d["title"] == "直播标题"
        assert d["watching"] == "123"

    asyncio.run(_run())


def test_twitch_room_detail_injection_chars_are_pure_data(monkeypatch):
    async def _run():
        """含引号/反斜杠/花括号的 login 仍是纯数据：旧 % 插值会被拼坏查询（400 → 详情恒空）。"""
        captured = {}

        async def fake_gql(self, payload):
            captured["payload"] = payload
            return {"data": {"user": {"displayName": "x", "profileImageURL": "",
                                      "stream": None}}}

        monkeypatch.setattr(TwitchAdapter, "_gql", fake_gql)
        weird = 'we"i\\rd { (login: "other") }'
        d = await TwitchAdapter().get_room_detail(weird)
        p = captured["payload"][0]
        assert p["query"] == _TWITCH_QUERY_CONST          # 查询结构恒定，未被注入改写
        assert p["variables"]["login"] == weird           # 原样作为数据传递
        # user 存在但 stream 为 None → 返回离线详情（isLive=False），browse 上游回退 parse
        assert d is not None and d["isLive"] is False

    asyncio.run(_run())


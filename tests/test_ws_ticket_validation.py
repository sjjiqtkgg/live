"""P0-7/8 单测：Cookie 头化 + WS 一次性 ticket + 弹幕 WS 路径参数校验。

背景（backend/main.py）：
  - /api/parse、/api/proxy 此前 Cookie 走 ?cookie= 查询串（落反代/服务日志与浏览器历史）
    → 改 x-platform-cookie 头（browse 路由 _cookie_for 同款头）；
  - /ws/soop 同样 ?cookie=（浏览器 WebSocket API 不支持自定义头）→ 新增 /api/ws-ticket
    两段式：头换 60s 单次有效 ticket（绑定 site+room_id），WS 只带 ?ticket=；
  - 三个弹幕 WS 路径参数零校验直接进下游（douyin 拼进 Go 服务 URL、twitch 拼进 IRC JOIN
    可注入 \\r\\n、soop 拼进 bjid 查询参数与 /Websocket/ 路径）→ 平台字符集白名单。

分两部分：A/B 纯函数（校验矩阵 + ticket 生命周期）；C 活体 e2e——真实 in-process
uvicorn + httpx/websockets 客户端，monkeypatch request_with_proxy_group 与
websockets.connect（非本地目标替换为假上游），钉住：头换票全链路、ticket 单次消费、
?cookie= 已死、注入全拒、访问日志零凭据。

运行：python -m pytest backend/tests/test_ws_ticket_validation.py -q（见 README「本地测试」）。
"""
import asyncio
import logging
import re
import threading
import time

import httpx
import pytest
import uvicorn
import websockets

import main


# ---------- A. 弹幕 WS 路径参数校验矩阵 ----------

@pytest.mark.parametrize("value,ok", [
    ("383942708879", True),          # 规范房间号
    ("1", True),
    ("12345678901234567890", True),  # 20 位上限
    ("", False),
    ("abc", False),                  # 非数字
    ("123456789012345678901", False),# 超长
    ("../admin", False),             # 路径注入（..%2F 解码产物）
    ("..\\x", False),
    ("123;rm -rf", False),           # 命令元字符
    ("12 34", False),                # 空白
])
def test_valid_ws_room_douyin(value, ok):
    assert main._valid_ws_room("douyin", value) is ok


@pytest.mark.parametrize("value,ok", [
    ("kato_junichi0817", True),      # 规范频道登录名
    ("nix", True),                   # 3 位下限
    ("LIRIK", True),                 # 大写
    ("ab", False),                   # 太短
    ("x" * 26, False),               # 超长
    ("evil\r\nJOIN #x", False),      # IRC 命令注入（\r\n 拆命令）
    ("evil\nPRIVMSG #a :hi", False),
    ("name with space", False),
    ("name-dash", False),            # 连字符不在字符集
    ("", False),
])
def test_valid_ws_room_twitch(value, ok):
    assert main._valid_ws_room("twitch", value) is ok


@pytest.mark.parametrize("value,ok", [
    ("leesh2148", True),             # 规范 bj_id
    ("khm11903", True),
    ("a" * 30, True),                # 30 位上限
    ("a", False),                    # 太短
    ("a" * 31, False),               # 超长
    ("leesh2148&pwd=1", False),      # bjid 查询参数注入
    ("leesh2148#frag", False),
    ("../chat", False),              # /Websocket/ 路径注入
    ("bj 123", False),
    ("", False),
])
def test_valid_ws_room_soop(value, ok):
    assert main._valid_ws_room("soop", value) is ok


@pytest.mark.parametrize("site", ["huya", "douyu", "bilibili", "pandatv", "", "unknown"])
def test_valid_ws_room_unknown_site_rejected(site):
    """未登记站点一律拒绝（新增弹幕站点必须显式登记字符集）。"""
    assert main._valid_ws_room(site, "anything") is False


def test_ws_room_patterns_cover_all_three_ws_sites():
    assert set(main._WS_ROOM_PATTERNS) == {"douyin", "twitch", "soop"}


# ---------- B. ticket 生命周期 ----------

def test_ticket_roundtrip_returns_cookie():
    t = main._issue_ws_ticket("soop", "leesh2148", "AuthTicket=abc; _au=xyz")
    assert main._consume_ws_ticket(t, "soop", "leesh2148") == "AuthTicket=abc; _au=xyz"


def test_ticket_single_use():
    t = main._issue_ws_ticket("soop", "leesh2148", "ck")
    assert main._consume_ws_ticket(t, "soop", "leesh2148") == "ck"
    assert main._consume_ws_ticket(t, "soop", "leesh2148") is None  # 用后即焚


def test_ticket_room_mismatch_rejected():
    t = main._issue_ws_ticket("soop", "leesh2148", "ck")
    assert main._consume_ws_ticket(t, "soop", "other_bj") is None
    # 失败尝试也把票消耗掉（防重放试探）
    assert main._consume_ws_ticket(t, "soop", "leesh2148") is None


def test_ticket_site_mismatch_rejected():
    t = main._issue_ws_ticket("soop", "leesh2148", "ck")
    assert main._consume_ws_ticket(t, "twitch", "leesh2148") is None


def test_ticket_expired_rejected():
    t = main._issue_ws_ticket("soop", "leesh2148", "ck")
    main._WS_TICKETS[t]["expire"] = time.time() - 1  # 手动过期
    assert main._consume_ws_ticket(t, "soop", "leesh2148") is None


def test_ticket_empty_is_anonymous():
    assert main._consume_ws_ticket("", "soop", "leesh2148") is None


def test_ticket_unknown_ticket_rejected():
    assert main._consume_ws_ticket("not-a-real-ticket", "soop", "leesh2148") is None


def test_ticket_empty_cookie_roundtrip():
    """ticket 对应空 Cookie（前端没配 Cookie 也换票）→ 消费得到空串（匿名语义）。"""
    t = main._issue_ws_ticket("soop", "leesh2148", "")
    assert main._consume_ws_ticket(t, "soop", "leesh2148") == ""


def test_ticket_format_and_uniqueness():
    """token_urlsafe(32)：URL 安全字符集且足够长；两次签发不重复。"""
    t1 = main._issue_ws_ticket("soop", "a", "ck")
    t2 = main._issue_ws_ticket("soop", "a", "ck")
    assert re.fullmatch(r"[A-Za-z0-9_-]{43}", t1), t1
    assert t1 != t2
    for t in (t1, t2):
        main._WS_TICKETS.pop(t, None)


def test_ticket_sweep_removes_expired():
    main._WS_TICKETS.clear()
    for i in range(3):
        t = main._issue_ws_ticket("soop", f"r{i}", "ck")
        main._WS_TICKETS[t]["expire"] = time.time() - 1
    main._issue_ws_ticket("soop", "fresh", "ck")  # 签发时顺带清扫
    live = [v for v in main._WS_TICKETS.values() if v["room"] == "fresh"]
    assert len(live) == 1
    assert len(main._WS_TICKETS) == 1


def test_ticket_ttl_constant():
    assert main._WS_TICKET_TTL_SECONDS == 60


# ---------- C. 活体 e2e（真实 in-process uvicorn） ----------

_E2E_PORT = 39903


@pytest.fixture(scope="module")
def live_server():
    config = uvicorn.Config(main.app, host="127.0.0.1", port=_E2E_PORT, log_level="info")
    server = uvicorn.Server(config)
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


class _FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class _FakeSoopUpstream:
    """假 SOOP chat 上游：记录 URL/头/收到的包；迭代挂起（等任务被取消）。"""

    def __init__(self, url, kwargs, record):
        record["url"] = url
        record["headers"] = kwargs.get("additional_headers") or kwargs.get("extra_headers") or {}
        record["sent"] = []
        self.record = record

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def send(self, data):
        self.record["sent"].append(data)

    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.sleep(3600)  # 永不产出：转发循环挂起，由任务取消收尾
        raise StopAsyncIteration


@pytest.fixture()
def soop_stubs(monkeypatch):
    """拦住 SOOP 链路的两个真实上游：live_api（request_with_proxy_group）与 chat WS。"""
    calls = {"live_api": [], "chat_ws": []}
    channel_payload = {"CHANNEL": {"CHATNO": "9876", "FTK": "ftk123", "CHDOMAIN": "chat-fake.sooplive.example", "CHPT": 1}}

    async def fake_proxy_request(method, url, **kwargs):
        calls["live_api"].append({
            "method": method, "url": url,
            "headers": dict(kwargs.get("headers") or {}),
            "data": kwargs.get("data"),
        })
        return _FakeResp(channel_payload)

    real_connect = websockets.connect

    def fake_connect(url, **kwargs):
        if "127.0.0.1" in url or "localhost" in url:
            return real_connect(url, **kwargs)  # 测试客户端 → 被测服务器，直通
        record = {}
        calls["chat_ws"].append(record)
        return _FakeSoopUpstream(url, kwargs, record)

    monkeypatch.setattr(main, "request_with_proxy_group", fake_proxy_request)
    monkeypatch.setattr(websockets, "connect", fake_connect)
    return calls


def _get_ticket(cookie="AuthTicket=topsecret; _au=au1", site="soop", room="leesh2148"):
    r = httpx.get(f"http://127.0.0.1:{_E2E_PORT}/api/ws-ticket",
                  params={"site": site, "room_id": room},
                  headers={"x-platform-cookie": cookie} if cookie else {}, timeout=5)
    return r


class _AccessLogCapture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.lines = []

    def emit(self, record):
        self.format(record)
        self.lines.append(record.getMessage())


def _attach_access_capture():
    """同时挂 uvicorn.access（HTTP 请求行）与 uvicorn.error（WS [accepted]/[closed] 连接级
    日志——uvicorn 的 websockets 实现把 WS 访问行打到 connection logger，不在 access）"""
    cap = _AccessLogCapture()
    loggers = [logging.getLogger("uvicorn.access"), logging.getLogger("uvicorn.error")]
    for lg in loggers:
        lg.addHandler(cap)
    return cap, loggers


def test_ws_ticket_endpoint_returns_ticket_not_cookie(live_server):
    r = _get_ticket()
    assert r.status_code == 200
    body = r.json()
    assert re.fullmatch(r"[A-Za-z0-9_-]{43}", body["ticket"])
    assert body["expires_in"] == 60
    # 凭据绝不出现在响应里（只回随机 ticket）
    assert "topsecret" not in r.text


@pytest.mark.parametrize("site,room", [
    ("soop", "../admin"),
    ("soop", "leesh2148&pwd=1"),
    ("twitch", "x" * 26),
    ("douyin", "abc"),
    ("huya", "123"),          # 未登记站点
    ("", "123"),
])
def test_ws_ticket_endpoint_rejects_bad_input(live_server, site, room):
    r = _get_ticket(site=site, room=room)
    assert r.status_code == 400


def test_soop_ws_full_ticket_flow(live_server, soop_stubs):
    """头换票 → WS 只带 ticket → ticket 里的 Cookie 到达两个上游触点（live_api 头 + chat WS 头）。"""
    cookie = "AuthTicket=e2esecret; _au=au2"
    r = _get_ticket(cookie=cookie)
    assert r.status_code == 200
    ticket = r.json()["ticket"]

    async def run():
        async with websockets.connect(
            f"ws://127.0.0.1:{_E2E_PORT}/ws/soop/leesh2148?ticket={ticket}", open_timeout=5
        ) as ws:
            await asyncio.sleep(0.6)  # 等握手包发往假上游
            await ws.close()

    asyncio.run(run())

    # live_api 调用带上了 ticket 里的 Cookie（匿名旧版这里没有 cookie 头）
    assert len(soop_stubs["live_api"]) == 1
    api_call = soop_stubs["live_api"][0]
    assert api_call["headers"].get("cookie") == cookie
    assert "bjid=leesh2148" in api_call["url"]
    assert api_call["data"]["bid"] == "leesh2148"
    # chat WS 连接同样带上 Cookie（19+ 房间认证的第二个触点），路径无注入
    assert len(soop_stubs["chat_ws"]) == 1
    chat = soop_stubs["chat_ws"][0]
    assert chat["url"] == "wss://chat-fake.sooplive.example:2/Websocket/leesh2148"
    assert chat["headers"].get("Cookie") == cookie
    # 握手包确实发出：connect(service=1) + join(service=2)
    services = [b[2:6] for b in chat["sent"]]
    assert b"0001" in services and b"0002" in services


def test_soop_ws_ticket_single_use_rejected(live_server, soop_stubs):
    ticket = _get_ticket().json()["ticket"]

    async def use():
        async with websockets.connect(
            f"ws://127.0.0.1:{_E2E_PORT}/ws/soop/leesh2148?ticket={ticket}", open_timeout=5
        ) as ws:
            await asyncio.sleep(0.3)
            await ws.close()

    asyncio.run(use())  # 第一次：消费成功
    assert len(soop_stubs["chat_ws"]) == 1

    async def replay():
        try:
            async with websockets.connect(
                f"ws://127.0.0.1:{_E2E_PORT}/ws/soop/leesh2148?ticket={ticket}", open_timeout=5
            ):
                return "connected"
        except websockets.exceptions.InvalidStatus as e:
            return getattr(getattr(e, "response", None), "status_code", None)

    assert asyncio.run(replay()) == 403  # 重放被拒（close 1008 → 握手 403）
    assert len(soop_stubs["chat_ws"]) == 1  # 零上游触达


def test_soop_ws_bad_ticket_rejected(live_server, soop_stubs):
    async def probe():
        try:
            async with websockets.connect(
                f"ws://127.0.0.1:{_E2E_PORT}/ws/soop/leesh2148?ticket=forged-ticket", open_timeout=5
            ):
                return "connected"
        except websockets.exceptions.InvalidStatus as e:
            return getattr(getattr(e, "response", None), "status_code", None)

    assert asyncio.run(probe()) == 403
    assert soop_stubs["live_api"] == [] and soop_stubs["chat_ws"] == []


def test_soop_ws_anonymous_still_works(live_server, soop_stubs):
    """不带 ticket：照常匿名连接（兼容旧行为），live_api 无 Cookie 头。"""

    async def run():
        async with websockets.connect(
            f"ws://127.0.0.1:{_E2E_PORT}/ws/soop/leesh2148", open_timeout=5
        ) as ws:
            await asyncio.sleep(0.4)
            await ws.close()

    asyncio.run(run())
    assert len(soop_stubs["live_api"]) == 1
    assert "cookie" not in soop_stubs["live_api"][0]["headers"]


def test_soop_ws_legacy_cookie_query_is_dead(live_server, soop_stubs):
    """旧 ?cookie= 查询串不再被服务端读取（匿名语义）；同时钉住泄漏向量本身：
    uvicorn 的 WS 访问日志会【原样回显查询串】——凭据一旦进 URL 必落日志，
    这正是本批改走 ticket 的原因（新流程查询串里只有随机 ticket）。"""
    log_capture, attached = _attach_access_capture()
    try:
        async def run():
            async with websockets.connect(
                f"ws://127.0.0.1:{_E2E_PORT}/ws/soop/leesh2148?cookie=LEGACYSECRET", open_timeout=5
            ) as ws:
                await asyncio.sleep(0.3)
                await ws.close()

        asyncio.run(run())
    finally:
        for lg in attached:
            lg.removeHandler(log_capture)

    # 匿名语义：上游调用没有 Cookie 头（?cookie= 参数服务端已不读）
    assert len(soop_stubs["live_api"]) == 1
    assert "cookie" not in soop_stubs["live_api"][0]["headers"]
    # 泄漏向量钉住：访问日志原样回显客户端查询串——所以凭据绝不能进 URL；
    # 对照 test_access_log_never_contains_ticket_flow_cookie：ticket 流的日志只有随机票。
    assert any("cookie=LEGACYSECRET" in line for line in log_capture.lines)


def test_access_log_never_contains_ticket_flow_cookie(live_server, soop_stubs):
    """头换票全流程：访问日志只出现随机 ticket，凭据零落盘。"""
    log_capture, attached = _attach_access_capture()
    try:
        cookie = "AuthTicket=LOGSCAN; _au=au3"
        r = _get_ticket(cookie=cookie)
        ticket = r.json()["ticket"]

        async def run():
            async with websockets.connect(
                f"ws://127.0.0.1:{_E2E_PORT}/ws/soop/leesh2148?ticket={ticket}", open_timeout=5
            ) as ws:
                await asyncio.sleep(0.3)
                await ws.close()

        asyncio.run(run())
    finally:
        for lg in attached:
            lg.removeHandler(log_capture)

    text = "\n".join(log_capture.lines)
    assert "LOGSCAN" not in text, "访问日志出现凭据！"
    assert any("ticket=" in line for line in log_capture.lines)  # 查询串只出现随机 ticket
    assert any("/api/ws-ticket" in line for line in log_capture.lines)


@pytest.mark.parametrize("path", [
    "/ws/soop/..%2Fadmin",
    "/ws/soop/leesh2148%26pwd%3D1",
    "/ws/douyin/..%2F..%2Fetc",
    "/ws/douyin/abc",
    "/ws/twitch/evil%0d%0aJOIN%20%23x",
    "/ws/twitch/a",
])
def test_ws_path_injection_rejected_before_any_upstream(live_server, soop_stubs, path):
    """路径参数注入（..%2F / &pwd= / IRC \\r\\n）在握手层拒绝，零上游触达。"""

    async def probe():
        try:
            async with websockets.connect(f"ws://127.0.0.1:{_E2E_PORT}{path}", open_timeout=5):
                return "connected"
        except websockets.exceptions.InvalidStatus as e:
            return getattr(getattr(e, "response", None), "status_code", None)

    assert asyncio.run(probe()) == 403
    assert soop_stubs["live_api"] == []
    assert soop_stubs["chat_ws"] == []


def test_api_parse_reads_cookie_from_header(live_server, monkeypatch):
    """/api/parse：Cookie 从 x-platform-cookie 头读（旧 ?cookie= 查询参数已删）。"""
    captured = {}

    async def fake_dispatch(url, cookie=""):
        captured["url"] = url
        captured["cookie"] = cookie
        return {"streams": [], "isLive": False, "title": "", "avatar": ""}

    monkeypatch.setattr(main, "_parse_dispatch", fake_dispatch)
    r = httpx.get(f"http://127.0.0.1:{_E2E_PORT}/api/parse",
                  params={"url": "https://www.huya.com/660000"},
                  headers={"x-platform-cookie": "HEADERSECRET"}, timeout=10)
    assert r.status_code == 200
    assert captured["cookie"] == "HEADERSECRET"
    assert r.text.count("HEADERSECRET") == 0  # 响应不回显凭据

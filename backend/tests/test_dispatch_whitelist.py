"""P0-1 分派白名单统一（backend/main.py）单测。

背景：main.py 里四处域名判定此前各自为政且全是「子串 in url」：
  ① _parse_dispatch —— https://douyin.com.evil.com/ 能混进 parse_douyin，
     而它对传入 URL 有两处裸抓取（DouyinLiveStream.fetch_web_stream_data /
     request_with_retry 直拉页面）→ SSRF；
  ② api_proxy use_external —— 域名出现在路径/查询串也误切外代池；
  ③ api_follows_batch 分池 —— 同款子串；
  ④ api_proxy m3u8 分流（_is_foreign_stream / eff_referer 兜底）—— "twitch" in url。
收口后统一为 hostname 全等/点分后缀匹配（PLATFORM_SITES + EXTERNAL_STREAM_DOMAINS），
本文件钉住：平台识别矩阵（含攻击形态）、分派路由与 400 拒绝、抖音 SSRF 收口的
「零网络调用」、外代池/海外流判定、_browse_identity 回归。

运行：python -m pytest backend/tests/test_dispatch_whitelist.py -q（见 README「本地测试」）。
"""
import asyncio

import pytest
from fastapi import HTTPException

import main


# ---------- A. hostname 解析与平台识别矩阵 ----------

@pytest.mark.parametrize("url,want", [
    # 规范平台链接（前端 search.vue / browse make_room_url 的真实形态）
    ("https://www.huya.com/660000", "huya"),
    ("https://m.huya.com/660000", "huya"),
    ("https://www.douyu.com/9999", "douyu"),
    ("https://live.bilibili.com/5440", "bilibili"),
    ("https://live.douyin.com/123456", "douyin"),
    ("https://www.douyin.com/123456", "douyin"),
    ("https://www.twitch.tv/kato_junichi0817", "twitch"),
    ("https://play.sooplive.com/leesh2148", "soop"),
    ("https://www.pandalive.co.kr/oyoua128", "pandatv"),
    # 大小写：urlparse 的 hostname 天然小写
    ("HTTPS://WWW.DOUYU.COM/9999", "douyu"),
    # 攻击形态：全部不得命中
    ("https://douyin.com.evil.com/123", ""),          # 后缀伪装主域
    ("https://evildouyin.com/123", ""),               # 前缀拼接
    ("https://evil.com/douyin.com", ""),              # 平台域在路径
    ("https://evil.com/?q=douyin.com", ""),           # 平台域在查询串
    ("https://douyin.com@evil.com/x", ""),            # userinfo 伪装
    ("https://douyin.com:65535@evil.com/x", ""),      # userinfo 带端口伪装
    # 无 scheme / 空串 / 畸形：解析不出 hostname 一律未知
    ("douyin.com/123456", ""),
    ("", ""),
    ("http://", ""),
    ("http://[invalid", ""),
    # CDN 域不是平台页（分派层不认，代理层才认）
    ("https://video-weaver.ar.hls.ttvnw.net/x.ts", ""),
])
def test_site_of_url_matrix(url, want):
    assert main._site_of_url(url) == want


@pytest.mark.parametrize("host,domain,want", [
    ("douyin.com", "douyin.com", True),
    ("live.douyin.com", "douyin.com", True),
    ("douyin.com.evil.com", "douyin.com", False),
    ("evil.com", "douyin.com", False),
    ("", "douyin.com", False),
])
def test_host_matches(host, domain, want):
    assert main._host_matches(host, domain) == want


def test_platform_sites_table_matches_old_dispatch_set():
    """平台表必须覆盖旧 _parse_dispatch 支持的全部 7 家（不能收口顺手砍平台）。"""
    assert set(main.PLATFORM_SITES.values()) == {
        "huya", "douyu", "bilibili", "douyin", "twitch", "soop", "pandatv",
    }


# ---------- B. _parse_dispatch 路由与拒绝 ----------

def _dispatch(url, cookie=""):
    return asyncio.run(main._parse_dispatch(url, cookie))


@pytest.fixture()
def spy_parsers(monkeypatch):
    """把 7 个 parse_xxx 换成记录调用的桩——只验证路由，不碰网络。"""
    calls = []

    def make(name):
        async def fake(url, cookie=""):
            calls.append((name, url, cookie))
            return {"streams": [], "isLive": False, "title": "", "avatar": ""}
        return fake

    async def fake_panda(url):
        calls.append(("pandatv", url, None))
        return {"streams": [], "isLive": False, "title": "", "avatar": ""}

    monkeypatch.setattr(main, "parse_huya", make("huya"))
    monkeypatch.setattr(main, "parse_douyu", make("douyu"))
    monkeypatch.setattr(main, "parse_bilibili", make("bilibili"))
    monkeypatch.setattr(main, "parse_douyin", make("douyin"))
    monkeypatch.setattr(main, "parse_twitch", make("twitch"))
    monkeypatch.setattr(main, "parse_soop", make("soop"))
    monkeypatch.setattr(main, "parse_panda_manual", fake_panda)
    return calls


@pytest.mark.parametrize("url,expect_site", [
    ("https://www.huya.com/660000", "huya"),
    ("https://www.douyu.com/9999", "douyu"),
    ("https://live.bilibili.com/5440", "bilibili"),
    ("https://live.douyin.com/123456", "douyin"),
    ("https://www.twitch.tv/kato_junichi0817", "twitch"),
    ("https://play.sooplive.com/leesh2148", "soop"),
    ("https://www.pandalive.co.kr/oyoua128", "pandatv"),
])
def test_dispatch_routes_by_hostname(spy_parsers, url, expect_site):
    result = _dispatch(url, cookie="ck")
    assert result["isLive"] is False          # 桩结果原样透传
    assert spy_parsers[-1][0] == expect_site  # 路由到正确的 parse_xxx
    assert spy_parsers[-1][1] == url          # URL 原样透传


@pytest.mark.parametrize("url,cookie,want_cookie", [
    ("https://www.huya.com/660000", "ck", "ck"),
    ("https://www.huya.com/660000", "", ""),
])
def test_dispatch_passes_cookie_through(spy_parsers, url, cookie, want_cookie):
    _dispatch(url, cookie)
    assert spy_parsers[-1][2] == want_cookie


def test_dispatch_pandatv_keeps_no_cookie_signature(spy_parsers):
    """pandatv 走 parse_panda_manual(url)（无 cookie 形参）——收口不动函数签名。"""
    _dispatch("https://www.pandalive.co.kr/oyoua128")
    assert spy_parsers[-1][0] == "pandatv"
    assert spy_parsers[-1][2] is None


@pytest.mark.parametrize("url", [
    "https://douyin.com.evil.com/123",      # 旧版子串判定放行 → SSRF 直达
    "https://evil.com/?q=douyin.com",       # 同上（query 里带平台域）
    "https://evil.com/douyin.com",          # 同上（path 里带平台域）
    "https://douyin.com@evil.com/x",        # userinfo 伪装
    "https://google.com/search?q=1",        # 纯外部站点
    "douyin.com/123456",                    # 裸域名无 scheme（hostname 解析不出）
    "",
])
def test_dispatch_rejects_non_platform_urls(spy_parsers, url):
    with pytest.raises(HTTPException) as ei:
        _dispatch(url)
    assert ei.value.status_code == 400
    assert spy_parsers == []                # 拒绝发生在任何 parse_xxx 之前


def test_dispatch_parser_httpexception_not_swallowed(monkeypatch):
    """parse_xxx 抛出的 HTTPException 必须原样透传（不能被兜底 except 吞成 500）。"""

    async def boom(url, cookie=""):
        raise HTTPException(502, "上游暂时不可用")

    monkeypatch.setattr(main, "parse_huya", boom)
    with pytest.raises(HTTPException) as ei:
        _dispatch("https://www.huya.com/660000")
    assert ei.value.status_code == 502


# ---------- C. parse_douyin SSRF 收口（零网络调用） ----------

@pytest.fixture()
def no_network(monkeypatch):
    """抖音路径的两处裸抓取全部换成哨兵——被触达即让测试失败。"""
    touches = []

    class SentinelLive:
        def __init__(self, *a, **kw):
            touches.append("DouyinLiveStream()")

        async def fetch_web_stream_data(self, url, process_data=False):
            touches.append("fetch_web_stream_data")
            raise AssertionError("parse_douyin 不应触达网络（SSRF 收口测试）")

    async def sentinel_retry(method, url, **kw):
        touches.append(f"request_with_retry:{method}")
        raise AssertionError("parse_douyin 不应触达网络（SSRF 收口测试）")

    monkeypatch.setattr(main, "DouyinLiveStream", SentinelLive)
    monkeypatch.setattr(main, "request_with_retry", sentinel_retry)
    return touches


@pytest.mark.parametrize("url", [
    "https://douyin.com.evil.com/123",
    "https://evil.com/?q=douyin.com",
    "https://douyin.com@evil.com/x",
    "https://live.douyin.com.evil.com/123",
])
def test_douyin_gate_rejects_before_any_fetch(no_network, url):
    with pytest.raises(HTTPException) as ei:
        asyncio.run(main.parse_douyin(url))
    assert ei.value.status_code == 400
    assert no_network == []   # 构造 DouyinLiveStream / fetch / request_with_retry 全零


def test_douyin_gate_passes_legit_url_no_network(no_network):
    """合法抖音链接不被闸门拦（不抛 400）：过闸后继续走解析流程，走到网络层
    才被哨兵拦下、由原有 except Exception 兜底成空结果——证明不是「一刀切全拒」。"""
    result = asyncio.run(main.parse_douyin("https://live.douyin.com/123456"))
    assert result == {"streams": [], "isLive": False, "title": "", "avatar": ""}
    # 过闸证据：确实构造了 DouyinLiveStream 并发起 fetch（哨兵随后拦截）；
    # request_with_retry 未触达即可（首轮 fetch 失败就走不到 room_id 补全分支）。
    assert no_network[:2] == ["DouyinLiveStream()", "fetch_web_stream_data"]


# ---------- D. api_proxy 内外代理池判定（_use_external_proxies） ----------

@pytest.mark.parametrize("url,referer,want", [
    # 海外平台域 / 流 CDN 域：走外代池
    ("https://www.twitch.tv/xxx", "", True),
    ("https://usher.ttvnw.net/api/channel/hls/xxx.m3u8", "", True),
    ("https://video-weaver.ar.hls.ttvnw.net/x.ts", "", True),
    ("https://play.sooplive.com/live/xxx.m3u8", "", True),
    ("https://livestream-manager.sooplive.com/x", "", True),   # 旧表里的子域，后缀匹配兼容
    ("https://nlive.pandalive.co.kr/live/xxx.m3u8", "", True),
    ("https://live-video.net/stream/x", "", True),
    ("https://pandalivecdn.com/live/x", "", True),
    # 国内域：内代池
    ("https://www.douyu.com/9999", "", False),
    ("https://pubbilivideo.douyucdn.cn/x.flv", "", False),
    # 【钉住修复】平台域出现在查询串/路径：旧 any(d in url) 会误切，现按主机名判定
    ("https://www.douyu.com/api/redirect?target=https://twitch.tv", "", False),
    ("https://hw.flv.huya.com/twitch-cdn/path.flv", "", False),
    # referer 命中海外站 → 外代池（播放器经 api_proxy 取海外流的真实形态）
    ("https://www.douyu.com/9999", "https://player.twitch.tv/", True),
    ("https://www.douyu.com/9999", "https://www.pandalive.co.kr/", True),
    # 【钉住修复】referer 里平台域只出现在路径：旧子串误判，现不切
    ("https://www.douyu.com/9999", "https://evil.com/twitch.tv", False),
    # referer 为空串
    ("https://www.douyu.com/9999", "", False),
])
def test_use_external_proxies_matrix(url, referer, want):
    assert main._use_external_proxies(url, referer) == want


def test_use_external_proxies_default_referer_empty():
    """referer 缺省值为空串（api_proxy 现调用点不传 referer 时语义不变）。"""
    assert main._use_external_proxies("https://www.douyu.com/x") is False


# ---------- E. m3u8 海外流判定（_is_foreign_stream 同款调用） ----------

@pytest.mark.parametrize("url,want", [
    ("https://video-weaver.xxx.hls.ttvnw.net/v/x.ts", True),
    ("https://play.sooplive.com/live/x.m3u8", True),
    # 【钉住修复】国内 CDN 路径里含 twitch/sooplive 字样：旧子串误判为海外流
    # （会被改走 CF Worker + 海外 referer），现按主机名判定为国内
    ("https://pubbilivideo.douyucdn.cn/twitch/path.flv", False),
    ("https://hw.flv.huya.com/sooplive/path.flv", False),
    ("https://www.douyu.com/x", False),
])
def test_foreign_stream_domain_check(url, want):
    assert main._host_in(url, main.EXTERNAL_STREAM_DOMAINS) is want


# ---------- F. 关注批量分池（FOREIGN_ROOM_DOMAINS） ----------

@pytest.mark.parametrize("url,want", [
    ("https://www.twitch.tv/kato_junichi0817", True),
    ("https://play.sooplive.com/leesh2148", True),
    ("https://www.pandalive.co.kr/oyoua128", True),
    ("https://www.douyu.com/9999", False),
    ("https://live.douyin.com/123456", False),
    # 【钉住修复】旧 any(m in url) 子串：查询串带 twitch.tv 的国内房间会被分错池
    ("https://www.douyu.com/9999?from=twitch.tv", False),
    ("https://evil.com/?q=twitch.tv", False),
])
def test_foreign_room_domain_pool(url, want):
    assert main._host_in(url, main.FOREIGN_ROOM_DOMAINS) is want


# ---------- G. _browse_identity 回归（改用统一表后行为不变） ----------

@pytest.mark.parametrize("url,want", [
    ("https://www.huya.com/660000", ("huya", "660000")),
    ("https://live.bilibili.com/5440", ("bilibili", "5440")),
    ("https://play.sooplive.com/leesh2148/297819453", ("soop", "leesh2148")),  # bj_id 取第一段
    ("https://www.pandalive.co.kr/oyoua128", ("pandatv", "oyoua128")),
    ("https://douyin.com.evil.com/123", ("", "123")),  # 未知平台 site 为空
])
def test_browse_identity_unchanged(url, want):
    assert main._browse_identity(url) == want

"""resolve_site 单测（backend/browse_engine.py：任意直播链接 → (site_id, room_id)）。

关键回归锚点（历史踩坑，docstring 同源）：
  - SOOP：房间标识是路径【第一段】bj_id——/leesh2148/297819453 里的 297819453 是场次号，
    绝不能当房间号；/leesh2148 也不能抠数字抠成 2148；
  - Twitch/PandaTV：频道名可含数字（kato_junichi0817 / oyoua128），原样返回不抠数字；
  - query/fragment 丢弃；无路径 → (site, None)；未登记域名/非 URL → (None, None)。
"""
import pytest

from browse_engine import resolve_site


@pytest.mark.parametrize(
    "url, expected",
    [
        # ---- SOOP：取第一段 bj_id，场次号不误当房间 ----
        ("https://play.sooplive.com/leesh2148", ("soop", "leesh2148")),
        ("https://play.sooplive.com/leesh2148/297819453", ("soop", "leesh2148")),
        ("https://play.sooplive.com/khm11903", ("soop", "khm11903")),
        ("https://www.afreecatv.com/abc123", ("soop", "abc123")),
        ("https://play.sooplive.com", ("soop", None)),
        ("https://play.sooplive.com:8080/leesh2148", ("soop", "leesh2148")),
        # ---- Twitch：频道名原样（含数字），不抠数字 ----
        ("https://www.twitch.tv/kato_junichi0817", ("twitch", "kato_junichi0817")),
        ("https://m.twitch.tv/oyoua128", ("twitch", "oyoua128")),
        ("https://twitch.tv", ("twitch", None)),
        ("HTTPS://WWW.TWITCH.TV/SomeChannel", ("twitch", "SomeChannel")),
        # ---- 虎牙 / 斗鱼：路径最后一段即房间号 ----
        ("https://www.huya.com/11342412", ("huya", "11342412")),
        ("https://www.huya.com/11342412/", ("huya", "11342412")),
        ("https://www.douyu.com/123456?from=xyz", ("douyu", "123456")),
        ("https://www.douyu.com/topic/xx?did=999", ("douyu", "xx")),
        # ---- 抖音系（三域名）----
        ("https://live.douyin.com/7452921559329954663", ("douyin", "7452921559329954663")),
        ("https://www.iesdouyin.com/live/123456", ("douyin", "123456")),
        ("https://live.bytedance.com/live/98765", ("douyin", "98765")),
        # ---- B站 / PandaTV ----
        ("https://live.bilibili.com/6", ("bilibili", "6")),
        ("https://www.pandalive.co.kr/oyoua128", ("pandatv", "oyoua128")),
        # ---- 未登记域名 / 非法输入 ----
        ("https://example.com/foo", (None, None)),
        ("https://www.youtube.com/watch?v=abc", (None, None)),
        ("not a url at all", (None, None)),
        ("", (None, None)),
        (None, (None, None)),
    ],
)
def test_resolve_site(url, expected):
    assert resolve_site(url) == expected


def test_resolve_site_strips_whitespace():
    assert resolve_site("  https://www.huya.com/11342412  ") == ("huya", "11342412")


def test_resolve_site_fragment_discarded():
    assert resolve_site("https://www.huya.com/11342412#frag") == ("huya", "11342412")

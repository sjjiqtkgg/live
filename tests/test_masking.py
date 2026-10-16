"""脱敏函数单测（用户实测用例全量回归 + 对抗扫描 + 已接受行为钉住）。

覆盖对象（backend/main.py）：
  - _mask_pool_url / _mask_pool_key：连接池 key（代理 URL + _t超时 + _v校验）脱敏；
  - _strip_url_userinfo / _USERINFO_RE：通用 userinfo 剥离（贪婪到最后一个 @）；
  - _scrub_proxy_secrets：日志兜底（登记字面量替换 + 通用剥离）；
  - health()：/health 的 connection_pool 输出脱敏 e2e。

历史背景（对应用户逐批评测）：
  - 补2：未登记代理走兜底不再原样泄露（结构对不上时按通用规则抹 ://user:pass@）；
  - 补3：密码带裸 @ 时贪婪到最后一个 @，不再残留半截密码（p@ss 的 ss）；
  - 已接受（宁多勿漏）：query 里带 @ 的畸形串可能多抹几个字符——
    https://example.com?email=a@b.c → https://***@b.c，用户确认接受，本测试钉住该行为。
"""
import asyncio
import re
import time

import main

# 「://xxx@」形态的凭据残留检测（输出里不允许出现除 *** 外的 userinfo）
LEAK_RE = re.compile(r"(?i)://([^/@?\s]+)@")


# ==================== _mask_pool_key：用户实测用例 ====================

def test_user_case_bare_at_in_password():
    """用户实测：http://u:p@ss@h:1_tfoo_v1（密码带裸 @ + 结构尾巴不合法）走兜底分支，
    旧输出 http://***@ss@h:1_tfoo_v1（ss 是密码后半段），必须整段抹净。"""
    out = main._mask_pool_key("http://u:p@ss@h:1_tfoo_v1")
    assert out == "http://***@h:1_tfoo_v1", out
    assert "p@ss" not in out and "ss@" not in out


# ==================== _mask_pool_key：结构分支（真实连接池 key 形态） ====================

import pytest


@pytest.mark.parametrize(
    "key, expected",
    [
        # p@ss 密码 + 小数超时（用户实测过正确的形态，不回退）
        ("http://u:p@ss@h:1_t7.5_v1", "http://h:1_t7.5_v1"),
        # 常规密码
        ("http://testuser:hunter2secret@127.0.0.1:9_t15_v1", "http://127.0.0.1:9_t15_v1"),
        # socks5 裸 @ 密码
        ("socks5://u:p@q@h:1_t15_v1", "socks5://h:1_t15_v1"),
        # 双 @（user:p@ss@host 形态）
        ("http://u:p@ss@h:1_t15_v1", "http://h:1_t15_v1"),
        # 直连条目不受影响
        ("direct_t15_v1", "direct_t15_v1"),
        # 无凭据 key 原样保留（含小数超时）
        ("http://1.2.3.4:8080_t7.5_v0", "http://1.2.3.4:8080_t7.5_v0"),
    ],
)
def test_mask_pool_key_structural(key, expected):
    assert main._mask_pool_key(key) == expected


# ==================== _mask_pool_key：兜底分支（裸 @ / 畸形变体） ====================

@pytest.mark.parametrize(
    "key, label",
    [
        ("http://u:p@ss@h:1", "裸@密码·无结构尾巴"),
        ("https://u:p@q@r@h:2_tx_v1", "多个裸@·结构尾巴不合法"),
        ("weird http://u:p@ss@h:3 mixed text", "消息里混 URL"),
        ("all://u:p@ss@h_tfoo_v0", "all:// scheme 兜底"),
        ("http://u:p@ss@h:1/path@x_tfoo_v1", "路径里也有 @（贪婪停在 /）"),
    ],
)
def test_mask_pool_key_fallback_no_credentials_left(key, label):
    out = main._mask_pool_key(key)
    assert "p@" not in out and "ss@" not in out, f"{label}: in={key} out={out}"


# ==================== _mask_pool_key：零泄露对抗扫描 ====================

ADVERSARIAL_KEYS = [
    "http://u:p@ss@h:1_tfoo_v1",
    "http://a:pw2@h:1_t7.5_v1",
    "http://u:p@ss@h:1_t7.5_v1",
    "https://onlyuser@h:2_t1_v0",
    "socks5://u:p@h:1_t15_v1",
    "all://u:p@h_t10_v1",
    "http://u:p@ss@h@g_t1_v1",
    "HTTP://U:P@SS@H:1_t15_v0",
    "ftp://user:pass@h_t9_v1",
    "http://u%40x:p%40y@h:1_t3_v1",
    "direct_t7.5_v1",
    "http://u:p@ss@h:1_t7.5_v0",
    "http://u:p@ss@h:1_tfoo_v0",
    "x://@@unparseable@weird",
]


@pytest.mark.parametrize("key", ADVERSARIAL_KEYS)
def test_mask_pool_key_zero_leak(key):
    out = main._mask_pool_key(key)
    leaks = [g for g in LEAK_RE.findall(out) if set(g) != {"*"}]
    assert not leaks, f"in={key} out={out} leaks={leaks}"


def test_mask_pool_key_unparseable_masked_whole():
    """抹完仍残留 scheme://...@ 形态（无法解析的异常 key）→ 整体打码，宁可不输出也不能泄露。
    触发条件：通用正则吃不到 userinfo（@ 在 / 之后）但形态仍可疑。"""
    assert main._mask_pool_key("weird://a/b@c_tfoo") == "<masked-pool-key>"


# ==================== _strip_url_userinfo：通用剥离 + 已接受行为钉住 ====================

def test_strip_url_userinfo_basic():
    assert main._strip_url_userinfo("http://u:secret@h:1/path") == "http://***@h:1/path"


def test_strip_url_userinfo_bare_at():
    assert main._strip_url_userinfo("http://u:p@ss@h:1") == "http://***@h:1"


def test_strip_url_userinfo_any_scheme():
    """任意 xxx:// scheme（httpx 的 all://、socks5:// 等）都覆盖。"""
    assert main._strip_url_userinfo("socks5://u:p@h:1") == "socks5://***@h:1"
    assert main._strip_url_userinfo("all://u:p@h") == "all://***@h"


def test_strip_url_userinfo_no_credentials_passthrough():
    assert main._strip_url_userinfo("http://h:1/path?a=b") == "http://h:1/path?a=b"
    assert main._strip_url_userinfo("plain text without url") == "plain text without url"


def test_strip_url_userinfo_over_redaction_accepted():
    """【已接受行为钉住】宁多勿漏：query 里带 @ 的畸形串会多抹几个字符——
    https://example.com?email=a@b.c → https://***@b.c（用户复测点名并确认接受，
    「宁多勿漏」；将来若收紧规则须同步改这里并重新评估泄露面）。"""
    assert main._strip_url_userinfo("https://example.com?email=a@b.c") == "https://***@b.c"


def test_strip_url_userinfo_multiple_in_one_line():
    out = main._strip_url_userinfo("all://u:p@ss@h and socks5://u2:p2@q@h2 in one line")
    assert "p@ss" not in out and "u2:p2" not in out, out


# ==================== _mask_proxy_url：登记代理的脱敏形态 ====================

def test_mask_proxy_url_direct():
    assert main._mask_proxy_url(None) == "直连"


def test_mask_proxy_url_no_credentials():
    assert main._mask_proxy_url("http://1.2.3.4:8080") == "http://1.2.3.4:8080"


def test_mask_proxy_url_credentials():
    out = main._mask_proxy_url("http://testuser:hunter2secret@127.0.0.1:9")
    assert out == "http://127.0.0.1:9"
    assert "hunter2" not in out


def test_mask_proxy_url_invalid():
    """urlparse 拋 ValueError（如非法 IPv6 括号）→ <invalid-proxy>。
    注：不合法但能解析的字符串（如 "://not a url"）不抛异常、原样返回——不在此分支。"""
    assert main._mask_proxy_url("http://[invalid") == "<invalid-proxy>"


# ==================== _scrub_proxy_secrets：日志兜底 ====================

def test_scrub_unregistered_bare_at():
    s = main._scrub_proxy_secrets("connect error to http://u:p@ss@h:1 refused (unregistered)")
    assert "p@ss" not in s and "ss@" not in s, s
    assert "http://***@h:1" in s, s


def test_scrub_multiple_unregistered():
    s = main._scrub_proxy_secrets("all://u:p@ss@h and socks5://u2:p2@q@h2 in one line")
    assert "p@ss" not in s and "u2:p2" not in s, s


def test_scrub_registered_literal_replaced(monkeypatch):
    """登记过的代理先做字面量替换（输出最干净的无凭据形态），再过通用规则。"""
    monkeypatch.setattr(main, "PROXY_URLS", ["http://testuser:hunter2secret@127.0.0.1:9"])
    monkeypatch.setattr(main, "EXTERNAL_PROXY_URLS", [None])
    s = main._scrub_proxy_secrets("connect error to http://testuser:hunter2secret@127.0.0.1:9 refused")
    assert "hunter2" not in s and "testuser" not in s, s
    assert "http://127.0.0.1:9" in s, s


# ==================== health()：connection_pool 输出脱敏 e2e ====================

def test_health_connection_pool_masked(monkeypatch):
    """池内混放含裸 @ 密码的 key：/health 输出零凭据，且 host:port 结构保留（可运维区分）。"""
    monkeypatch.setattr(main, "CLIENT_POOL", {
        "http://u:p@ss@h:1_tfoo_v1": (None, time.time()),
        "http://a:pw2@h:1_t7.5_v1": (None, time.time()),
        "direct_t15_v1": (None, time.time()),
    })
    h = asyncio.run(main.health())
    blob = repr(h)
    for secret in ("p@ss", "ss@", "pw2"):
        assert secret not in blob, f"health() 输出含 {secret!r}: {blob}"
    assert "direct_t15_v1" in blob, "直连 key 应原样保留"
    assert "h:1_tfoo_v1" in blob and "h:1_t7.5_v1" in blob, "host:port 结构应保留"

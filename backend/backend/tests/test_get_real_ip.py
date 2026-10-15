"""get_real_ip / _peer_trusted_proxy 单测（backend/main.py 的限速取 IP 逻辑）。

规则（P0限速伪造修复后）：
  - 只信反代追加的那一跳：取 XFF 从右数第 TRUST_PROXY_HOPS 项（不信客户端可伪造的首项）；
  - 仅当直连对端 IP ∈ TRUST_PROXY_NETS 时才参考 XFF，其余场景一律用对端 IP；
  - TRUST_PROXY_HOPS=0 完全不信任 XFF；XFF 项数不足 hops 时退回对端 IP。

用最小 scope 直接构造 starlette Request（无需起服务/发请求）。
"""
import pytest
from starlette.requests import Request

import main


def make_request(client_host="127.0.0.1", xff=None):
    headers = []
    if xff is not None:
        headers.append((b"x-forwarded-for", xff.encode()))
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": headers,
        "client": (client_host, 52341) if client_host is not None else None,
        "query_string": b"",
    }
    return Request(scope)


# ==================== 可信反代（默认网段）+ XFF ====================

def test_trusted_proxy_single_xff():
    """可信对端（127.0.0.1）+ 单值 XFF → 取 XFF（该值是反代追加的真实客户端 IP）。"""
    assert main.get_real_ip(make_request("127.0.0.1", "9.9.9.9")) == "9.9.9.9"


def test_trusted_proxy_multi_xff_takes_rightmost():
    """hops=1 取 XFF 最后一项（右数第 1 项），不取客户端可伪造的首项。"""
    assert main.get_real_ip(make_request("10.0.0.1", "1.1.1.1, 2.2.2.2")) == "2.2.2.2"


def test_trusted_proxy_xff_spaces_stripped():
    assert main.get_real_ip(make_request("127.0.0.1", " 1.1.1.1 ,  2.2.2.2 ")) == "2.2.2.2"


def test_trusted_proxy_ipv6_loopback():
    assert main.get_real_ip(make_request("::1", "9.9.9.9")) == "9.9.9.9"


def test_trusted_proxy_hops_2(monkeypatch):
    """hops=2：取右数第 2 项（两级反代各追加一跳的场景）。"""
    monkeypatch.setattr(main, "TRUST_PROXY_HOPS", 2)
    assert main.get_real_ip(make_request("10.0.0.1", "1.1.1.1, 2.2.2.2, 3.3.3.3")) == "2.2.2.2"


def test_trusted_proxy_hops_2_exact_len(monkeypatch):
    monkeypatch.setattr(main, "TRUST_PROXY_HOPS", 2)
    assert main.get_real_ip(make_request("10.0.0.1", "1.1.1.1, 2.2.2.2")) == "1.1.1.1"


# ==================== 不可信对端：XFF 一律忽略 ====================

def test_untrusted_peer_ignores_spoofed_xff():
    """公网对端伪造 XFF（限速伪造攻击）→ 一律按对端 IP 限速。"""
    assert main.get_real_ip(make_request("8.8.8.8", "1.2.3.4")) == "8.8.8.8"


def test_untrusted_peer_empty_xff():
    assert main.get_real_ip(make_request("8.8.8.8", "")) == "8.8.8.8"


def test_untrusted_peer_no_xff():
    assert main.get_real_ip(make_request("8.8.8.8")) == "8.8.8.8"


# ==================== 边界：hops / XFF 缺失 ====================

def test_hops_zero_completely_ignores_xff(monkeypatch):
    """TRUST_PROXY_HOPS=0（本地直连调试）→ 完全不信任 XFF。"""
    monkeypatch.setattr(main, "TRUST_PROXY_HOPS", 0)
    assert main.get_real_ip(make_request("127.0.0.1", "1.2.3.4")) == "127.0.0.1"


def test_trusted_proxy_but_xff_shorter_than_hops(monkeypatch):
    """XFF 项数少于 hops（反代没按预期追加）→ 退回对端 IP，不越界取值。"""
    monkeypatch.setattr(main, "TRUST_PROXY_HOPS", 3)
    assert main.get_real_ip(make_request("127.0.0.1", "1.1.1.1, 2.2.2.2")) == "127.0.0.1"


def test_trusted_proxy_no_xff_header():
    assert main.get_real_ip(make_request("192.168.1.1")) == "192.168.1.1"


def test_missing_client_scope():
    """ASGI scope 无 client（测试客户端/Unix socket 场景）→ "unknown" 不崩。"""
    assert main.get_real_ip(make_request(None, "1.2.3.4")) == "unknown"


# ==================== _peer_trusted_proxy：网段判定 ====================

@pytest.mark.parametrize(
    "ip, expected",
    [
        ("127.0.0.1", True),      # 127.0.0.0/8
        ("127.0.0.99", True),
        ("10.1.2.3", True),       # 10.0.0.0/8
        ("172.16.0.1", True),     # 172.16.0.0/12
        ("192.168.0.1", True),    # 192.168.0.0/16
        ("::1", True),            # 显式 ::1
        ("8.8.8.8", False),       # 公网
        ("172.32.0.1", False),    # 172.16/12 之外
        ("not-an-ip", False),     # 非法输入
        ("", False),
    ],
)
def test_peer_trusted_proxy_default_nets(ip, expected):
    assert main._peer_trusted_proxy(ip) is expected


def test_peer_trusted_proxy_wildcard(monkeypatch):
    monkeypatch.setattr(main, "TRUST_PROXY_NETS", "0.0.0.0/0")
    assert main._peer_trusted_proxy("8.8.8.8") is True


def test_peer_trusted_proxy_star(monkeypatch):
    monkeypatch.setattr(main, "TRUST_PROXY_NETS", "*")
    assert main._peer_trusted_proxy("1.2.3.4") is True


def test_peer_trusted_proxy_invalid_net_entry_skipped(monkeypatch):
    """网段列表里混入非法条目 → 跳过该条，不影响其余判定。"""
    monkeypatch.setattr(main, "TRUST_PROXY_NETS", "not-a-net,127.0.0.0/8")
    assert main._peer_trusted_proxy("127.0.0.1") is True
    assert main._peer_trusted_proxy("8.8.8.8") is False

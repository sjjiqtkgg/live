"""Twitch 弹幕 WS 桥接单测（backend/main.py）。

背景（本批修复的丢消息根因）：Twitch IRC-over-WebSocket 一个 WebSocket 帧可含
多条以 \\r\\n 分隔的 IRC 消息（官方 chatbot 文档明说消息以 \\r\\n 分隔，一帧
多条是常态）。修复前整帧只 re.match 首条 —— 同帧其余弹幕全部丢失；PING 只查
帧首，夹在中间不回 PONG 会被 Twitch 断连。

纯函数矩阵 + 「一帧多行」端到端序列模拟（拆行 → 判 PING → 解析 PRIVMSG 的
转发循环核心逻辑，与 websocket_twitch_danmaku 的 forward_to_frontend 同序）。
"""
import main
from main import (
    _is_twitch_ping,
    _parse_twitch_privmsg,
    _split_twitch_irc_frame,
)

PRIVMSG = ":someuser!someuser@someuser.tmi.twitch.tv PRIVMSG #somechannel :"


# ==================== _split_twitch_irc_frame ====================

def test_split_single_line():
    assert _split_twitch_irc_frame(PRIVMSG + "hello") == [PRIVMSG + "hello"]


def test_split_multi_line_crlf():
    frame = PRIVMSG + "first\r\n" + PRIVMSG + "second\r\n"
    assert _split_twitch_irc_frame(frame) == [PRIVMSG + "first", PRIVMSG + "second"]


def test_split_multi_line_no_trailing_crlf():
    frame = PRIVMSG + "first\r\n" + PRIVMSG + "second"
    assert _split_twitch_irc_frame(frame) == [PRIVMSG + "first", PRIVMSG + "second"]


def test_split_lone_lf_tolerated():
    """宽松实现的客户端/服务端偶见只以 \\n 分隔，同样拆开不丢。"""
    frame = PRIVMSG + "a\n" + PRIVMSG + "b"
    assert _split_twitch_irc_frame(frame) == [PRIVMSG + "a", PRIVMSG + "b"]


def test_split_drops_blank_lines():
    frame = "\r\n\r\n" + PRIVMSG + "x\r\n\r\n"
    assert _split_twitch_irc_frame(frame) == [PRIVMSG + "x"]


def test_split_empty_frame():
    assert _split_twitch_irc_frame("") == []
    assert _split_twitch_irc_frame("\r\n\r\n") == []


def test_split_preserves_inner_content_spaces():
    """拆行不动行内容本身：消息里的空格是弹幕正文的一部分。"""
    line = PRIVMSG + "gg  well  played"
    assert _split_twitch_irc_frame(line + "\r\n") == [line]


# ==================== _is_twitch_ping ====================

def test_ping_standard():
    assert _is_twitch_ping("PING :tmi.twitch.tv") is True


def test_ping_bare():
    assert _is_twitch_ping("PING") is True


def test_ping_not_confused_by_privmsg_ending_ping():
    assert _is_twitch_ping(PRIVMSG + "ping") is False


def test_ping_not_confused_by_room_ping():
    # JOIN 前缀含 PING 字样也不该误判（startswith 从行首判定）
    assert _is_twitch_ping(":tmi.twitch.tv PING") is False


def test_pong_line_is_not_ping():
    assert _is_twitch_ping("PONG :tmi.twitch.tv") is False


# ==================== _parse_twitch_privmsg ====================

def test_parse_standard_privmsg():
    assert _parse_twitch_privmsg(PRIVMSG + "hello world") == ("someuser", "hello world")


def test_parse_nick_with_digits_underscore():
    line = ":kato_junichi0817!kato_junichi0817@kato_junichi0817.tmi.twitch.tv PRIVMSG #oyoua128 :你好"
    assert _parse_twitch_privmsg(line) == ("kato_junichi0817", "你好")


def test_parse_content_with_regex_chars_and_url():
    content = "look https://example.com/a?b=1&c=2 *** <b>hi</b>"
    assert _parse_twitch_privmsg(PRIVMSG + content) == ("someuser", content)


def test_parse_content_trailing_spaces_kept():
    # 消息体尾随空格原样保留（拆行不 strip 行内容）
    assert _parse_twitch_privmsg(PRIVMSG + "hi  ") == ("someuser", "hi  ")


def test_parse_ignores_join_notice_cap():
    for line in (
        ":someuser!someuser@someuser.tmi.twitch.tv JOIN #somechannel",
        ":tmi.twitch.tv NOTICE * :Login authentication failed",
        ":tmi.twitch.tv CAP * ACK :twitch.tv/tags",
    ):
        assert _parse_twitch_privmsg(line) is None


def test_parse_empty_content_still_matches():
    # IRC 允许空 trailing 参数；空内容照常解析（前端 addDm 有自身空值防护）
    assert _parse_twitch_privmsg(PRIVMSG) == ("someuser", "")


# ==================== 端到端序列：一帧多行（修复前丢消息的场景） ====================

def _forward_frame(frame, sent, pongs):
    """照 forward_to_frontend 的行处理顺序跑一个帧，记录发出的 chat 与 PONG。"""
    for line in _split_twitch_irc_frame(frame):
        if _is_twitch_ping(line):
            pongs.append(line)
            continue
        parsed = _parse_twitch_privmsg(line)
        if parsed:
            sent.append(parsed)


def test_frame_multi_privmsg_all_forwarded():
    """核心回归：修复前一个帧只送出首条，其余丢失。"""
    frame = PRIVMSG + "1\r\n" + PRIVMSG + "2\r\n" + PRIVMSG + "3"
    sent, pongs = [], []
    _forward_frame(frame, sent, pongs)
    assert sent == [("someuser", "1"), ("someuser", "2"), ("someuser", "3")]


def test_frame_ping_in_middle_gets_pong():
    """核心回归：PING 夹在帧中间也回 PONG（修复前只查帧首 → 被断连）。"""
    frame = PRIVMSG + "1\r\n" + "PING :tmi.twitch.tv\r\n" + PRIVMSG + "2"
    sent, pongs = [], []
    _forward_frame(frame, sent, pongs)
    assert pongs == ["PING :tmi.twitch.tv"]
    assert sent == [("someuser", "1"), ("someuser", "2")]


def test_frame_mixed_join_ping_privmsg():
    """真实常序：JOIN/PING/PRIVMSG 混一帧，只送 PRIVMSG、PING 回 PONG。"""
    frame = (
        ":someuser!someuser@someuser.tmi.twitch.tv JOIN #somechannel\r\n"
        "PING :tmi.twitch.tv\r\n"
        + PRIVMSG
        + "Kappa\r\n"
        ":tmi.twitch.tv NOTICE * :off"
    )
    sent, pongs = [], []
    _forward_frame(frame, sent, pongs)
    assert sent == [("someuser", "Kappa")]
    assert pongs == ["PING :tmi.twitch.tv"]


def test_module_attributes_importable():
    """路由处理器仍引用这些符号，确保签名重构后模块可导入且函数存在。"""
    assert callable(main._split_twitch_irc_frame)
    assert callable(main._is_twitch_ping)
    assert callable(main._parse_twitch_privmsg)
    assert callable(main.websocket_twitch_danmaku)

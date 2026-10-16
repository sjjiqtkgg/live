"""审查补丁·回归：补齐「稳健性修复无测试守护」中可单测化的两条。

背景（第四轮审查指出）：把 ①清理循环单轮异常保护 ②/api/parse 总期限
③连接超时 5 秒 ④抖音静默探活 分别改回去，397 个测试全绿——说明其中三条
没有测试守着。此处补上可离线确定性验证的两条：

- /api/parse 总期限（PARSE_TOTAL_TIMEOUT）：解析协程超期 → 504，而非无限挂起；
- 连接超时（CONNECT_TIMEOUT，默认 15s）：get_client 必须把 connect 超时接进
  httpx.Timeout（改掉 connect= 参数或用整段 timeout 覆盖时本用例变红）。

①清理循环与④抖音探活属事件循环/计时器级行为（探活窗口 60s），靠部署观察，
不进单测（见报告第五节部署清单）。
"""
from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

import main


class _FakeRequest:
    """api_parse 只用到 request.headers.get('x-platform-cookie')，字典即够。"""

    headers: dict = {}


def _raw_api_parse():
    # slowapi 装饰器包了一层；__wrapped__ 取原协程，避免在无 Request 上下文下触发限速器
    return getattr(main.api_parse, "__wrapped__", main.api_parse)


def test_parse_total_timeout_returns_504(monkeypatch):
    monkeypatch.setattr(main, "PARSE_TOTAL_TIMEOUT", 0.05)

    async def _slow(url: str, cookie: str = ""):
        await asyncio.sleep(30)
        return {"streams": []}

    monkeypatch.setattr(main, "_parse_dispatch", _slow)
    with pytest.raises(HTTPException) as ei:
        asyncio.run(_raw_api_parse()(_FakeRequest(), url="https://www.huya.com/1"))
    assert ei.value.status_code == 504, "解析超期必须 504（防协程无限堆积）"


def test_parse_fast_path_still_normalized(monkeypatch):
    async def _fast(url: str, cookie: str = ""):
        return {"streams": [], "isLive": False, "title": "t", "avatar": ""}

    monkeypatch.setattr(main, "_parse_dispatch", _fast)
    out = asyncio.run(_raw_api_parse()(_FakeRequest(), url="https://www.huya.com/1"))
    assert out["platform"] == "huya", "快路径应仍走 normalize_parse 注入平台/房间号"
    assert out["roomId"] == "1"
    assert out["lines"] == []


def test_get_client_wires_default_connect_timeout(monkeypatch):
    monkeypatch.setattr(main, "CLIENT_POOL", {})
    monkeypatch.setattr(main, "CLIENT_LOCK", asyncio.Lock())

    async def _go():
        client = await main.get_client(None, 15)
        try:
            return client.timeout
        finally:
            await client.aclose()

    timeout = asyncio.run(_go())
    assert timeout.connect == main.DEFAULT_CONNECT_TIMEOUT, (
        f"connect 超时应接线到 CONNECT_TIMEOUT（默认 15s），实际 {timeout.connect}；"
        "删掉 connect= 会让 CONNECT_TIMEOUT 失效（该常量 2026-10-16 由 5s 调回 15s）"
    )
    assert timeout.read == 15, "read/write 保持整体超时值"

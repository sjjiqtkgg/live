"""L5·审计回归：公开 /health 不再暴露 go_danmaku 内网端点（host:port）。

修复：公开面只留 available 布尔；完整端点（含端口）移到 token 保护的
/health/detail 下发。
"""
from __future__ import annotations

import asyncio

import main


def test_public_health_hides_go_danmaku_endpoint(monkeypatch):
    monkeypatch.setattr(main, "CLIENT_POOL", {})
    h = asyncio.run(main.health())
    assert h["go_danmaku"]["available"] in (True, False)
    assert "endpoint" not in h["go_danmaku"], "公开 /health 不应含端点字段"
    blob = repr(h)
    assert main.GO_DANMAKU_PORT not in blob, f"公开 /health 不应泄露端口号: {blob}"
    assert main.GO_DANMAKU_HOST not in blob, f"公开 /health 不应泄露内网主机名: {blob}"


class _FakeRequest:
    def __init__(self, token: str) -> None:
        self.headers = {"x-health-token": token}


def test_detail_health_exposes_endpoint_with_token(monkeypatch):
    monkeypatch.setattr(main, "HEALTH_TOKEN", "unit-test-token")
    fn = getattr(main.health_detail, "__wrapped__", main.health_detail)
    d = asyncio.run(fn(_FakeRequest("unit-test-token")))
    assert d["go_danmaku"]["endpoint"] == main.GO_DANMAKU_BASE, "token 保护接口应下发端点明细"


def test_detail_health_rejects_bad_token(monkeypatch):
    from fastapi import HTTPException

    monkeypatch.setattr(main, "HEALTH_TOKEN", "unit-test-token")
    fn = getattr(main.health_detail, "__wrapped__", main.health_detail)
    try:
        asyncio.run(fn(_FakeRequest("wrong-token")))
        raise AssertionError("错误 token 应被拒绝")
    except HTTPException as e:
        assert e.status_code == 403

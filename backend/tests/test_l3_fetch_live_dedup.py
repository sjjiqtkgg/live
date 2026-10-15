"""L3·审计回归：统一解析引擎 fetch_live 的多清晰度去重。

原实现把 qualityId 字符串与流记录混在同一个列表去重：平台未回 quality 字段时
qualityId 恒为空串，首条入列后 "" 即视为「已存在」→ 第二条起全被丢弃
（多清晰度平台只剩一条流）。修复后：独立去重集合；空 qualityId 以 URL 为
去重键（不同清晰度全保留），有 qualityId 时按 qualityId 去重语义不变。

fetch_live 当前无调用方（死代码），本测试防止将来启用时踩回旧坑。
"""
from __future__ import annotations

import asyncio
import json
import types

import browse_engine


class _StreamObj:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def to_json(self) -> str:
        return json.dumps(self._payload)


class _FakeNoQualityField:
    """模拟“不回 quality 字段”的平台：5 个档位各返回不同 URL。"""

    def __init__(self, *args, **kwargs) -> None:  # noqa: D401 —— 吞掉各版本构造签名
        pass

    async def fetch_web_stream_data(self, url, process_data=False):
        return {"is_live": True, "title": "t"}

    async def fetch_stream_url(self, raw, q):
        # 关键：没有 quality 字段 → _norm_quality 的 qualityId 为空串
        return _StreamObj({"m3u8_url": f"https://cdn.example/{q}.m3u8"})


class _FakeSameQuality:
    """所有档位回同一 qualityId+URL：应去重为 1 条。"""

    def __init__(self, *args, **kwargs) -> None:
        pass

    async def fetch_web_stream_data(self, url, process_data=False):
        return {"is_live": True, "title": "t"}

    async def fetch_stream_url(self, raw, q):
        return _StreamObj({"m3u8_url": "https://cdn.example/same.m3u8", "quality": "OD"})


def _patch_streamget(monkeypatch, cls) -> None:
    monkeypatch.setattr(browse_engine, "streamget", types.SimpleNamespace(HuyaLiveStream=cls))


def test_fetch_live_keeps_all_qualities_without_quality_field(monkeypatch):
    _patch_streamget(monkeypatch, _FakeNoQualityField)
    data = asyncio.run(browse_engine.fetch_live("https://www.huya.com/123"))
    assert data["isLive"] is True
    assert len(data["lines"]) == 5, f"5 个档位应全部保留（修复前只剩 1 条）: {data['lines']}"
    assert len({l["url"] for l in data["lines"]}) == 5, "各档位 URL 应齐全且互异"


def test_fetch_live_dedups_identical_lines(monkeypatch):
    _patch_streamget(monkeypatch, _FakeSameQuality)
    data = asyncio.run(browse_engine.fetch_live("https://www.huya.com/123"))
    assert len(data["lines"]) == 1, f"同 qualityId/URL 应只保留一条: {data['lines']}"

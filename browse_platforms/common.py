"""Shared HTTP and model helpers for platform browse adapters."""
from __future__ import annotations
import re
from typing import Any
import httpx

class Http:
    def __init__(self, extra_headers: dict[str, str] | None = None) -> None:
        self.extra = extra_headers or {}
        self._client: httpx.AsyncClient | None = None
    async def _get(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=18, follow_redirects=True)
        return self._client
    async def get_json(self, url: str, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None) -> Any:
        response = await (await self._get()).get(url, params=params, headers={**self.extra, **(headers or {})})
        response.raise_for_status()
        return response.json()
    async def post_json(self, url: str, data: dict[str, Any] | None = None, json: Any = None, headers: dict[str, str] | None = None) -> Any:
        response = await (await self._get()).post(url, data=data, json=json, headers={**self.extra, **(headers or {})})
        response.raise_for_status()
        return response.json()
    async def get_text(self, url: str, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None) -> str:
        response = await (await self._get()).get(url, params=params, headers={**self.extra, **(headers or {})})
        response.raise_for_status()
        return response.text
    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

# 【P1：strip_tags 泛化】旧实现只删 <em…>（搜索关键词高亮）；各平台标题/昵称里
# 还会混入 <b>/<br>/<a> 等真实标签（SOOP/Douyu 实测），名字叫 strip_tags 实际删不全。
# 泛化为「真标签」剥离：</开头是字母（或 /字母）且不含 <> 的片段；刻意不匹配
# 「分数<100分>」这类字面角括号（< 后跟数字不是标签），避免误吞纯文本。
_TAG_RE = re.compile(r"</?[a-zA-Z][^<>]*>")


def strip_tags(text: Any) -> str:
    """剥离 HTML 标签（em 高亮 / b、br 等真标签），保留纯文本。"""
    return _TAG_RE.sub("", str(text or ""))

def as_str(v: Any, default: str = "") -> str:
    return str(v) if v is not None else default

def as_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default

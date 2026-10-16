"""Shared HTTP and model helpers for platform browse adapters."""
from __future__ import annotations
import os
import re
from typing import Any, Awaitable, Callable
from urllib.parse import urlparse
import httpx

# 【2026-10-16 试验开关·BROWSE_VIA_PROXY】国内平台的浏览请求（推荐/分区/分类/搜索）改走
# 国内代理池——复用解析链路 request_with_retry：顺序换代理 → 全挂直连兜底 → 冷静期 → 日志
# （成功行带 [browse] 标签，可看清走了哪台代理）。默认 0=直连（原行为，回退只需删变量）。
# 海外平台适配器（twitch/soop/pandatv）与未知域名恒直连，不受本开关影响。
_BROWSE_VIA_PROXY = os.getenv("BROWSE_VIA_PROXY", "0") == "1"
_DOMESTIC_BROWSE_DOMAINS = ("huya.com", "douyu.com", "douyucdn.cn", "bilibili.com", "douyin.com")

# 池取函数由 main.py 装配时注入（与 browse_api.configure_parser 同一姿势：避免
# main → browse_api → browse_platforms 的循环导入；未注入时即使开关打开也退回直连，
# 保证 browse_api 被单独加载/测试时不炸）
POOL_FETCH: Callable[..., Awaitable[httpx.Response]] | None = None


def configure_pool_fetch(fn: Callable[..., Awaitable[httpx.Response]]) -> None:
    global POOL_FETCH
    POOL_FETCH = fn


def _browse_via_proxy(url: str) -> bool:
    """本请求是否改走国内代理池：开关开启 + 已装配池取函数 + 目标是国内平台域（含子域）。"""
    if not _BROWSE_VIA_PROXY or POOL_FETCH is None:
        return False
    host = (urlparse(url).hostname or "").lower()
    return any(host == d or host.endswith("." + d) for d in _DOMESTIC_BROWSE_DOMAINS)


class Http:
    def __init__(self, extra_headers: dict[str, str] | None = None) -> None:
        self.extra = extra_headers or {}
        self._client: httpx.AsyncClient | None = None
    async def _get(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=18, follow_redirects=True)
        return self._client
    async def _pool_request(self, method: str, url: str, *, params: dict[str, Any] | None = None,
                            data: dict[str, Any] | None = None, json: Any = None,
                            headers: dict[str, str] | None = None) -> httpx.Response:
        # _browse_via_proxy 已保证 POOL_FETCH 非空
        return await POOL_FETCH(method, url, params=params, data=data, json=json,
                                headers={**self.extra, **(headers or {})}, timeout=18, log_tag="browse")
    async def get_json(self, url: str, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None) -> Any:
        if _browse_via_proxy(url):
            response = await self._pool_request("GET", url, params=params, headers=headers)
        else:
            response = await (await self._get()).get(url, params=params, headers={**self.extra, **(headers or {})})
        response.raise_for_status()
        return response.json()
    async def post_json(self, url: str, data: dict[str, Any] | None = None, json: Any = None, headers: dict[str, str] | None = None) -> Any:
        if _browse_via_proxy(url):
            response = await self._pool_request("POST", url, data=data, json=json, headers=headers)
        else:
            response = await (await self._get()).post(url, data=data, json=json, headers={**self.extra, **(headers or {})})
        response.raise_for_status()
        return response.json()
    async def get_text(self, url: str, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None) -> str:
        if _browse_via_proxy(url):
            response = await self._pool_request("GET", url, params=params, headers=headers)
        else:
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

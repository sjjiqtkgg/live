"""Browse APIs registered on the existing uploaded main.py FastAPI app."""
from __future__ import annotations

import logging
import os
from typing import Any, Awaitable, Callable

from fastapi import APIRouter, HTTPException, Query, Request
from urllib.parse import quote, urlparse

from browse_platforms import make_room_url, registry

log = logging.getLogger("live.browse")
router = APIRouter(prefix="/api")
Parser = Callable[[str, str], Awaitable[dict[str, Any]]]
_parser: Parser | None = None

_COOKIE_ENV = {
    "huya": "HUYA_COOKIE", "douyu": "DOUYU_COOKIE", "bilibili": "BILI_COOKIE",
    "douyin": "DOUYIN_COOKIE", "twitch": "TWITCH_COOKIE", "soop": "SOOP_COOKIE", "pandatv": "PANDA_COOKIE",
}


def configure_parser(parser: Parser) -> None:
    global _parser
    _parser = parser


def _cookie_for(site: str, request: Request | None = None, explicit: str = "") -> str:
    if explicit:
        return explicit
    if request is not None:
        header_cookie = request.headers.get("x-platform-cookie", "")
        if header_cookie:
            return header_cookie
    env = _COOKIE_ENV.get(site.lower())
    return os.getenv(env, "") if env else ""


def _adapter(site: str, cookie: str = ""):
    adapter = registry.get(site, cookie)
    if adapter is None:
        raise HTTPException(404, f"不支持的平台: {site}")
    return adapter


async def _close(adapter: Any) -> None:
    close = getattr(adapter, "close", None)
    if close:
        try:
            await close()
        except Exception:
            log.debug("adapter close failed", exc_info=True)


def _room(room: dict[str, Any], site: str, room_id: str = "") -> dict[str, Any]:
    return {
        **room,
        "roomId": str(room.get("roomId") or room_id),
        "title": str(room.get("title") or ""),
        "nickname": str(room.get("nickname") or ""),
        "avatar": _wrap_image_url(str(room.get("avatar") or "")),
        "cover": _wrap_image_url(str(room.get("cover") or "")),
        "area": str(room.get("area") or ""),
        "watching": str(room.get("watching") or ""),
        "isLive": bool(room.get("isLive", True)),
        "platform": site,
    }


# 海外平台流域名 → 走 CF Worker 代理（浏览器直连 403/无 CORS）；referer 与 worker.js 的重写规则配套
# sooplive.co.kr / afreecatv.com 是 SOOP（原 AfreecaTV）的旧 CDN 域名变体，一併入表（worker 白名单本就允许）
_WORKER_REFERRERS = [
    (("ttvnw.net", "twitch.tv", "twitchsvc.net"), "https://player.twitch.tv"),
    (("pandalive.co.kr", "pandalivecdn.com", "live-video.net"), "https://www.pandalive.co.kr/"),
    (("sooplive.com", "sooplive.net", "sooplivecdn.com", "sooplive.co.kr", "afreecatv.com"), "https://play.sooplive.com"),
]


# 海外平台图片域名（Twitch 缩略图/头像、SOOP/Panda 封面头像）国内直连被墙，
# 照原代码 isForeignAvatar 方案统一包一层 CF Worker 的 /avatar 代理
#（worker.js /avatar 路由已内置这些域名的 Referer 映射，失败时还有 1px 兜底图）；
# 国内平台图片域名不在列表内，原样直连不受影响。
# 【L2·审计修复】原判定是子串匹配（"sooplive"/"pandalive" 出现在路径/查询串也会
# 误包 Worker）→ 改 hostname 后缀白名单，与 main.py 的 _host_in 判定风格统一。
# 域族覆盖与原关键词对真实域名等价：jtvnw/ttvnw（Twitch 图床）、sooplive 系
#（含旧 AfreecaTV）、pandalive 系。
_FOREIGN_IMG_DOMAINS = (
    "jtvnw.net", "ttvnw.net",
    "sooplive.com", "sooplive.co.kr", "sooplive.net", "sooplivecdn.com", "afreecatv.com",
    "pandalive.co.kr", "pandalivecdn.com",
)


def _is_foreign_img_host(hostname: str) -> bool:
    return any(hostname == d or hostname.endswith("." + d) for d in _FOREIGN_IMG_DOMAINS)


def _wrap_image_url(url: str) -> str:
    """海外平台封面/头像走 CF Worker /avatar 代理；未配置 CF_WORKER_URL 时保持直连。"""
    if not url or not url.startswith("http"):
        return url
    worker = os.getenv("CF_WORKER_URL", "").strip().rstrip("/")
    if not worker or url.startswith(worker):
        return url
    try:
        hostname = (urlparse(url).hostname or "").lower()
    except ValueError:
        return url
    if _is_foreign_img_host(hostname):
        if url.startswith("http://"):
            url = "https://" + url[len("http://"):]
        return f"{worker}/avatar?url={quote(url, safe='')}"
    return url


def _wrap_stream_url(url: str) -> str:
    """国内流 http→https（https 页面 fetch http 会被浏览器混合内容拦截）；
    海外流包一层 CF Worker（直连 403 / 无 CORS，Worker 已实测可用）。"""
    if url.startswith("http://"):
        url = "https://" + url[len("http://"):]
    worker = os.getenv("CF_WORKER_URL", "").strip().rstrip("/")
    if not worker or url.startswith(worker):
        return url
    low = url.lower()
    for markers, referer in _WORKER_REFERRERS:
        if any(m in low for m in markers):
            return f"{worker}?url={quote(url, safe='')}&referer={quote(referer, safe='')}"
    return url


def normalize_parse(data: dict[str, Any], site: str = "", room_id: str = "") -> dict[str, Any]:
    """Preserve legacy streams while adding Vue's lines/qualities aliases."""
    result = dict(data or {})
    streams = result.get("streams") if isinstance(result.get("streams"), list) else []
    lines: list[dict[str, Any]] = []
    qualities: list[dict[str, str]] = []
    seen_urls: set[str] = set()
    wrapped_urls: set[str] = set()
    seen_qualities: set[str] = set()
    for source in streams:
        if not isinstance(source, dict):
            continue
        raw = str(source.get("url") or "").strip()
        if not raw or raw in seen_urls:
            continue
        seen_urls.add(raw)
        url = _wrap_stream_url(raw)
        wrapped_urls.add(url)
        source["url"] = url  # streams 与 lines 保持同一份地址
        quality = str(source.get("quality") or source.get("cdn") or "原画")
        qid = str(source.get("qualityId") or quality)
        line = {
            **source,
            "label": str(source.get("label") or source.get("cdn") or quality),
            "quality": quality,
            "qualityId": qid,
            "url": url,
            "type": str(source.get("type") or ("m3u8" if ".m3u8" in url.lower() else "flv")),
        }
        lines.append(line)
        if qid not in seen_qualities:
            seen_qualities.add(qid)
            qualities.append({"id": qid, "label": quality})
    result.update({
        "roomId": room_id or str(result.get("roomId") or ""),
        "platform": site or str(result.get("platform") or ""),
        "nickname": str(result.get("nickname") or result.get("anchor_name") or ""),
        # 头像/封面也过图片分流（原版渲染层 getFinalAvatarUrl 的后端化）：
        # 海外域名 → Worker /avatar；国内 → 直连。_wrap_image_url 对已包装/非 http 地址是幂等安全的
        "avatar": _wrap_image_url(str(result.get("avatar") or "")),
        "cover": _wrap_image_url(str(result.get("cover") or "")),
        "lines": lines,
        "qualities": qualities,
    })
    if lines and isinstance(result.get("streams"), list):
        result["streams"] = [x for x in result["streams"] if isinstance(x, dict) and str(x.get("url") or "") in wrapped_urls]
    return result


async def _parse_main(url: str, cookie: str, site: str, room_id: str) -> dict[str, Any]:
    if _parser is None:
        raise HTTPException(500, "browse router is not bound to main._parse_dispatch")
    return normalize_parse(await _parser(url, cookie), site, room_id)


@router.get("/{site}/getCategories")
async def get_categories(site: str, request: Request):
    adapter = _adapter(site, _cookie_for(site, request))
    try:
        categories = await adapter.get_categories()
        # 分类图（Twitch 游戏封面等海外 CDN 图）同样包 Worker 代理
        for cat in categories if isinstance(categories, list) else []:
            if isinstance(cat, dict):
                cat["pic"] = _wrap_image_url(str(cat.get("pic") or ""))
                for sub in cat.get("children") or []:
                    if isinstance(sub, dict):
                        sub["pic"] = _wrap_image_url(str(sub.get("pic") or ""))
        return {"site": site, "name": adapter.name, "categories": categories}
    finally:
        await _close(adapter)


@router.get("/{site}/getCategoryRooms")
async def get_category_rooms(site: str, request: Request, parentAreaId: str = Query(""), areaId: str = Query(""), page: int = Query(1, ge=1), pageSize: int = Query(24, ge=1, le=100)):
    adapter = _adapter(site, _cookie_for(site, request))
    try:
        result = await adapter.get_category_rooms(parentAreaId, areaId, page, pageSize)
        rows = result.get("list", [])
        return {"site": site, **result, "list": [_room(item, site) for item in rows if isinstance(item, dict)]}
    finally:
        await _close(adapter)


@router.get("/{site}/getRecommendRooms")
async def get_recommend_rooms(site: str, request: Request, page: int = Query(1, ge=1), pageSize: int = Query(24, ge=1, le=100)):
    adapter = _adapter(site, _cookie_for(site, request))
    try:
        result = await adapter.get_recommend_rooms(page, pageSize)
        rows = result.get("list", [])
        return {"site": site, **result, "list": [_room(item, site) for item in rows if isinstance(item, dict)]}
    finally:
        await _close(adapter)


@router.get("/{site}/searchRooms")
async def search_rooms(site: str, request: Request, kw: str = Query(""), page: int = Query(1, ge=1), pageSize: int = Query(24, ge=1, le=100)):
    if not kw.strip():
        return {"site": site, "list": [], "hasMore": False}
    adapter = _adapter(site, _cookie_for(site, request))
    try:
        result = await adapter.search_rooms(kw.strip(), page, pageSize)
        rows = result.get("list", [])
        return {"site": site, **result, "list": [_room(item, site) for item in rows if isinstance(item, dict)]}
    finally:
        await _close(adapter)


def _ws_base(request: Request) -> str:
    """推导弹幕 WebSocket 的对外绝对地址；支持反代/TLS 终止，也允许用 WS_PUBLIC_BASE 显式指定。"""
    override = os.getenv("WS_PUBLIC_BASE", "").strip()
    if override:
        return override.rstrip("/")
    proto = request.headers.get("x-forwarded-proto", request.url.scheme)
    ws_proto = "wss" if proto == "https" else "ws"
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc
    return f"{ws_proto}://{host}"


def _ws_for(site: str, room_id: str, request: Request, parsed: dict[str, Any]) -> str | None:
    """详情响应里下发的弹幕 WebSocket 地址；平台没实现网页弹幕时返回 None。

    douyin/twitch/soop 由 main.py 的 /ws/* 端点代理；huya 由前端直连平台
    （Tars 二进制协议），只需在 detail 里附 danmaku 参数。
    """
    ws = parsed.get("ws")
    if not ws and site in ("douyin", "twitch", "soop"):
        ws = f"{_ws_base(request)}/ws/{site}/{room_id}"
    return ws or None


@router.get("/{site}/getRoomDetail/{room_id}")
async def get_room_detail(site: str, room_id: str, request: Request):
    adapter = _adapter(site, _cookie_for(site, request))
    try:
        detail = await adapter.get_room_detail(room_id)
    finally:
        await _close(adapter)
    room_url = make_room_url(site, room_id)
    if not room_url:
        raise HTTPException(400, f"无法拼接 {site} 房间链接")
    if detail is None:
        # 详情接口没拿到元信息（如虎牙），直接用解析结果作为详情返回
        parsed = await _parse_main(room_url, _cookie_for(site, request), site, room_id)
        ws = _ws_for(site, room_id, request, parsed)
        if ws:
            parsed["ws"] = ws
        return _room(parsed, site, room_id)
    # 浏览器播放器依赖详情里的流地址：各平台详情接口只回元信息，需要再取一次流并合并。
    try:
        parsed = await _parse_main(room_url, _cookie_for(site, request), site, room_id)
        detail.update({
            "streams": parsed.get("streams") or [],
            "lines": parsed.get("lines") or [],
            "qualities": parsed.get("qualities") or [],
        })
        if parsed.get("danmaku"):
            detail["danmaku"] = parsed["danmaku"]
        ws = _ws_for(site, room_id, request, parsed)
        if ws:
            detail["ws"] = ws
    except Exception:
        log.debug(f"{site} {room_id} 详情取流失败，仅返回元信息", exc_info=True)
    return _room(detail, site, room_id)


@router.get("/{site}/getRoomStream/{room_id}")
async def get_room_stream(site: str, room_id: str, request: Request):
    # P0-7/8：去掉 cookie 查询参数（现有前端不传，_cookie_for 的 x-platform-cookie
    # 头已覆盖同一能力；查询串里的凭据会落反代/服务日志与浏览器历史）
    room_url = make_room_url(site, room_id)
    if not room_url:
        raise HTTPException(400, f"无法拼接 {site} 房间链接")
    return await _parse_main(room_url, _cookie_for(site, request), site, room_id)


@router.get("/{site}/getLiveStatus/{ids}")
async def get_live_status(site: str, ids: str, request: Request):
    adapter = _adapter(site, _cookie_for(site, request))
    try:
        return {"site": site, "list": await adapter.get_live_status([value for value in ids.split(",") if value][:30])}
    finally:
        await _close(adapter)

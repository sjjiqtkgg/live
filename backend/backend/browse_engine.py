"""streamget 统一解析引擎。

职责：把「任一路由地址」交给 streamget，产出我们统一的数据结构：
    { roomId, title, nickname, avatar, cover, isLive, qualities[], lines[] }

streamget 的模型是：
    cls(url=..., cookies=...)  ->  data = await live.fetch_web_stream_data(url)  ->  stream_obj = await live.fetch_stream_url(data, quality)  ->  json 里有 flv_url / m3u8_url / anchor_name

它对所有支持平台（douyin/huya/douyu/bilibili/soop/twitch/pandatv）暴露同一套类接口，
因此这里不需要按平台手写解析，全部走 streamget 即可；不同平台只在【质量档位】上做归一化。
"""
from __future__ import annotations

import json
import re
import logging
from typing import Any

# streamget 仅是本模块 fetch_live（统一解析引擎）的依赖；resolve_site 纯字符串处理不依赖它。
# 改成可选导入：streamget 未安装时不能拖垮 /api/parse（main.py 的 api_parse 会 import 本模块拿 resolve_site），
# 只影响 fetch_live/_class_for（目前无调用方），与原代码对 streamget 的 try/except 容忍姿势一致。
try:
    import streamget
except ImportError:  # pragma: no cover - 部署环境未装 streamget 时走这里
    streamget = None

log = logging.getLogger("live.engine")

# 域名 -> site id（与 streamget 平台类名对应）
_DOMAIN_TO_SITE = {
    "huya.com": "huya",
    "douyu.com": "douyu",
    "douyin.com": "douyin",
    "iesdouyin.com": "douyin",
    "live.bytedance.com": "douyin",
    "bilibili.com": "bilibili",
    "bilivideo.com": "bilibili",
    "sooplive.com": "soop",
    "afreecatv.com": "soop",
    "twitch.tv": "twitch",
    "pandalive.co.kr": "pandatv",
}

# streamget 导出的类名
_CLASS_BY_SITE = {
    "huya": "HuyaLiveStream",
    "douyu": "DouyuLiveStream",
    "douyin": "DouyinLiveStream",
    "bilibili": "BilibiliLiveStream",
    "soop": "SoopLiveStream",
    "twitch": "TwitchLiveStream",
    "pandatv": "PandaLiveStream",
}

# 质量档位：streamget 多数用 OD/UHD/HD/SD/LD（抖音系），其余平台直接用原接口的档位。
# 统一映射到前端显示名。
_QUALITY_LABELS = {
    "OD": "原画",
    "UHD": "蓝光",
    "HD": "超清",
    "SD": "高清",
    "LD": "标清",
    "FHD": "超清",
    "origin": "原画",
    "blue": "蓝光",
    "ultra": "超清",
    "high": "高清",
    "normal": "标清",
}


class ParseError(Exception):
    pass


def resolve_site(url: str) -> tuple[str | None, str | None]:
    """从任意直播链接里识别 (site_id, room_id)。

    room_id 提取规则（与各平台原网页 URL 结构对齐，也和 main.py 各 parse_xxx 的抠法一致）：
      - soop/afreecatv：play.sooplive.com/{bj_id} 或 /{bj_id}/{broad_no}，
        房间标识是【第一段】bj_id（字母数字混合，如 leesh2148/khm11903，绝不能抠数字——
        原写法取末段+抠数字会把 /leesh2148/297819453 解析成 297819453（那是场次号），
        把 /leesh2148 解析成 2148，导致前端跳错房间）；
      - 其余平台：路径最后一段即房间号/频道名，原样返回不抠数字
        （twitch/pandatv 频道名可含数字，如 kato_junichi0817/oyoua128，抠数字会得到错 ID）。
    """
    if not url:
        return None, None
    url = url.strip()
    m = re.search(r"https?://([^/?#]+)", url, re.IGNORECASE)
    if not m:
        return None, None
    host = m.group(1).split(":")[0].lower()
    if host.startswith("www."):
        host = host[4:]
    for domain, site in _DOMAIN_TO_SITE.items():
        if host == domain or host.endswith("." + domain):
            # 去掉 scheme://host，取纯路径段（丢弃 query/fragment）
            # 【补】fragment 此前实际没丢（docstring 声称丢弃但旧实现只 split("?")，
            # /room#frag 会把 #frag 拼进 room_id）——写 pytest 时暴露，一行修复
            mpath = re.search(r"https?://[^/]+(/.*)",
                              url.split("#")[0].split("?")[0].rstrip("/"), re.IGNORECASE)
            segments = [s for s in (mpath.group(1).strip("/").split("/") if mpath else []) if s]
            if not segments:
                return site, None
            if site == "soop":
                # SOOP：/{bj_id} 或 /{bj_id}/{broad_no} → 取第一段 bj_id
                return site, segments[0]
            # 其余平台：最后一段即房间号/频道名（原样，不抠数字）
            return site, segments[-1]
    return None, None


def _class_for(site: str):
    if streamget is None:
        raise ParseError("streamget 未安装（pip install streamget），统一解析引擎不可用")
    cls_name = _CLASS_BY_SITE.get(site)
    if not cls_name:
        raise ParseError(f"site {site!r} 没有对应的 streamget 平台类")
    cls = getattr(streamget, cls_name, None)
    if cls is None:
        raise ParseError(f"streamget 未导出 {cls_name}")
    return cls


def _build_instance(cls, cookies: str | None, proxy: str | None):
    """兼容不同版本 streamget 的构造签名，逐个尝试。"""
    attempts = [
        lambda: cls(cookies=cookies, proxy_addr=proxy),
        lambda: cls(cookie=cookies, proxy_addr=proxy),
        lambda: cls(cookies=cookies),
        lambda: cls(cookie=cookies),
        lambda: cls(),
    ]
    if not cookies and not proxy:
        attempts = [lambda: cls(), lambda: cls(proxy_addr=None, cookies=None)]
    last = None
    for fn in attempts:
        try:
            return fn()
        except TypeError as e:
            last = e
            continue
        except Exception:
            raise
    raise ParseError(f"无法实例化 {cls.__name__}: {last}")


def _norm_quality(item: dict[str, Any]) -> dict[str, Any]:
    """把 streamget 的单条流记录归一化成前端需要的行。"""
    label = str(item.get("quality") or item.get("type") or "").upper()
    display = _QUALITY_LABELS.get(label) or item.get("quality") or item.get("label") or "原画"
    url = (
        item.get("m3u8_url")
        or item.get("flv_url")
        or item.get("live_url")
        or item.get("url")
        or ""
    )
    if not url:
        return {}
    stream_type = "m3u8" if (".m3u8" in url.lower() or item.get("type") == "m3u8") else "flv"
    cdn = item.get("cdn") or item.get("line") or item.get("host") or "默认"
    return {
        "label": f"{cdn}-{display}",
        "cdn": cdn,
        "quality": display,
        "qualityId": item.get("quality", ""),
        "url": url,
        "type": stream_type,
    }


async def fetch_live(url: str, cookies: str | None = None, proxy: str | None = None) -> dict[str, Any]:
    """把一个直播链接解析成统一结构。"""
    site, room_id = resolve_site(url)
    if site is None:
        raise ParseError("不支持的平台链接")

    cls = _class_for(site)
    live = _build_instance(cls, cookies, proxy)

    raw = await live.fetch_web_stream_data(url, process_data=True)
    if not raw:
        raise ParseError(f"{site} 解析失败：未返回数据")

    is_live = bool(raw.get("is_live")) if isinstance(raw, dict) else bool(raw)
    anchor_name = ""
    avatar = _deep_find(raw, ("avatarThumb", "avatar_thumb", "avatar", "face", "avatar_larger"))

    # 尝试抓所有常见质量档位
    # 【L3·审计修复】原实现把 qualityId 字符串与流记录混在同一个列表做去重：
    # 平台未回 quality 字段时 qualityId 恒为空串，首条入列后 "" 即视为「已存在」，
    # 第二条起全部被丢弃（多清晰度只剩一条）。改为独立去重集合；空 qualityId
    # 退化用 URL 作去重键。
    lines: list[dict[str, Any]] = []
    seen_dedup_keys: set = set()
    for q in ("OD", "UHD", "HD", "SD", "LD"):
        try:
            stream_obj = await live.fetch_stream_url(raw, q)
            if hasattr(stream_obj, "to_json"):
                obj = stream_obj.to_json()
                if isinstance(obj, str):
                    obj = json.loads(obj)
            elif isinstance(stream_obj, dict):
                obj = stream_obj
            else:
                obj = json.loads(stream_obj.json())
            if not isinstance(obj, dict):
                continue
            if obj.get("anchor_name"):
                anchor_name = obj["anchor_name"]
            line = _norm_quality(obj)
            if line and line["url"]:
                dedup_key = line.get("qualityId") or line["url"]
                if dedup_key not in seen_dedup_keys:
                    seen_dedup_keys.add(dedup_key)
                    lines.append(line)
        except Exception:
            continue

    # 上面若失败（个别平台只给单一清晰度），回退到 get_stream_url
    if not lines:
        try:
            obj = await live.get_stream_url(raw, "OD", url_type="all")
            if isinstance(obj, dict):
                line = _norm_quality(obj)
                if line and line["url"]:
                    lines.append(line)
                if obj.get("anchor_name"):
                    anchor_name = obj["anchor_name"]
        except Exception:
            pass
    if not is_live or not lines:
        return {
            "roomId": room_id or "",
            "title": raw.get("title") if isinstance(raw, dict) else "",
            "nickname": anchor_name or "",
            "avatar": avatar,
            "cover": "",
            "isLive": bool(is_live),
            "qualities": [],
            "lines": [],
        }

    qualities = [{"id": l.get("qualityId", ""), "label": l.get("quality", l.get("cdn", ""))} for l in lines]
    return {
        "roomId": room_id or "",
        "title": raw.get("title") if isinstance(raw, dict) else "",
        "nickname": anchor_name or "",
        "avatar": avatar,
        "cover": raw.get("cover") if isinstance(raw, dict) else "",
        "isLive": True,
        "qualities": qualities,
        "lines": lines,
    }


def _deep_find(data: Any, keys: tuple[str, ...], depth: int = 0) -> str:
    """在任意嵌套字典里找第一个匹配的字符串头像地址。"""
    if depth > 14:
        return ""
    if isinstance(data, dict):
        for k, v in data.items():
            if k in keys and isinstance(v, dict):
                u = v.get("urlList") or v.get("url_list") or v.get("url") or v.get("src")
                if isinstance(u, list) and u:
                    return str(u[0])
                if isinstance(u, str):
                    return u
            if k in keys and isinstance(v, str) and v.startswith("http"):
                return v
            r = _deep_find(v, keys, depth + 1)
            if r:
                return r
    elif isinstance(data, list):
        for item in data:
            r = _deep_find(item, keys, depth + 1)
            if r:
                return r
    return ""

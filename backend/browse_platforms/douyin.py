"""Douyin browse/search adapter ported from pure_live/dart_simple_live.

The A-Bogus JS is the upstream implementation in douyin_abogus.js; execution
uses PyExecJS when available, then Node. Upstream licenses are in THIRD_PARTY_NOTICES.
"""
from __future__ import annotations
import asyncio
import json
import re
import subprocess
import threading
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, quote
import httpx
import logging
from browse_platforms.base import LivePlatform, room_dict
from browse_platforms.common import Http, as_str
from browse_platforms.area_pic_mapper import build_pic_map, get_pic
from browse_cache import LIST_CACHE

# 【P1修复】search_rooms 的 except 分支用了未定义的 log（NameError 会炸出 except 块，
# 直接把整个搜索打桧），按 twitch.py 同款方式定义模块 logger
log = logging.getLogger("live.douyin")

try:
    from streamget import DouyinLiveStream
except ImportError:
    DouyinLiveStream = None

UA = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.97 Safari/537.36 Core/1.116.567.400 QQBrowser/19.7.6764.400"
DEFAULT_COOKIE = "ttwid=1%7CB1qls3GdnZhUov9o2NxOMxxYS2ff6OSvEWbv0yTTIdES4%7C1680522049%7C280d802d6d478e3e78d0c807f7c487e7ffec0ae4e5fdd6a0fe74c3c6af149511"
JS_PATH = Path(__file__).with_name("douyin_abogus.js")

_ANON_COOKIE_KEY = "douyin_anon_cookie"


def _first_str(d: dict, keys: tuple) -> str:
    for k in keys:
        v = as_str(d.get(k))
        if v:
            return v
    return ""


def _dy_online_viewers(room: Any) -> str:
    """照 pure_live douyin_audience.dart douyinOnlineViewers：并发观众字段候选序。"""
    if not isinstance(room, dict):
        return ""
    view_stats = room.get("room_view_stats") if isinstance(room.get("room_view_stats"), dict) else {}
    room_stats = room.get("stats") if isinstance(room.get("stats"), dict) else {}
    candidates = (
        room.get("user_count"), room.get("user_count_str"), room.get("online_user_count"), room.get("online_user_for_anchor"),
        view_stats.get("user_count"), view_stats.get("online_user_count"), view_stats.get("online_user_for_anchor"),
        room_stats.get("user_count"), room_stats.get("online_user_count"), room_stats.get("online_user_for_anchor"),
    )
    for v in candidates:
        text = str(v).strip() if v is not None else ""
        if text and text != "null" and re.search(r"[0-9]", text):
            return text
    return ""


def _dy_total_viewers(room: Any) -> str:
    """照 pure_live douyinTotalViewers：累计观众字段候选序（数值 >0 才有效）。"""
    if not isinstance(room, dict):
        return ""
    view_stats = room.get("room_view_stats") if isinstance(room.get("room_view_stats"), dict) else {}
    room_stats = room.get("stats") if isinstance(room.get("stats"), dict) else {}
    candidates = (
        view_stats.get("display_value"), view_stats.get("total_user_str"), view_stats.get("total_user"),
        room_stats.get("total_user_str"), room_stats.get("total_user"),
        room.get("total_user_str"), room.get("total_user"),
    )
    for v in candidates:
        text = str(v).strip() if v is not None else ""
        if not text or text == "null" or not re.search(r"[0-9]", text):
            continue
        normalized = text.replace(",", "").replace("，", "")
        m = re.search(r"[0-9]+(?:\.[0-9]+)?", normalized)
        if m and float(m.group(0)) > 0:
            return text
    return ""


def _dy_first_text(candidates: tuple) -> str:
    for v in candidates:
        if v is None or isinstance(v, (dict, list)):
            continue
        text = str(v).strip()
        if text and text != "null":
            return text
    return ""


def _dy_first_image(v: Any) -> str:
    if isinstance(v, dict):
        urls = v.get("url_list") or v.get("urlList")
        if isinstance(urls, list):
            for u in urls:
                u = str(u)
                if u:
                    return u
    s = str(v or "")
    return s if s.startswith("http") else ""


def _looks_like_douyin_room(v: Any) -> bool:
    return isinstance(v, dict) and (isinstance(v.get("owner"), dict) or v.get("title") is not None or v.get("id_str") is not None or isinstance(v.get("stream_url"), dict))


def _extract_category_data_json(source: str) -> str:
    """照 pure_live douyin_site.extractCategoryDataJson：起点标记 + 花括号配平。

    旧正则要求 `{` 后直接跟 pathname，与页面里 `{\"pathname\"...` 的实际转义
    形态对不上（从未匹配成功过）；配平法不依赖结尾格式，实测可解析出 8 组分区。
    """
    start = source.find(r"{\"pathname\":\"/\",\"categoryData\":")
    if start == -1:
        return ""
    open_braces = 0
    found_first = False
    for i in range(start, len(source)):
        ch = source[i]
        if ch == "{":
            open_braces += 1
            found_first = True
        elif ch == "}":
            open_braces -= 1
        if found_first and open_braces == 0:
            return source[start:i + 1].replace('\\"', '"').replace("\\\\", "\\")
    return ""


class _ABogus:
    _ctx = None
    # 懒编译 + 签名串行锁：PyExecJS 的 call 与 subprocess.run 都会跨线程共享
    # _ctx，避免并发首次调用时双重初始化/竞态
    _sign_lock = threading.Lock()

    @classmethod
    def _sign_sync(cls, query: str) -> str:
        """同步签名（execjs.call / subprocess.run 均为阻塞调用，子进程冷启动数百 ms 起）。"""
        with cls._sign_lock:
            if cls._ctx is None:
                source = JS_PATH.read_text(encoding="utf-8")
                try:
                    import execjs
                    cls._ctx = execjs.compile(source)
                except Exception:
                    cls._ctx = source
            if not isinstance(cls._ctx, str):
                return str(cls._ctx.call("getABogus", query, UA))
            script = cls._ctx + "\nconsole.log(getABogus(" + json.dumps(query) + "," + json.dumps(UA) + "));"
            run = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=20, check=True)
            return run.stdout.strip().splitlines()[-1]

    @classmethod
    async def sign(cls, query: str) -> str:
        # 【本批修复】签名从 async 函数里的同步调用改为线程池执行：
        # 此前 execjs/subprocess 直接在事件循环里阻塞（子进程最长 20s），
        # 期间所有并发请求与弹幕 WS 转发全部卡死
        return await asyncio.to_thread(cls._sign_sync, query)


async def _signed_url(base: str, params: dict[str, Any], cookie: str = "") -> str:
    query = urlencode({key: str(value) for key, value in params.items()})
    ms_token = cookie.split("msToken=", 1)[1].split(";", 1)[0] if "msToken=" in cookie else ""
    if not ms_token:
        import secrets
        ms_token = secrets.token_urlsafe(96)
    signed_query = query + "&msToken=" + ms_token
    signature = await _ABogus.sign(signed_query)
    return base + "?" + signed_query + "&a_bogus=" + signature


def _first(values: list[Any]) -> str:
    for value in values:
        if value is not None and str(value).strip():
            return str(value)
    return ""


def _room(item: dict[str, Any], fallback: dict[str, Any] | None = None) -> dict[str, Any] | None:
    fallback = fallback or {}
    room = item.get("room") if isinstance(item.get("room"), dict) else {}
    owner = room.get("owner") if isinstance(room.get("owner"), dict) else {}
    cover = room.get("cover") if isinstance(room.get("cover"), dict) else {}
    avatar = owner.get("avatar_thumb") if isinstance(owner.get("avatar_thumb"), dict) else {}
    cover_urls = cover.get("url_list") if isinstance(cover.get("url_list"), list) else []
    avatar_urls = avatar.get("url_list") if isinstance(avatar.get("url_list"), list) else []
    room_id = _first([item.get("web_rid"), owner.get("web_rid"), room.get("id_str"), room.get("id")])
    if not room_id:
        return None
    road = room.get("partition_road_map") if isinstance(room.get("partition_road_map"), list) else []
    road_item = road[0] if road and isinstance(road[0], dict) else {}
    # 照 pure_live：status==2 在播、==4 已结束；分区列表无字段时默认在播
    raw_status = item.get("status", room.get("status"))
    status = (str(raw_status) == "2") if raw_status not in (None, "") else True
    # 人气照 pure_live douyin_audience：累计优先、并发兜底
    watching = (_dy_total_viewers(room) or _dy_online_viewers(room)) if status else ""
    return room_dict(
        roomId=room_id,
        title=_first([room.get("title"), item.get("title"), fallback.get("title"), owner.get("nickname")]),
        nickname=_first([owner.get("nickname"), item.get("nickname"), fallback.get("nickname")]),
        avatar=avatar_urls[0] if avatar_urls else "",
        cover=cover_urls[0] if cover_urls else str(item.get("cover_url", "")),
        area=_first([road_item.get("title"), fallback.get("tag")]),
        watching=watching, isLive=status, platform="douyin",
    )


class DouyinAdapter(LivePlatform):
    site = "douyin"
    name = "抖音直播"
    logo = "https://www.douyin.com/favicon.ico"
    def __init__(self, cookie: str = "") -> None:
        self.cookie = cookie or DEFAULT_COOKIE
        self.http = Http()
    def _headers(self, referer: str = "https://live.douyin.com/") -> dict[str, str]:
        return {"user-agent": UA, "referer": referer, "origin": "https://live.douyin.com", "cookie": self.cookie}
    def _base_params(self, page: int, partition: str, partition_type: str, page_size: int) -> dict[str, str]:
        return {"aid":"6383", "app_name":"douyin_web", "live_id":"1", "device_platform":"web", "language":"zh-CN", "enter_from":"link_share", "cookie_enabled":"true", "screen_width":"1980", "screen_height":"1080", "browser_language":"zh-CN", "browser_platform":"Win32", "browser_name":"Edge", "browser_version":"125.0.0.0", "browser_online":"true", "count":str(page_size), "offset":str((page-1)*page_size), "partition":partition, "partition_type":partition_type, "req_from":"2"}

    async def _cookie(self) -> str:
        """照 pure_live getRequestHeaders/_fetchAnonymousCookie：用户 cookie 优先，
        否则访问首页取新鲜匿名 ttwid+UIFID_TEMP（模块缓存 12h），兜底老默认 ttwid。"""
        if self.cookie:
            return self.cookie
        cached = LIST_CACHE.get(_ANON_COOKIE_KEY)
        if cached:
            return cached
        try:
            async with httpx.AsyncClient(timeout=18, follow_redirects=True) as client:
                resp = await client.get("https://live.douyin.com/", params={"from_nav": "1"},
                                        headers={"user-agent": UA, "referer": "https://live.douyin.com/"})
            pairs = []
            for value in resp.headers.get_list("set-cookie"):
                pair = value.split(";", 1)[0].strip()
                if pair.startswith("ttwid=") or pair.startswith("UIFID_TEMP="):
                    pairs.append(pair)
            fresh = "; ".join(pairs)
            if fresh:
                LIST_CACHE.set(_ANON_COOKIE_KEY, fresh, 12 * 3600)
                return fresh
        except Exception:
            pass
        return DEFAULT_COOKIE

    async def get_categories(self) -> list[dict[str, Any]]:
        """照 pure_live douyin_site.getCategores：抓首页 RENDER_DATA 的 categoryData
        （from_nav=1 + 新鲜匿名 cookie，花括号配平提取），组两级分区并把父级
        自条目插为第一个 child；无图分类用 AreaPicMapper 回填其它平台图标。"""
        cached = LIST_CACHE.get("douyin_cats")
        if cached is not None:
            return cached
        self.cookie = await self._cookie()
        try:
            text = await self.http.get_text("https://live.douyin.com/", params={"from_nav": "1"}, headers=self._headers())
            data = json.loads(_extract_category_data_json(text) or "{}")
            if isinstance(data, list):
                data = {"categoryData": data}
            result = []
            for item in data.get("categoryData", []) if isinstance(data, dict) else []:
                if not isinstance(item, dict): continue
                parent = item.get("partition", {}) or {}
                parent_id = f"{parent.get('id_str','')},{parent.get('type','')}"
                children = []
                for child in item.get("sub_partition", []) or []:
                    part = child.get("partition", {}) if isinstance(child, dict) else {}
                    children.append({"id":f"{part.get('id_str','')},{part.get('type','')}","name":as_str(part.get("title")),"pic":"","parentId":parent_id})
                result.append({"id":parent_id,"name":as_str(parent.get("title")),"pic":"","children":[{"id":parent_id,"name":as_str(parent.get("title")),"pic":"","parentId":parent_id},*children]})
            await self._fill_pics(result)
            LIST_CACHE.set("douyin_cats", result, 600)
            return result
        except Exception:
            return []

    async def _fill_pics(self, groups: list[dict[str, Any]]) -> None:
        """照 pure_live AreaPicMapper：用虎牙/斗鱼/B站的分类图标按名称回填
        抖音无图分类（精确 → 别名 → 包含 → 模糊匹配）。"""
        try:
            pic_map = LIST_CACHE.get("douyin_area_pic_map")
            if pic_map is None:
                from browse_platforms.huya import HuyaAdapter
                from browse_platforms.douyu import DouyuAdapter
                from browse_platforms.bilibili import BilibiliAdapter
                sources = []
                for adapter_cls in (HuyaAdapter, DouyuAdapter, BilibiliAdapter):
                    adapter = None
                    try:
                        adapter = adapter_cls()
                        sources.append(await adapter.get_categories())
                    except Exception:
                        continue
                    finally:
                        if adapter is not None:
                            try:
                                await adapter.close()
                            except Exception:
                                pass
                pic_map = build_pic_map(sources)
                if pic_map:
                    LIST_CACHE.set("douyin_area_pic_map", pic_map, 600)
            if pic_map:
                for group in groups:
                    for sub in group.get("children", []):
                        if not sub.get("pic"):
                            sub["pic"] = get_pic(str(sub.get("name") or ""), pic_map)
        except Exception:
            pass

    async def _partition_rooms(self, partition: str, kind: str, page: int, page_size: int) -> dict[str, Any]:
        self.cookie = await self._cookie()
        base = "https://live.douyin.com/webcast/web/partition/detail/room/v2/"
        params = self._base_params(page, partition, kind, page_size)
        signed = await _signed_url(base, params, self.cookie)
        d = await self.http.get_json(signed, headers=self._headers())
        raw = d.get("data", {}).get("data", []) if isinstance(d, dict) else []
        rooms = [entry for entry in (_room(item) for item in raw if isinstance(item, dict)) if entry]
        # 【P1二轮：hasMore】partition 接口 page/offset 生效（实测 p1-p4 各 20 条、
        # 首房间各不相同、p30 已空），响应无 has_more 元数据 → 本页非空即还有
        # 下一页（旧 20>=24 恒 False，分区第 1 页就断）。
        return {"list":rooms,"hasMore":len(rooms)>0}

    async def get_category_rooms(self, parent_area_id: str, area_id: str, page: int, page_size: int) -> dict[str, Any]:
        parts = (area_id or parent_area_id).split(",")
        return await self._partition_rooms(parts[0], parts[1] if len(parts)>1 else "1", page, page_size)

    async def get_recommend_rooms(self, page: int, page_size: int) -> dict[str, Any]:
        """照 pure_live douyin_site.getRecommendRooms：webcast/feed 推荐流（匿名可调、
        room.user_count 自带并发人数），解析兼容新旧两代响应（parseRecommendRooms）。"""
        try:
            self.cookie = await self._cookie()
            url = "https://live.douyin.com/webcast/feed/?" + urlencode({
                "aid": "6383", "app_name": "douyin_web", "need_map": "1", "is_draw": "1",
                "inner_from_drawer": "0", "enter_source": "web_homepage_hot_web_live_card",
                "source_key": "web_homepage_hot_web_live_card",
            })
            d = await self.http.get_json(url, headers=self._headers())
            if not isinstance(d, dict) or str(d.get("status_code", "1")) != "0":
                return {"list": [], "hasMore": False}
            raw = d.get("data")
            if isinstance(raw, dict):
                raw = raw.get("data")
            if not isinstance(raw, list):
                return {"list": [], "hasMore": False}
            rooms: list[dict[str, Any]] = []
            seen: set[str] = set()
            for env in raw:
                if not isinstance(env, dict):
                    continue
                embedded = env.get("data")
                if isinstance(embedded, str) and embedded.strip().startswith("{"):
                    try:
                        embedded = json.loads(embedded)
                    except Exception:
                        embedded = None
                nested = env.get("room") if isinstance(env.get("room"), dict) else None
                room = next((c for c in (embedded, nested, env) if _looks_like_douyin_room(c)), None)
                if room is None:
                    continue
                owner = room.get("owner") if isinstance(room.get("owner"), dict) else (env.get("owner") if isinstance(env.get("owner"), dict) else {})
                room_id = _dy_first_text((env.get("web_rid"), owner.get("web_rid"), room.get("web_rid"), room.get("id_str"), room.get("id")))
                if not room_id or room_id in seen:
                    continue
                seen.add(room_id)
                rooms.append(room_dict(
                    roomId=room_id,
                    title=_dy_first_text((room.get("title"), env.get("title"), owner.get("nickname"))),
                    nickname=_dy_first_text((owner.get("nickname"), env.get("nickname"))),
                    avatar=_dy_first_image(owner.get("avatar_thumb") or owner.get("avatar_large") or env.get("avatar_thumb")),
                    cover=_dy_first_image(room.get("cover") or env.get("cover")),
                    watching=(_dy_total_viewers(room) or _dy_online_viewers(room)),
                    isLive=True, platform=self.site,
                ))
            # 【P1二轮：hasMore】feed 接口不接受任何翻页参数（实测 p1-p4 返回完全
            # 相同的 20 条、extra.offset 恒 0），page 形参是装饰性的——hasMore 必须
            # 恒 False，否则前端会反复拉首页造成重复卡片；分区的真翻页在
            # _partition_rooms（len>0）。旧 len>=page_size 在 20<24 时碰巧也是 False，
            # 此处钉住语义防未来 feed 返回量变化时误开翻页。
            return {"list": rooms, "hasMore": False}
        except Exception:
            return {"list": [], "hasMore": False}

    async def search_rooms(self, keyword: str, page: int, page_size: int) -> dict[str, Any]:
        self.cookie = await self._cookie()
        # 【P1修复】关键词 URL 编码后再进 Referer（中文/空格/#/? 会让 Referer 失效，
        # pure_live 原版行为；同时避免参数注入到路径）
        headers = self._headers(f"https://www.douyin.com/search/{quote(keyword, safe='')}?source=switch_tab&type=live")
        count = min(max(page_size, 1), 50)
        offset = (page - 1) * count
        common = {"device_platform":"webapp","aid":"6383","channel":"channel_pc_web","search_channel":"aweme_live","keyword":keyword,"offset":str(offset),"count":str(count),"os_version":"10"}
        attempts = [
            ("https://www.douyin.com/aweme/v1/web/live/search/", {**common,"search_source":"switch_tab","query_correct_type":"1","need_filter_settings":"1","list_type":"single"}),
            ("https://www.douyin.com/aweme/v1/web/general/search/stream/", common),
        ]
        for base, params in attempts:
            try:
                signed = await _signed_url(base, params, self.cookie)
                d = await self.http.get_json(signed, headers=headers)
                if str(d.get("status_code", "0")) != "0": continue
                raw = d.get("data", [])
                rooms=[]
                for entry in raw if isinstance(raw, list) else []:
                    if not isinstance(entry, dict): continue
                    live = entry.get("lives") or entry.get("live") or entry.get("live_info") or entry.get("data") or entry
                    if isinstance(live, str):
                        try: live=json.loads(live)
                        except Exception: continue
                    item = _room(live if isinstance(live,dict) else {}, {"nickname":entry.get("nickname"),"title":entry.get("title",entry.get("desc")),"tag":entry.get("search_keyword")})
                    if item: rooms.append(item)
                if rooms: return {"list":rooms,"hasMore":len(rooms)>0}
            except Exception:
                log.debug("Douyin search endpoint failed", exc_info=True)
        return {"list":[],"hasMore":False}

    async def get_room_detail(self, room_id: str) -> dict[str, Any] | None:
        """照 main.py parse_douyin 走 streamget 的 DouyinLiveStream（enter 签名在库内、
        自带可用 ttwid），room_data['anchor_name']=主播名、room['title']=直播间标题。
        之前自拼 enter 签名因 cookie/签名绑定问题被静默拒绝，详情退化成只有主播名的 parse。"""
        if DouyinLiveStream is None:
            return None
        try:
            self.cookie = await self._cookie()
            try:
                live = DouyinLiveStream(cookie=self.cookie) if self.cookie else DouyinLiveStream()
            except TypeError:
                live = DouyinLiveStream()
            data = await live.fetch_web_stream_data(f"https://live.douyin.com/{room_id}", process_data=True)
            if not data:
                return None
            owner = data.get("owner") or {}
            avatar_thumb = owner.get("avatar_thumb") or owner.get("avatarThumb") or {}
            avatar_urls = avatar_thumb.get("url_list") or avatar_thumb.get("urlList") or []
            cover_obj = data.get("cover") or {}
            cover_urls = cover_obj.get("url_list") or cover_obj.get("urlList") or []
            live_url = as_str(data.get("live_url"))
            web_rid = as_str(owner.get("web_rid") or owner.get("webRid")) or (live_url.rsplit("/", 1)[-1] if live_url else "") or room_id
            # 人气照 pure_live douyin_audience：累计优先、并发兜底（匿名 enter 常为占位 0，由前端隐藏）
            watching = _dy_total_viewers(data) or _dy_online_viewers(data)
            return room_dict(roomId=web_rid, title=as_str(data.get("title")), nickname=as_str(data.get("anchor_name")),
                             avatar=avatar_urls[0] if avatar_urls else "", cover=cover_urls[0] if cover_urls else "",
                             watching=watching, isLive=str(data.get("status")) == "2", platform=self.site)
        except Exception:
            return None
    async def close(self) -> None:
        await self.http.close()

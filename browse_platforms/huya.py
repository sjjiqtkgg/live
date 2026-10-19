"""Huya browse adapter. Endpoints and field names ported from pure_live huya_site.dart.
Source project: https://github.com/liuchuancong/pure_live (AGPL-3.0).
"""
from __future__ import annotations
import asyncio
import json
import logging
import re
from typing import Any
from browse_platforms.base import LivePlatform, room_dict
from browse_platforms.common import Http, as_str, strip_tags

UA = "Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.91 Mobile Safari/537.36 Edg/117.0.0.0"
# 照 pure_live HuyaRequestParams.hysdkUa：普通浏览器 UA 现在只能拿到 JS 壳页面，
# HYSDK UA 才会返回含 TT_ROOM_DATA / stream: 数据块的完整房间页
HYSDK_UA = "HYSDK(Windows,30000002)_APP(pc_exe&7100004&official)_SDK(trans&2.40.0.6448)"
_HUYA_CATEGORY_GROUPS = (("1", "网游"), ("2", "单机"), ("8", "娱乐"), ("3", "手游"))
_HUYA_CATEGORY_URL = "https://live.cdn.huya.com/liveconfig/game/bussLive"
log = logging.getLogger("live.browse")


class HuyaCategories(list[dict[str, Any]]):
    """列表兼容的虎牙分类结果，附带抓取完整性元数据。"""

    def __init__(self, values=(), *, complete: bool = True, failed_groups=()):
        super().__init__(values)
        self.complete = complete
        self.failed_groups = tuple(str(group) for group in failed_groups)


class HuyaAdapter(LivePlatform):
    site = "huya"
    name = "虎牙直播"
    logo = "https://www.huya.com/favicon.ico"
    def __init__(self, cookie: str = "") -> None:
        self.http = Http(extra_headers={"user-agent": UA, "referer": "https://www.huya.com/", "origin": "https://www.huya.com", **({"cookie": cookie} if cookie else {})})

    async def get_categories(self) -> list[dict[str, Any]]:
        # 【2026-10-19】四个顶级分组必须全部抓到，单组失败最多重试一次；
        # 返回仍保持 list 形状，完整性由 HuyaCategories 元数据 + browse_api 校验。
        cats = HuyaCategories()
        failed_groups = []
        for raw_id, title in _HUYA_CATEGORY_GROUPS:
            data = None
            last_error: Exception | None = None
            for attempt in range(2):
                try:
                    data = await self.http.get_json(
                        _HUYA_CATEGORY_URL,
                        params={"bussType": raw_id},
                        headers={"user-agent": UA},
                    )
                    if not isinstance(data, dict) or not isinstance(data.get("data"), list):
                        raise ValueError("分类响应缺少 data 数组")
                    break
                except Exception as exc:
                    last_error = exc
                    if attempt == 0:
                        log.warning("huya get_categories 分组 %s 首次请求失败，准备重试 [%s]: %s", raw_id, type(exc).__name__, exc)
            if not isinstance(data, dict) or not isinstance(data.get("data"), list):
                failed_groups.append(raw_id)
                log.warning("huya get_categories 分组 %s 重试后仍失败 [%s]: %s", raw_id, type(last_error).__name__ if last_error else "InvalidResponse", last_error or "invalid response")
                continue

            children = []
            for item in data["data"]:
                if not isinstance(item, dict):
                    continue
                raw_gid = item.get("gid", "")
                if isinstance(raw_gid, dict):
                    gid = str(raw_gid.get("value", "")).split(",")[0]
                elif isinstance(raw_gid, float):
                    gid = str(int(raw_gid))
                else:
                    gid = str(raw_gid)
                name = as_str(item.get("gameFullName", item.get("gameName", "")))
                if gid and name:
                    children.append({"id": gid, "name": name, "pic": f"https://huyaimg.msstatic.com/cdnimage/game/{gid}-MS.jpg", "parentId": raw_id})
            cats.append({"id": raw_id, "name": title, "pic": "", "children": children})

        cats.complete = not failed_groups
        cats.failed_groups = tuple(failed_groups)
        return cats

    async def get_category_rooms(self, parent_area_id: str, area_id: str, page: int, page_size: int) -> dict[str, Any]:
        d = await self.http.get_json("https://www.huya.com/cache.php", params={"m": "LiveList", "do": "getLiveListByPage", "tagAll": 0, "gameId": area_id, "page": page}, headers={"user-agent": UA})
        rows = d.get("data", {}).get("datas", []) if isinstance(d, dict) else []
        rooms = []
        for x in rows if isinstance(rows, list) else []:
            if not isinstance(x, dict): continue
            cover = as_str(x.get("screenshot", ""))
            if cover and "?" not in cover: cover += "?x-oss-process=style/w338_h190&"
            rooms.append(room_dict(roomId=as_str(x.get("profileRoom", "")), title=as_str(x.get("introduction") or x.get("roomName")), cover=cover, nickname=as_str(x.get("nick")), watching=as_str(x.get("totalCount")), avatar=as_str(x.get("avatar180")), area=as_str(x.get("gameFullName")), isLive=True, platform=self.site))
        # 【P1二轮：hasMore】cache.php 自带 data.totalPage（实测 544 房 5 页：
        # p5 为 73 条收尾页、p6 空）→ page < totalPage 精确终止；
        # totalPage 缺失/为 0 时退回本页非空。
        data = d.get("data") if isinstance(d, dict) else {}
        try:
            total_page = int((data or {}).get("totalPage") or 0)
        except (TypeError, ValueError):
            total_page = 0
        has_more = (page < total_page) if total_page > 0 else len(rooms) > 0
        return {"list": rooms, "hasMore": has_more}

    async def get_recommend_rooms(self, page: int, page_size: int) -> dict[str, Any]:
        d = await self.http.get_json("https://www.huya.com/cache.php", params={"m": "LiveList", "do": "getLiveListByPage", "tagAll": 0, "page": page}, headers={"user-agent": UA, "Origin": "https://www.huya.com", "Referer": "https://www.huya.com/"})
        rows = d.get("data", {}).get("datas", []) if isinstance(d, dict) else []
        rooms = []
        for x in rows if isinstance(rows, list) else []:
            if not isinstance(x, dict): continue
            cover = as_str(x.get("screenshot", ""))
            if cover and "?" not in cover: cover += "?x-oss-process=style/w338_h190&"
            rooms.append(room_dict(roomId=as_str(x.get("profileRoom", "")), title=as_str(x.get("introduction") or x.get("roomName")), cover=cover, nickname=as_str(x.get("nick")), watching=as_str(x.get("totalCount")), avatar=as_str(x.get("avatar180")), area=as_str(x.get("gameFullName")), isLive=True, platform=self.site))
        # 【P1二轮：hasMore】同上 totalPage（推荐流实测 72 页：p72 为 18 条收尾、
        # p73 空）；pageSize 参数被服务端忽略（固定 120/页）。
        data = d.get("data") if isinstance(d, dict) else {}
        try:
            total_page = int((data or {}).get("totalPage") or 0)
        except (TypeError, ValueError):
            total_page = 0
        has_more = (page < total_page) if total_page > 0 else len(rooms) > 0
        return {"list": rooms, "hasMore": has_more}

    async def search_rooms(self, keyword: str, page: int, page_size: int) -> dict[str, Any]:
        params = {"m": "Search", "do": "getSearchContent", "q": keyword, "uid": "0", "v": "4", "typ": "-5", "livestate": "0", "rows": str(page_size), "start": str((page - 1) * page_size)}
        d = await self.http.get_json("https://search.cdn.huya.com/", params=params, headers={"user-agent": UA})
        # 【P1二轮：hasMore】Solr 响应自带 response.3.numFound（实测上限 100）
        # → 已看条数（start+len）< numFound 即还有；numFound 缺失时退回本页非空。
        # 注：实测该上游 start 之后的页与首页重叠严重（p2 的 40 条里 23 条与 p1 重复），
        # 重复内容是上游行为；numFound 至少保证翻页最终能停（旧 len>=page_size 会无限翻）。
        r3 = ((d.get("response") or {}).get("3") or {}) if isinstance(d, dict) else {}
        rows = r3.get("docs", []) or []
        try:
            num_found = int(r3.get("numFound") or 0)
        except (TypeError, ValueError):
            num_found = 0
        rooms = []
        for x in rows if isinstance(rows, list) else []:
            if not isinstance(x, dict): continue
            rooms.append(room_dict(roomId=as_str(x.get("room_id", x.get("roomroomid", ""))), title=strip_tags(x.get("game_roomName", x.get("introduction", ""))), nickname=strip_tags(x.get("game_nick", x.get("nick", ""))), avatar=as_str(x.get("game_avatarUrl180", x.get("avatar180", ""))), cover=as_str(x.get("game_screenshot", "")), area=as_str(x.get("game_name", "")), watching=as_str(x.get("game_totalCount", "")), isLive=True, platform=self.site))
        has_more = ((page - 1) * page_size + len(rooms) < num_found) if num_found > 0 else len(rooms) > 0
        return {"list": rooms, "hasMore": has_more}

    async def get_room_detail(self, room_id: str) -> dict[str, Any] | None:
        """照 pure_live huya_site._loadRoomDetail：HYSDK UA 取房间页，
        从 TT_ROOM_DATA（state）+ stream: 数据块的 gameLiveInfo 取
        introduction（直播间标题）/nick/avatar180/screenshot/totalCount。
        旧实现只在页面里找 TT_ROOM_DATA，页面改版后拿不到就退回只有主播名的 parse。"""
        try:
            text = await self.http.get_text(f"https://www.huya.com/{room_id}", headers={"user-agent": HYSDK_UA, "referer": "https://www.huya.com/", "origin": "https://www.huya.com"})
            room_m = re.search(r"var\s+TT_ROOM_DATA\s*=\s*(\{[\s\S]*?\})", text)
            stream_m = re.search(r"stream:\s*(\{[\s\S]*?\n\s*\})", text)
            game: dict[str, Any] = {}
            state = ""
            is_replay = False
            if stream_m:
                # 照 pure_live：数据块首行才是完整 JSON（尾部带外层包装的 }，用 split 清掉）
                stream_json = json.loads(stream_m.group(0).replace("stream: ", "", 1).split("\n")[0])
                items = stream_json.get("data") or []
                first = items[0] if items and isinstance(items[0], dict) else {}
                game = first.get("gameLiveInfo") or {}
            if room_m:
                room_json = json.loads(room_m.group(1))
                state = str(room_json.get("state", ""))
                is_replay = room_json.get("isReplay") is True
            if not game:
                return None
            # 照 pure_live：state=ON 且非回放才算开播
            live = state.upper() == "ON" and not is_replay
            return room_dict(roomId=room_id, title=as_str(game.get("introduction")), nickname=as_str(game.get("nick")), avatar=as_str(game.get("avatar180")), cover=as_str(game.get("screenshot")), area=as_str(game.get("gameFullName")), watching=as_str(game.get("totalCount")), isLive=live, platform=self.site)
        except Exception:
            return None

    async def get_live_status(self, room_ids: list[str]) -> list[dict[str, Any]]:
        # 【L4·审计修复】原逐房间串行：30 个房间最坏等于 30 倍单房延迟（可达数分钟）。
        # 改信号量限流的并发（5 路），单项异常隔离，返回顺序与入参一致。
        sem = asyncio.Semaphore(5)

        async def _one(rid: str):
            async with sem:
                try:
                    return rid, await self.get_room_detail(rid)
                except Exception:
                    return rid, None

        pairs = await asyncio.gather(*(_one(r) for r in room_ids))
        out = []
        for rid, d in pairs:
            if d:
                out.append({"roomId": rid, "isLive": d["isLive"], "title": d["title"], "watching": d["watching"]})
        return out
    async def close(self) -> None:
        await self.http.close()

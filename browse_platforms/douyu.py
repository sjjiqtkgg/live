"""Douyu browse adapter. Endpoint/field mappings ported from pure_live douyu_site.dart.
Source project: https://github.com/liuchuancong/pure_live (AGPL-3.0).
"""
from __future__ import annotations
import asyncio
from typing import Any
from browse_platforms.base import LivePlatform, room_dict
from browse_platforms.common import Http, as_str, strip_tags

class DouyuAdapter(LivePlatform):
    site = "douyu"
    name = "斗鱼直播"
    logo = "https://www.douyu.com/favicon.ico"
    def __init__(self, cookie: str = "") -> None:
        self.http = Http(extra_headers={"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36", "referer": "https://www.douyu.com/", "origin": "https://www.douyu.com", **({"cookie": cookie} if cookie else {})})

    async def get_categories(self) -> list[dict[str, Any]]:
        d = await self.http.get_json("https://m.douyu.com/api/cate/list")
        data = d.get("data", {}) if isinstance(d, dict) else {}
        subs = data.get("cate2Info", []) if isinstance(data, dict) else []
        cats = []
        for item in data.get("cate1Info", []) if isinstance(data, dict) else []:
            cid = as_str(item.get("cate1Id", ""))
            children = []
            for sub in subs if isinstance(subs, list) else []:
                if str(sub.get("cate1Id", "")) == cid:
                    children.append({"id": as_str(sub.get("cate2Id", "")), "name": as_str(sub.get("cate2Name", "")), "pic": as_str(sub.get("pic", sub.get("icon", ""))), "parentId": cid})
            cats.append({"id": cid, "name": as_str(item.get("cate1Name", "")), "pic": "", "children": children})
        return cats

    async def get_category_rooms(self, parent_area_id: str, area_id: str, page: int, page_size: int) -> dict[str, Any]:
        d = await self.http.get_json(f"https://www.douyu.com/gapi/rkc/directory/mixList/2_{area_id}/{page}", params={})
        rows = d.get("data", {}).get("rl", []) if isinstance(d, dict) else []
        rooms = [self._room(x) for x in rows if isinstance(x, dict) and str(x.get("type")) == "1"]
        # 【P1二轮：hasMore】mixList 自带 data.pgcnt 总页数（服务端固定 120/页，实测
        # pgcnt=6 时 p6 为 41 条收尾页、p7 空）→ page < pgcnt 精确终止；
        # pgcnt 缺失/为 0 时退回本页非空（fetch-until-empty，末页拉空自然停）。
        data = d.get("data") if isinstance(d, dict) else {}
        try:
            pgcnt = int((data or {}).get("pgcnt") or 0)
        except (TypeError, ValueError):
            pgcnt = 0
        has_more = (page < pgcnt) if pgcnt > 0 else len(rooms) > 0
        return {"list": rooms, "hasMore": has_more}

    async def get_recommend_rooms(self, page: int, page_size: int) -> dict[str, Any]:
        d = await self.http.get_json(f"https://www.douyu.com/japi/weblist/apinc/allpage/6/{page}", params={})
        rows = d.get("data", {}).get("rl", []) if isinstance(d, dict) else []
        rooms = [self._room(x) for x in rows if isinstance(x, dict) and str(x.get("type")) == "1"]
        # 【P1二轮：hasMore】allpage 接口的 pgcnt 恒 0（无可用元数据，实测 40/页、
        # page=999 才空）→ 本页非空即还有下一页，末页之后拉空自然停。
        data = d.get("data") if isinstance(d, dict) else {}
        try:
            pgcnt = int((data or {}).get("pgcnt") or 0)
        except (TypeError, ValueError):
            pgcnt = 0
        has_more = (page < pgcnt) if pgcnt > 0 else len(rooms) > 0
        return {"list": rooms, "hasMore": has_more}

    async def search_rooms(self, keyword: str, page: int, page_size: int) -> dict[str, Any]:
        d = await self.http.get_json("https://www.douyu.com/japi/search/api/searchShow", params={"kw": keyword, "page": page, "pageSize": page_size}, headers={"referer": "https://www.douyu.com/search/"})
        if isinstance(d, dict) and str(d.get("error")) not in ("0", "None"):
            return {"list": [], "hasMore": False}
        rows = d.get("data", {}).get("relateShow", []) if isinstance(d, dict) else []
        rooms=[]
        for x in rows if isinstance(rows, list) else []:
            if not isinstance(x, dict): continue
            room = self._room({"rid":x.get("rid"),"rs16":x.get("rs16"),"ol":x.get("ol"),"rn":x.get("rn"),"nn":x.get("nn"),"av":x.get("av"),"c2name":x.get("c2name")})
            room["title"] = strip_tags(room["title"])
            rooms.append(room)
        # 【P1二轮：hasMore】searchShow 无分页元数据（沙箱实测 error=9 限频；正常
        # 部署下 relateShow 亦无 total 字段）→ 本页非空即还有下一页，末页拉空自然停。
        return {"list": rooms, "hasMore": len(rooms) > 0}

    def _room(self, x: dict[str, Any]) -> dict[str, Any]:
        av = as_str(x.get("av", "")).strip()
        # 【头像 404 修复】斗鱼部分接口的 av 已经是完整 URL（https://apic.douyucdn.cn/
        # upload/..._middle.jpg），旧实现无条件再拼一次 /upload/ 和 _middle.jpg，生成
        # “upload/https://...” 的双重前缀，实测 404 → 关注/房间列表头像裂图。
        if av.startswith("http://") or av.startswith("https://"):
            avatar = av
        elif av.startswith("//"):
            avatar = "https:" + av
        elif av:
            avatar = f"https://apic.douyucdn.cn/upload/{av}"
            if not avatar.endswith("_middle.jpg"):
                avatar += "_middle.jpg"
        else:
            avatar = ""
        return room_dict(roomId=as_str(x.get("rid", "")), title=as_str(x.get("rn", "")), nickname=as_str(x.get("nn", "")), avatar=avatar, cover=as_str(x.get("rs16", "")), area=as_str(x.get("c2name", "")), watching=as_str(x.get("ol", "")), isLive=True, platform=self.site)

    async def get_room_detail(self, room_id: str) -> dict[str, Any] | None:
        d = await self.http.get_json(f"https://www.douyu.com/betard/{room_id}", params={}, headers={"referer": f"https://www.douyu.com/{room_id}", "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"})
        room = d.get("room", {}) if isinstance(d, dict) else {}
        if not room: return None
        biz=room.get("room_biz_all", {}) or {}
        live=str(room.get("show_status", "")) == "1" and str(room.get("videoLoop", "")) != "1"
        return room_dict(roomId=as_str(room.get("room_id", room_id)), title=as_str(room.get("room_name")), nickname=as_str(room.get("owner_name")), avatar=as_str(room.get("owner_avatar")), cover=as_str(room.get("room_pic")), area=as_str(room.get("second_lvl_name")), watching=as_str(biz.get("hot", "")), isLive=live, platform=self.site)

    async def get_live_status(self, room_ids: list[str]) -> list[dict[str, Any]]:
        # 【L4·审计修复】逐房间串行 → 信号量限流并发（5 路），单项异常隔离，顺序保持
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

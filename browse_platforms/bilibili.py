"""B站直播适配器：浏览接口完整实现（分区/分区房间/推荐/搜索/详情/状态）。

接口参考 pure_live 的 lib/shared/platforms/bilibili/bilibili_site.dart：
  - 分区:     /room/v1/Area/getList                    (无需签名)
  - 分区房间: /room/v1/Area/getListByAreaID            (纯_live 热门榜单用的端点, 无需签名)
  - 推荐:     /xlive/web-interface/v1/webMain/getMoreRecList
  - 搜索:     /x/web-interface/search/type?search_type=live
  - 详情:     /xlive/web-room/v1/index/getInfoByRoom

注：B站部分列表接口(zwf2/second/getList)需要 WBI 签名；若日后被风控拦下，
需从 bilibili-API-collect 抄权威的 64 位 mixinKeyEncTab 常量并移植 _get_wbi_keys/_wbi_sign。
当前用无需签名的端点保证开箱即用。
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from browse_platforms.base import LivePlatform, room_dict
from browse_platforms.common import strip_tags

log = logging.getLogger("live.bilibili")


def _live_status(raw: Any, default: bool) -> bool:
    """B站 live_status 统一语义：0=未开播，1=直播中，2=轮播（与 main.py parse_bilibili
    的 not in (1, 2) 同一约定——1/2 都有流可播）。字段缺失/非法取 default
    （列表接口按在播、搜索按未播，维持各自旧默认倾向）。
    【P1：or 1 修复】旧实现 int(item.get("live_status", 1) or 1)：0 是 falsy，
    `0 or 1` → 1 → 未开播房间被标成开播；且 "0"/None/"" 等字符串形态会直接
    int() 抛异常。此 helper 对各形态安全解析且不吞 0。"""
    try:
        return int(raw) != 0
    except (TypeError, ValueError):
        return default

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
REFERER = "https://live.bilibili.com/"
API = "https://api.live.bilibili.com"


class BilibiliAdapter(LivePlatform):
    site = "bilibili"
    name = "哔哩哔哩直播"
    logo = "https://static.hdslb.com/images/favicon.ico"

    def __init__(self, cookie: str = "") -> None:
        self.cookie = cookie
        self._client: httpx.AsyncClient | None = None

    async def _headers(self) -> dict[str, str]:
        h = {"user-agent": UA, "referer": REFERER}
        if self.cookie:
            h["cookie"] = self.cookie
        return h

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=15, http2=True)
        return self._client

    # ---- 浏览接口 ----
    async def get_categories(self) -> list[dict[str, Any]]:
        # 【2026-10-16】分区缓存上移至 browse_api._browse_cached（统一 24h + stale-if-error）
        try:
            r = await (await self._get_client()).get(
                API + "/room/v1/Area/getList",
                params={"need_entrance": 1, "parent_id": 0},
                headers=await self._headers(),
            )
            cats: list[dict[str, Any]] = []
            for item in r.json().get("data", []):
                children = []
                for sub in item.get("list", []):
                    children.append({
                        "id": str(sub.get("id", "")), "name": sub.get("name", ""),
                        "pic": (sub.get("pic", "") or "") + "@100w.png",
                        "parentId": str(item.get("id", "")),
                    })
                cats.append({"id": str(item.get("id", "")), "name": item.get("name", ""), "pic": "", "children": children})
            return cats
        except Exception as e:
            log.warning(f"B站 分区失败: {e}")
            return []

    async def get_category_rooms(self, parent_area_id: str, area_id: str, page: int, page_size: int) -> dict[str, Any]:
        try:
            r = await (await self._get_client()).get(
                API + "/room/v1/Area/getListByAreaID",
                params={"areaId": area_id, "parent_area_id": parent_area_id, "sort": "online", "page": page, "pageSize": page_size},
                headers=await self._headers(),
            )
            data = r.json().get("data", {})
            # 【P1二轮：分区形状修正】实测（2025-10）该接口的 data 就是房间列表本身，
            # 不是旧代码假设的 dict.recommend_room_list——旧写法对真实响应必然抛
            # 'list' object has no attribute 'get' 被 except 吞掉，分区房间一直返回空。
            # 两代形状都兼容；响应无任何分页元数据、page=999 返回空列表（pageSize 被接受）
            # → hasMore 用 fetch-until-empty（本页非空即还有下一页，末页之后拉空自然停）。
            raw = data.get("recommend_room_list", []) if isinstance(data, dict) else data
            rooms = self._parse_list(raw)
            return {"list": rooms, "hasMore": len(rooms) > 0}
        except Exception as e:
            log.warning(f"B站 分区房间失败: {e}")
            return {"list": [], "hasMore": False}

    async def get_recommend_rooms(self, page: int, page_size: int) -> dict[str, Any]:
        # 【P1：hasMore】getMoreRecList 是服务端固定条数的无尽推荐流（实测每页 12 条，
        # page=999 仍返回 12 条，响应无任何分页元数据、也不接受 pageSize 参数）。
        # 旧判定 len(rooms) >= page_size（前端恒用默认 24）→ 12 >= 24 恒 False →
        # 首页推荐翻页在第 1 页就死。改为「本页有数据即认为还有下一页」（fetch-until-
        # empty），最后一页之后拉到空页自然停——对该接口语义精确。
        try:
            r = await (await self._get_client()).get(
                API + "/xlive/web-interface/v1/webMain/getMoreRecList",
                params={"platform": "web", "page": page},
                headers=await self._headers(),
            )
            data = r.json().get("data", {})
            raw = data.get("recommend_room_list", []) if isinstance(data, dict) else data
            rooms = self._parse_list(raw)
            return {"list": rooms, "hasMore": len(rooms) > 0}
        except Exception as e:
            log.warning(f"B站 推荐失败: {e}")
            return {"list": [], "hasMore": False}

    async def search_rooms(self, keyword: str, page: int, page_size: int) -> dict[str, Any]:
        try:
            r = await (await self._get_client()).get(
                "https://api.bilibili.com/x/web-interface/search/type",
                params={"search_type": "live", "keyword": keyword, "page": page, "page_size": page_size},
                headers=await self._headers(),
            )
            raw = r.json().get("data", {}).get("result", {}).get("live_room", []) or []
            rooms = []
            for item in raw:
                title = strip_tags(item.get("title", ""))
                rooms.append(room_dict(
                    roomId=str(item.get("roomid", "")), title=title, nickname=item.get("uname", ""),
                    avatar="https:" + str(item.get("uface", "")),
                    cover="https:" + str(item.get("cover", "")) + "@400w.jpg",
                    area=item.get("cate_name", ""), watching=item.get("online", ""),
                    isLive=_live_status(item.get("live_status"), default=False), platform=self.site,
                ))
            # 【P1二轮：hasMore】搜索接口有分页元数据：data.pageinfo.live_room.numPages
            # （实测 986 结果 42 页）→ page < numPages；元数据缺失时退回本页非空。
            info = ((r.json().get("data") or {}).get("pageinfo") or {}).get("live_room", {}) or {}
            try:
                pages = int(info.get("numPages") or info.get("pages") or 0)
            except (TypeError, ValueError):
                pages = 0
            has_more = (page < pages) if pages > 0 else len(rooms) > 0
            return {"list": rooms, "hasMore": has_more}
        except Exception as e:
            log.warning(f"B站 搜索失败: {e}")
            return {"list": [], "hasMore": False}

    def _parse_list(self, raw: Any) -> list[dict[str, Any]]:
        if not isinstance(raw, list):
            return []
        rooms = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            rooms.append(room_dict(
                roomId=str(item.get("roomid", item.get("room_id", ""))),
                title=strip_tags(item.get("title", "")),
                nickname=strip_tags(item.get("uname", "")),
                avatar=item.get("face", ""),
                cover=(item.get("cover", "") or "") + "@400w.jpg",
                # 分区接口（data 为裸列表）用 areaName/area_v2_name，推荐/搜索用 area_name
                area=item.get("area_name") or item.get("areaName") or item.get("area_v2_name", ""),
                watching=item.get("online", ""), isLive=_live_status(item.get("live_status"), default=True),
                platform=self.site,
            ))
        return rooms

    async def get_room_detail(self, room_id: str) -> dict[str, Any] | None:
        try:
            r = await (await self._get_client()).get(
                "https://api.live.bilibili.com/xlive/web-room/v1/index/getInfoByRoom",
                params={"room_id": room_id}, headers=await self._headers(),
            )
            d = r.json().get("data", {})
            room_info = d.get("room_info", {})
            if not room_info:
                # 风控(-352)或房间不存在时 data.room_info 为空，此时返回的元信息全是默认值，
                # 交由上游的解析接口兜底（它有正确的标题/开播状态）。
                return None
            base = d.get("anchor_info", {}).get("base_info", {})
            return room_dict(
                roomId=str(room_info.get("room_id", room_id)), title=room_info.get("title", ""),
                nickname=base.get("uname", ""), avatar=base.get("face", ""), cover=room_info.get("cover", ""),
                area=room_info.get("area_name", ""), watching=room_info.get("online", ""),
                # 与列表接口保持一致：live_status=2 是轮播，同样可播放，
                isLive=int(room_info.get("live_status", 0)) in (1, 2), platform=self.site,
            )
        except Exception as e:
            log.warning(f"B站 详情失败: {e}")
            return None

    async def get_live_status(self, room_ids: list[str]) -> list[dict[str, Any]]:
        # 【L4·审计修复】逐房间串行 → 信号量限流并发（5 路），单项异常隔离，顺序保持
        sem = asyncio.Semaphore(5)

        async def _one(rid: str):
            async with sem:
                try:
                    return rid, await self.get_room_detail(rid)
                except Exception:
                    return rid, None

        pairs = await asyncio.gather(*(_one(r) for r in room_ids[:30]))
        out = []
        for rid, d in pairs:
            if d:
                # 【P1二轮：关注状态匹配】get_room_detail 回的是 room_info.room_id
                # （真实长号）；关注列表存的可能是短号（URL 里就是短号），拿长号回给
                # 前端会对不上 → 回显请求时传入的 id，调用方拿什么进来就回什么。
                out.append({"roomId": rid, "isLive": d["isLive"], "title": d["title"], "watching": d["watching"]})
        return out

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
"""SOOP 直播适配器：浏览接口（分区/分区房间/推荐/搜索）。

接口照搬 pure_live soop_site.dart（普通 REST，可移植）：
  - 分区:   GET https://sch.sooplive.co.kr/api.php?m=categoryList&szPlatform=pc
  - 分区房间: GET https://sch.sooplive.co.kr/api.php?m=categoryContentsList&szCateNo={id}
  - 推荐:   GET https://live.sooplive.co.kr/api/main_broad_list_api.php (selectType=action)
  - 搜索:   GET https://sch.sooplive.co.kr/api.php (万能关键字查询)

分区结构与 pure_live 一致：单父级「热门」，children 递归把 categoryList 按
120/页拉完（getAllSubCategores），前端按扁平网格渲染。
"""
from __future__ import annotations
import re

from html import unescape
from typing import Any

from browse_platforms.base import LivePlatform, room_dict
from browse_platforms.common import Http, as_str, strip_tags
from browse_cache import LIST_CACHE

SCH = "https://sch.sooplive.co.kr/api.php"


def _display(text: Any) -> str:
    """照 pure_live soop_site._display：标题/昵称常带 HTML 实体（&amp; 等）"""
    return unescape(strip_tags(text))


def _soop_online_viewers(ch: dict) -> str:
    """照 pure_live soop_site.parseOnlineViewers：SOOP 把观众数拆在 PC/移动两个字段里。"""
    if not isinstance(ch, dict):
        return ""
    explicit_zero = ""

    def positive(v: Any) -> str:
        text = str(v).strip() if v is not None else ""
        if not text or text == "null" or not re.search(r"[0-9]", text):
            return ""
        try:
            parsed = int(text.replace(",", "").replace("，", ""))
        except ValueError:
            return ""
        return str(parsed) if parsed > 0 else ""

    for v in (ch.get("total_view_cnt"), ch.get("view_cnt"), ch.get("VIEW_CNT")):
        parsed = positive(v)
        if parsed:
            return parsed
    pairs = ((ch.get("pc_view_cnt"), ch.get("mobile_view_cnt")), (ch.get("current_view_cnt"), ch.get("m_current_view_cnt")))
    for left, right in pairs:
        lt, rt = positive(left), positive(right)
        if not lt and not rt:
            continue
        total = int(lt or 0) + int(rt or 0)
        if total > 0:
            return str(total)
    return explicit_zero


class SoopAdapter(LivePlatform):
    site = "soop"
    name = "SOOP直播"
    logo = "https://play.sooplive.com/favicon.ico"

    def __init__(self, cookie: str = "") -> None:
        self.cookie = cookie or ""
        self.http = Http()

    def _headers(self) -> dict[str, str]:
        h = {
            "accept": "*/*",
            "origin": "https://www.sooplive.co.kr",
            "referer": "https://www.sooplive.co.kr/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        }
        if self.cookie:
            h["cookie"] = self.cookie
        return h

    async def get_categories(self) -> list[dict[str, Any]]:
        """照 pure_live soop_site.dart getCategores：单父级「热门」，
        children 递归把 categoryList 按页拉完。"""
        cached = LIST_CACHE.get("soop_cats")
        if cached is not None:
            return cached
        try:
            categories: list[dict[str, Any]] = [{"id": "1", "name": "热门", "pic": "", "children": []}]
            categories[0]["children"] = await self._get_all_sub_categories("1", 1, 120, [])
            LIST_CACHE.set("soop_cats", categories, 600)
            return categories
        except Exception:
            return []

    async def _get_all_sub_categories(self, parent_id: str, page: int, page_size: int, acc: list[dict[str, Any]]) -> list[dict[str, Any]]:
        # 照 pure_live getAllSubCategores 的递归（hasMore = 本页满页）；
        # 页数上限 20 只是防 API 异常时的兜底，正常目录远用不到。
        try:
            subs = await self._get_sub_categories(parent_id, page, page_size)
            acc.extend(subs)
            if len(subs) >= page_size and page < 20:
                return await self._get_all_sub_categories(parent_id, page + 1, page_size, acc)
            return acc
        except Exception:
            return acc

    async def _get_sub_categories(self, parent_id: str, page: int, page_size: int) -> list[dict[str, Any]]:
        d = await self.http.get_json(SCH, params={"m": "categoryList", "szKeyword": "", "szOrder": "view_cnt", "nPageNo": str(page), "nListCnt": str(page_size), "nOffset": "0", "szPlatform": "pc"}, headers=self._headers())
        subs = []
        for item in (d.get("data", {}).get("list", []) if isinstance(d, dict) else []):
            if not isinstance(item, dict):
                continue
            cno = as_str(item.get("category_no", ""))
            cname = as_str(item.get("category_name", ""))
            if cno and cname:
                subs.append({"id": cno, "name": cname, "pic": as_str(item.get("cate_img", "")), "parentId": parent_id})
        return subs

    async def get_category_rooms(self, parent_area_id: str, area_id: str, page: int, page_size: int) -> dict[str, Any]:
        # 照 pure_live getCategoryRooms：pageSize clamp(1, 60)
        ps = min(max(page_size, 1), 60)
        try:
            d = await self.http.get_json(SCH, params={"m": "categoryContentsList", "szType": "live", "nPageNo": str(page), "nListCnt": str(ps), "szPlatform": "pc", "szOrder": "view_cnt_desc", "szCateNo": area_id}, headers=self._headers())
            rooms = []
            for item in (d.get("data", {}).get("list", []) if isinstance(d, dict) else []):
                if not isinstance(item, dict):
                    continue
                rooms.append(room_dict(
                    roomId=as_str(item.get("user_id", "")), title=_display(item.get("broad_title", "")),
                    nickname=_display(item.get("user_nick", "")), avatar=as_str(item.get("user_profile_img", "")),
                    cover=as_str(item.get("broad_img", item.get("broad_thumb", item.get("thumbnail", "")))), area=as_str(item.get("broad_cate_name", item.get("standard_broad_cate_name", item.get("category_name", "")))),
                    watching=as_str(item.get("total_view_cnt", item.get("current_view_cnt", item.get("view_cnt", "")))),
                    isLive=bool(item.get("user_id")), platform=self.site,
                ))
            # 【P1二轮：hasMore】categoryContentsList 无分页元数据，但实测翻页真实
            # 生效且末尾自然拉空（实测某分区 24/24/2/0）→ 本页非空即还有下一页
            # （旧 2>=24 恒 False，收尾页的第 3 页两条房间会被丢弃）。
            return {"list": rooms, "hasMore": len(rooms) > 0}
        except Exception:
            return {"list": [], "hasMore": False}

    async def get_recommend_rooms(self, page: int, page_size: int) -> dict[str, Any]:
        try:
            d = await self.http.get_json("https://live.sooplive.co.kr/api/main_broad_list_api.php", params={"selectType": "action", "selectValue": "all", "orderType": "view_cnt", "pageNo": str(page), "lang": "ko_KR"}, headers=self._headers())
            rooms = []
            for item in (d.get("broad", []) if isinstance(d, dict) else []):
                if not isinstance(item, dict):
                    continue
                rooms.append(room_dict(
                    roomId=as_str(item.get("user_id", "")), title=_display(item.get("broad_title", "")),
                    nickname=_display(item.get("user_nick", "")), avatar=as_str(item.get("user_profile_img", "")),
                    cover=as_str(item.get("broad_thumb", "")), area=as_str(item.get("category_name", "")),
                    watching=as_str(item.get("total_view_cnt", item.get("current_view_cnt", ""))),
                    isLive=bool(item.get("user_id")), platform=self.site,
                ))
            # 【P1二轮：hasMore】main_broad_list_api 自带 total_cnt（实测 "3289"，
            # 服务端固定 60/页：p55 为 49 条收尾页）→ page*cnt < total 精确终止；
            # 元数据缺失时退回本页非空。
            total, cnt = 0, 0
            if isinstance(d, dict):
                try:
                    total = int(str(d.get("total_cnt") or "0") or 0)
                except (TypeError, ValueError):
                    total = 0
                try:
                    cnt = int(str(d.get("cnt") or "0") or 0)
                except (TypeError, ValueError):
                    cnt = 0
            has_more = (page * cnt < total) if total > 0 and cnt > 0 else len(rooms) > 0
            return {"list": rooms, "hasMore": has_more}
        except Exception:
            return {"list": [], "hasMore": False}

    async def search_rooms(self, keyword: str, page: int, page_size: int) -> dict[str, Any]:
        try:
            d = await self.http.get_json(SCH, params={"l": "DF", "m": "liveSearch", "c": "UTF-8", "w": "webk", "isMobile": "0", "onlyParent": "1", "szType": "json", "szOrder": "score", "szKeyword": keyword, "nPageNo": str(page), "nListCnt": str(page_size), "tab": "live", "location": "total_search", "isHashSearch": "0", "v": "2.0"}, headers=self._headers())
            rooms = []
            for item in (d.get("REAL_BROAD", []) if isinstance(d, dict) else []):
                if not isinstance(item, dict):
                    continue
                rooms.append(room_dict(
                    roomId=as_str(item.get("user_id", "")), title=_display(item.get("broad_title", "")),
                    nickname=_display(item.get("user_nick", "")), avatar=as_str(item.get("user_profile_img", "")),
                    cover=as_str(item.get("broad_img", item.get("broad_thumb", item.get("thumbnail", "")))), area=as_str(item.get("broad_cate_name", item.get("standard_broad_cate_name", item.get("category_name", "")))),
                    watching=as_str(item.get("total_view_cnt", item.get("current_view_cnt", item.get("view_cnt", "")))),
                    isLive=bool(item.get("user_id")), platform=self.site,
                ))
            # 【P1二轮：hasMore】liveSearch 自带 HAS_MORE_LIST 布尔（实测 p1-p4 均
            # true；沙箱高频请求会被 403 限频，命中时返回空页自然停）→ 直接用
            # 上游元数据；缺失时退回本页非空。
            hm = d.get("HAS_MORE_LIST") if isinstance(d, dict) else None
            if hm is not None:
                has_more = hm is True or str(hm).strip().lower() == "true"
            else:
                has_more = len(rooms) > 0
            return {"list": rooms, "hasMore": has_more}
        except Exception:
            return {"list": [], "hasMore": False}

    async def get_room_detail(self, room_id: str) -> dict[str, Any] | None:
        """照 pure_live soop_site.getPlayerLiveApiData/getLiveRoomByApi：
        player_live_api 的 CHANNEL.TITLE 是直播间标题，BJNICK 是主播名，
        VIEWPRESET 存在即开播；RESULT 非 1（未开播/封禁）回退 parse。"""
        try:
            h = {"accept": "*/*", "origin": "https://www.sooplive.co.kr", "referer": f"https://play.sooplive.co.kr/{room_id}",
                 "user-agent": self._headers().get("user-agent", "Mozilla/5.0")}
            if self.cookie:
                h["cookie"] = self.cookie
            d = await self.http.post_json("https://live.sooplive.co.kr/afreeca/player_live_api.php",
                data={"bid": room_id, "bno": "", "type": "live", "pwd": "", "player_type": "html5",
                      "stream_type": "common", "quality": "HD", "mode": "landing", "from_api": "0", "is_revive": "false"},
                headers=h)
            ch = d.get("CHANNEL") if isinstance(d, dict) else None
            if not isinstance(ch, dict) or str(ch.get("RESULT")) != "1":
                return None
            bno = as_str(ch.get("BNO"))
            bj = as_str(ch.get("BJID", room_id)) or room_id
            avatar = f"https://stimg.sooplive.co.kr/LOGO/{bj[:2]}/{bj}/{bj}.jpg" if len(bj) >= 2 else ""
            watching = _soop_online_viewers(ch)
            if not watching:
                # 详情接口(player_live_api)不返回观众数；用搜索接口按房间号补查。
                # pure_live 的等价做法是纯前端回退（withAudienceFallbackFrom：沿用
                # 列表页带过来的人数），后端补查对深链接打开也同样有效。
                watching = await self._viewers_via_search(bj)
            return room_dict(roomId=bj, title=as_str(ch.get("TITLE")), nickname=as_str(ch.get("BJNICK")),
                             avatar=avatar, cover=f"https://liveimg.sooplive.co.kr/m/{bno}" if bno else "",
                             watching=watching, isLive=ch.get("VIEWPRESET") is not None, platform=self.site)
        except Exception:
            return None

    async def _viewers_via_search(self, room_id: str) -> str:
        """liveSearch 按房间号查询本房间，命中则用列表口径解析观众数
        （total_view_cnt / pc+mobile / current+m_current）。"""
        try:
            d = await self.http.get_json(SCH, params={
                "l": "DF", "m": "liveSearch", "c": "UTF-8", "w": "webk", "isMobile": "0",
                "onlyParent": "1", "szType": "json", "szOrder": "score", "szKeyword": room_id,
                "nPageNo": "1", "nListCnt": "5", "tab": "live", "location": "total_search",
                "isHashSearch": "0", "v": "2.0",
            }, headers=self._headers())
            for item in (d.get("REAL_BROAD", []) if isinstance(d, dict) else []):
                if not isinstance(item, dict):
                    continue
                if as_str(item.get("user_id", "")).lower() == room_id.lower():
                    watching = _soop_online_viewers(item)
                    if watching:
                        return watching
        except Exception:
            pass
        return ""

    async def close(self) -> None:
        await self.http.close()
"""平台浏览适配器基类 + 统一 Room 数据结构。"""
from __future__ import annotations

import asyncio
import logging
from functools import wraps
from typing import Any

log = logging.getLogger("live.browse")

# 【P1二轮：适配器异常统一兜底】此前各平台适配器的异常处理不一致：bilibili/
# soop/twitch 的方法自带 try/except 返回空态，斗鱼 get_room_detail、PandaTV 系
# 则裸奔——上游一抖（网络错误/JSON 形状变化/风控页）异常直接穿到路由层，整条
# 请求 500。在基类 __init_subclass__ 里给子类覆写的浏览方法统一包一层守卫：
# 异常 → 记 warning 日志 + 返回该方法约定的空态（get_room_detail → None，
# 正好落进路由层既有的「详情拿不到就回退 parse」兜底链）。
# 子类自带 try/except 的方法不受影响（不抛异常时守卫是透明转发）；
# 测试里 monkeypatch 掉的方法直接替换类属性，也不经过守卫。
_GUARDED_METHODS: dict[str, Any] = {
    "get_categories": [],
    "get_category_rooms": {"list": [], "hasMore": False},
    "get_recommend_rooms": {"list": [], "hasMore": False},
    "search_rooms": {"list": [], "hasMore": False},
    "get_room_detail": None,
    "get_live_status": [],
}


def _guarded(fn, site: str, name: str, fallback: Any):
    @wraps(fn)
    async def wrapper(self, *args, **kwargs):
        try:
            return await fn(self, *args, **kwargs)
        except Exception as e:  # noqa: BLE001 —— 浏览层兜底，任何异常都不上抛
            log.warning(
                f"{site or self.__class__.__name__} {name} 适配器异常，返回空态 "
                f"[{type(e).__name__}]: {e}"
            )
            return fallback

    return wrapper


class LivePlatform:
    """每个平台需实现浏览类接口（分类/分区房间/推荐/搜索/详情/流状态）。

    streamget 只会做「URL→流」，浏览类数据必须由各平台适配器实现。
    未实现的平台返回空态而非抛异常，前端能优雅降级；
    子类覆写的方法若抛异常，也由 __init_subclass__ 守卫统一转成空态 + 日志。
    """

    site: str = ""
    name: str = ""
    logo: str = ""

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        site = str(getattr(cls, "site", "") or cls.__name__)
        for name, fallback in _GUARDED_METHODS.items():
            fn = cls.__dict__.get(name)
            if fn is None or not asyncio.iscoroutinefunction(fn):
                continue
            setattr(cls, name, _guarded(fn, site, name, fallback))

    async def get_categories(self) -> list[dict[str, Any]]:
        """返回 [{id,name,pic,children:[{id,name,pic,parentId}]}]"""
        return []

    async def get_category_rooms(
        self, parent_area_id: str, area_id: str, page: int, page_size: int
    ) -> dict[str, Any]:
        """返回 {list:[Room], hasMore}"""
        return {"list": [], "hasMore": False}

    async def get_recommend_rooms(self, page: int, page_size: int) -> dict[str, Any]:
        return {"list": [], "hasMore": False}

    async def search_rooms(self, keyword: str, page: int, page_size: int) -> dict[str, Any]:
        return {"list": [], "hasMore": False}

    async def get_room_detail(self, room_id: str) -> dict[str, Any] | None:
        """返回 Room；未实现返回 None。"""
        return None

    async def get_live_status(self, room_ids: list[str]) -> list[dict[str, Any]]:
        """返回 [{roomId,isLive,title,watching}]"""
        return []


def room_dict(**kw: Any) -> dict[str, Any]:
    """统一 Room 结构，缺失字段补默认值，保证前端拿到一致形状。"""
    return {
        "roomId": str(kw.get("roomId", "")),
        "title": str(kw.get("title", "")),
        "nickname": str(kw.get("nickname", "")),
        "avatar": str(kw.get("avatar", "")),
        "cover": str(kw.get("cover", "")),
        "area": str(kw.get("area", "")),
        "watching": str(kw.get("watching", "")),
        "isLive": bool(kw.get("isLive", True)),
        "platform": str(kw.get("platform", "")),
    }

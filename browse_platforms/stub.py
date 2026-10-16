"""Empty browse fallback for platforms without directory endpoints."""
from __future__ import annotations
from typing import Any
from browse_platforms.base import LivePlatform

class StubPlatform(LivePlatform):
    site: str = ""
    name: str = ""
    logo: str | None = None
    async def get_categories(self) -> list[dict[str, Any]]:
        return []
    async def get_category_rooms(self, parent_area_id: str, area_id: str, page: int, page_size: int) -> dict[str, Any]:
        return {"list": [], "hasMore": False}
    async def get_recommend_rooms(self, page: int, page_size: int) -> dict[str, Any]:
        return {"list": [], "hasMore": False}
    async def search_rooms(self, keyword: str, page: int, page_size: int) -> dict[str, Any]:
        return {"list": [], "hasMore": False}
    async def get_room_detail(self, room_id: str) -> dict[str, Any] | None:
        return None
    async def get_live_status(self, room_ids: list[str]) -> list[dict[str, Any]]:
        return []
    async def close(self) -> None:
        return None

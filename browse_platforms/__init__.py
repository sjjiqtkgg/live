"""Browse adapters hosted inside the existing FastAPI backend."""
from __future__ import annotations

from browse_platforms.base import LivePlatform


def make_room_url(site: str, room_id: str) -> str:
    urls = {
        "huya": f"https://www.huya.com/{room_id}",
        "douyu": f"https://www.douyu.com/{room_id}",
        "douyin": f"https://live.douyin.com/{room_id}",
        "bilibili": f"https://live.bilibili.com/{room_id}",
        "soop": f"https://play.sooplive.com/{room_id}",
        "twitch": f"https://www.twitch.tv/{room_id}",
        "pandatv": f"https://www.pandalive.co.kr/{room_id}",
    }
    return urls.get(site, "") if room_id else ""


class _Registry:
    @staticmethod
    def _class_map() -> dict[str, type[LivePlatform]]:
        from browse_platforms import bilibili, douyin, douyu, huya, pandatv, soop, twitch

        return {
            "bilibili": bilibili.BilibiliAdapter,
            "huya": huya.HuyaAdapter,
            "douyu": douyu.DouyuAdapter,
            "douyin": douyin.DouyinAdapter,
            "twitch": twitch.TwitchAdapter,
            "pandatv": pandatv.PandaAdapter,
            "soop": soop.SoopAdapter,
        }

    def get(self, site: str, cookie: str = "") -> LivePlatform | None:
        factory = self._class_map().get(site.strip().lower())
        if factory is None:
            return None
        try:
            return factory(cookie=cookie)
        except TypeError:
            return factory()


registry = _Registry()


def supported_sites() -> list[str]:
    return list(registry._class_map().keys())

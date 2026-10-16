"""Twitch 直播适配器：浏览接口（分区/分区房间/推荐/搜索）。

分区方案照搬 pure_live twitch_site.dart 的 GraphQL persisted query：
  - BrowsePage_AllDirectories  sha256 2f67f71b…  分类目录（VIEWER_COUNT 排序，带 cursor）
  - DirectoryPage_Game         sha256 76cb069d…  分区房间（slug + cursor 分页）
  cursor 表 (kind,id,page)→cursor 的存取逻辑同 pure_live 的 cursorMap。

2026-10 实测：不带 cursor 的首页请求匿名 Client-ID 可用；带 cursor 的翻页请求
稳定返回 IntegrityCheckFailed（Kasada 校验），且 POST /integrity 裸取得的令牌
无法通过——pure_live 是在 App 内嵌 WebView 跑 KPSDK 才拿到可信令牌，纯服务器
端做不到。因此 GQL 路径只取首页数据；需要完整翻页时走 Helix 官方 API：
在 Railway 配置 TWITCH_CLIENT_ID / TWITCH_CLIENT_SECRET（Twitch 开发者控制台
注册一个应用即可拿到），本适配器自动切换并启用 games/top + streams 的 cursor 分页。
"""
from __future__ import annotations

import logging
import os
import random
import time
from typing import Any

from browse_platforms.base import LivePlatform, room_dict
from browse_platforms.common import Http, as_str, strip_tags
from browse_cache import LIST_CACHE

log = logging.getLogger("live.twitch")

CLIENT_IDS = [
    "kimne78kx3ncx6brgo4mv6wki5h1ko",  # 匿名网页播放器 ID，实测可用
    "ue666xxq81dq0l30715w03p3h3a6h",   # 2026-10 实测匿名 GQL 400（Invalid Client-ID）
    "8557m2777l2623943p9951150w621",   # 同上，保留在列表里仅为照原版三 ID 形态
]

GPL_URL = "https://gql.twitch.tv/gql"
HELIX_URL = "https://api.twitch.tv/helix"
OAUTH_URL = "https://id.twitch.tv/oauth2/token"

# 模块级记住最近一次成功的 Client-ID，新实例优先用它（配合 _gql 的降级链，
# 避免每个新 adapter 都先拿失效 ID 撞一次 400）
_GOOD_CLIENT_ID: dict[str, str] = {}

# 分类/房间目录统一包在单父级「热门」下，前端按扁平网格渲染
_ROOT = {"id": "hot", "name": "热门", "pic": "", "children": []}

# 照 pure_live twitch_site.dart 的 cursorMap；适配器每请求新建实例，
# 表必须挂模块级才能跨请求续页。
_CURSOR_MAP: dict[str, str] = {}
_CURSOR_MAP_MAX = 800

# 【P1二轮：语言过滤可配】分区/搜索的语言过滤原硬编码 ZH/KO（pure_live 面向中韩
# 观众），看不到英文区。改为环境变量 TWITCH_LANGS（逗号分隔 ISO 639-1，大小写
# 不敏感；设为空 = 不过滤全语言），默认 zh,ko 保持旧行为。分区用大写形态
# （broadcasterLanguages），搜索用小写形态（languageCodes，该接口已匿名不可用、
# 仅保形态）。
TWITCH_LANGS = [x.strip() for x in os.getenv("TWITCH_LANGS", "zh,ko").split(",") if x.strip()]


def _cursor_key(kind: str, cid: str, page: int) -> str:
    return f"{kind}_{cid}_{page}"


def _get_cursor(kind: str, cid: str, page: int) -> str:
    return _CURSOR_MAP.get(_cursor_key(kind, cid, page), "")


def _save_cursor(kind: str, cid: str, page: int, value: str) -> None:
    if not value:
        return
    if len(_CURSOR_MAP) >= _CURSOR_MAP_MAX:
        for k in list(_CURSOR_MAP)[: len(_CURSOR_MAP) // 2]:
            _CURSOR_MAP.pop(k, None)
    _CURSOR_MAP[_cursor_key(kind, cid, page + 1)] = value


def _is_integrity_error(envelope: dict[str, Any]) -> bool:
    # 照 pure_live buildGraphQlScript 里的 failedIntegrity 判定
    for err in envelope.get("errors", []) or []:
        msg = str((err or {}).get("message", "")).lower()
        if "failed integrity check" in msg or "integrity token" in msg:
            return True
    return False


def _gql_envelope_data(r: Any) -> dict[str, Any]:
    """GQL 单条请求回对象、批量回数组，两种形状都兼容。"""
    if isinstance(r, list) and r:
        return r[0].get("data", {}) or {}
    if isinstance(r, dict):
        return r.get("data", {}) or {}
    return {}


# Twitch GQL 的 previewImageURL 是带 {width}x{height} 字面模板的 URL（实测
# 2026-10：https://static-cdn.jtvnw.net/previews-ttv/live_user_xxx-{width}x{height}.jpg），
# 不替换的话前端会拿模板串直接当图片地址请求 → 必然 404 → 主页/分区房间封面全裂。
# 列表卡片 16:9，与详情/Helix 路径同尺寸保持一致。
_THUMB_SIZE = ("440", "248")


def _tw_thumb(url: str) -> str:
    """替换 previewImageURL 的 {width}x{height} 模板为实际尺寸。"""
    if not url:
        return url
    w, h = _THUMB_SIZE
    return url.replace("{width}", w).replace("{height}", h)


# Helix 应用令牌（client credentials 流程，官方文档接口）
_APP_TOKEN: dict[str, Any] = {"token": "", "expire_at": 0.0}


class TwitchAdapter(LivePlatform):
    site = "twitch"
    name = "Twitch直播"
    logo = "https://www.twitch.tv/favicon.ico"

    def __init__(self, cookie: str = "") -> None:
        self.cookie = cookie
        self.http = Http()
        # 实例化时随机选一个 Client-ID（原注释声称轮换却固定取 [0]，单 ID 长期高频有风控风险）
        self.client_id = random.choice(CLIENT_IDS)

    def _env_client_id(self) -> str:
        return os.getenv("TWITCH_CLIENT_ID", "").strip()

    def _env_client_secret(self) -> str:
        return os.getenv("TWITCH_CLIENT_SECRET", "").strip()

    def _headers(self) -> dict[str, str]:
        h = {"client-id": self.client_id, "content-type": "application/json", "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"}
        if self.cookie:
            h["cookie"] = self.cookie
        return h

    async def _gql(self, payload: list[dict]) -> Any:
        # 照原版 main.py TWITCH_CLIENT_IDS 的 for 回退链语义：起点随机 + 失败换下一个重试，
        # 成功的 ID 记到模块级供后续实例优先复用。
        # （修复 random.choice 抽到已失效 ID → GQL 400 → 整页空列表的间歇性故障）
        base = self._headers()
        order = [self.client_id] + [c for c in CLIENT_IDS if c != self.client_id]
        known = _GOOD_CLIENT_ID.get("id")
        if known and known in order:
            order = [known] + [c for c in order if c != known]
        for cid in order:
            headers = {**base, "client-id": cid}
            try:
                d = await self.http.post_json(GPL_URL, json=payload, headers=headers)
                self.client_id = cid
                _GOOD_CLIENT_ID["id"] = cid
                return d
            except Exception as e:
                log.warning(f"GQL 失败（client_id={cid[:8]}…）→ 换下一个重试 [{type(e).__name__}]: {e}")
        return None

    # ---- Helix（可选，配置环境变量后启用） ----

    async def _helix_token(self) -> str:
        cid, secret = self._env_client_id(), self._env_client_secret()
        if not cid or not secret:
            return ""
        now = time.time()
        if _APP_TOKEN["token"] and now < _APP_TOKEN["expire_at"] - 60:
            return _APP_TOKEN["token"]
        try:
            # 【P0凭据泄露】client_secret 放 POST body（原先拼在 URL query，会被 httpx 日志/中间层记录）
            d = await self.http.post_json(OAUTH_URL, data={
                "client_id": cid, "client_secret": secret, "grant_type": "client_credentials"
            })
            token = as_str(d.get("access_token", ""))
            if token:
                _APP_TOKEN["token"] = token
                _APP_TOKEN["expire_at"] = now + float(d.get("expires_in", 0) or 0)
                return token
        except Exception:
            log.warning("Helix app token 获取失败", exc_info=True)
        return ""

    async def _helix_get(self, path: str, params: dict[str, Any]) -> dict[str, Any] | None:
        token = await self._helix_token()
        if not token:
            return None
        try:
            return await self.http.get_json(f"{HELIX_URL}{path}", params=params, headers={
                "client-id": self._env_client_id(), "authorization": f"Bearer {token}",
            })
        except Exception:
            log.warning(f"Helix {path} 请求失败", exc_info=True)
            return None

    # ---- 分类 ----

    async def get_categories(self) -> list[dict[str, Any]]:
        cached = LIST_CACHE.get("twitch_cats")
        if cached is not None:
            return cached
        children = await self._helix_top_games()
        if not children:
            children = await self._gql_all_directories()
        if not children:
            return []
        root = dict(_ROOT)
        root["children"] = children
        LIST_CACHE.set("twitch_cats", [root], 600)
        return [root]

    async def _helix_top_games(self) -> list[dict[str, Any]] | None:
        """官方 API：GET /helix/games/top?first=100（需要应用令牌）。"""
        if not (self._env_client_id() and self._env_client_secret()):
            return None
        d = await self._helix_get("/games/top", {"first": 100})
        if d is None:
            return None
        children = []
        for g in d.get("data", []) if isinstance(d, dict) else []:
            if not isinstance(g, dict):
                continue
            gid = as_str(g.get("id", ""))
            name = as_str(g.get("name", ""))
            if gid and name:
                pic = as_str(g.get("box_art_url", "")).replace("{width}", "285").replace("{height}", "380")
                children.append({"id": gid, "name": name, "pic": pic, "parentId": _ROOT["id"]})
        return children

    async def _gql_all_directories(self) -> list[dict[str, Any]]:
        """照 pure_live getSubCategores 的 BrowsePage_AllDirectories persisted query，
        去掉 tags 过滤即全局热门游戏目录（实测首页可用；avatarURL 已是固定尺寸）。"""
        payload = [{
            "operationName": "BrowsePage_AllDirectories",
            "extensions": {"persistedQuery": {"version": 1, "sha256Hash": "2f67f71ba89f3c0ed26a141ec00da1defecb2303595f5cda4298169549783d9e"}},
            "variables": {
                "limit": 100,
                "options": {
                    "recommendationsContext": {"platform": "web"},
                    "requestID": "JIRA-VXP-2397",
                    "sort": "VIEWER_COUNT",
                },
            },
        }]
        r = await self._gql(payload)
        data = _gql_envelope_data(r)
        conn = data.get("directoriesWithTags") if isinstance(data, dict) else None
        children = []
        for e in (conn.get("edges", []) or []) if isinstance(conn, dict) else []:
            node = (e or {}).get("node", {}) or {}
            slug = as_str(node.get("slug", ""))
            name = as_str(node.get("displayName", ""))
            if slug and name:
                children.append({"id": slug, "name": name, "pic": as_str(node.get("avatarURL", "")), "parentId": _ROOT["id"]})
        return children

    # ---- 分区房间 ----

    async def get_category_rooms(self, parent_area_id: str, area_id: str, page: int, page_size: int) -> dict[str, Any]:
        ps = min(max(page_size, 1), 100)
        if self._env_client_id() and self._env_client_secret():
            result = await self._helix_category_rooms(area_id, page, ps)
            if result is not None:
                return result
        return await self._gql_category_rooms(area_id, page, ps)

    async def _helix_category_rooms(self, game_id: str, page: int, ps: int) -> dict[str, Any] | None:
        """官方 API：GET /helix/streams?game_id=…&after=cursor，完整翻页。"""
        cursor = _get_cursor("helix_rooms", game_id, page)
        if not cursor and page > 1:
            return {"list": [], "hasMore": False}
        params: dict[str, Any] = {"game_id": game_id, "first": ps}
        if cursor:
            params["after"] = cursor
        d = await self._helix_get("/streams", params)
        if d is None:
            return None
        rooms = []
        for s in d.get("data", []) if isinstance(d, dict) else []:
            if not isinstance(s, dict):
                continue
            login = as_str(s.get("user_login", ""))
            if not login:
                continue
            rooms.append(room_dict(
                roomId=login, title=as_str(s.get("title", "")),
                nickname=as_str(s.get("user_name", "")), avatar=as_str(s.get("profile_image_url", "")),
                cover=_tw_thumb(as_str(s.get("thumbnail_url", ""))),
                area=as_str(s.get("game_name", "")), watching=as_str(s.get("viewer_count", "")),
                isLive=True, platform=self.site,
            ))
        next_cursor = as_str((d.get("pagination") or {}).get("cursor", "")) if isinstance(d, dict) else ""
        _save_cursor("helix_rooms", game_id, page, next_cursor)
        # 【P1二轮：hasMore】Helix 的上游分页信号就是 pagination.cursor（空/缺失即末页）
        # → 纯用上游信号，不再叠加 len>=ps（短页+有 cursor 的边缘场景会被误判成末页）。
        return {"list": rooms, "hasMore": bool(next_cursor)}

    async def _gql_category_rooms(self, slug: str, page: int, ps: int) -> dict[str, Any]:
        """照 pure_live getCategoryRooms 的 DirectoryPage_Game persisted query。
        带 cursor 的翻页会被 Kasada 拦（IntegrityCheckFailed），此时返回空页
        让前端停在首页数据；首页（无 cursor）实测稳定可用。"""
        cursor = _get_cursor("gql_rooms", slug, page)
        if not cursor and page > 1:
            return {"list": [], "hasMore": False}
        variables: dict[str, Any] = {
            "imageWidth": 50,
            "slug": slug,
            "options": {
                "sort": "VIEWER_COUNT",
                "recommendationsContext": {"platform": "web"},
                "requestID": "JIRA-VXP-2397",
                "freeformTags": None,
                "tags": [],
                # 语言过滤可配（TWITCH_LANGS，默认 zh,ko 保持 pure_live 旧行为；空=全语言）
                "broadcasterLanguages": [x.upper() for x in TWITCH_LANGS],
                "systemFilters": [],
            },
            "sortTypeIsRecency": False,
            "limit": ps,
            "includeCostreaming": True,
        }
        if cursor:
            variables["cursor"] = cursor
        r = await self._gql([{
            "operationName": "DirectoryPage_Game",
            "variables": variables,
            "extensions": {"persistedQuery": {"version": 1, "sha256Hash": "76cb069d835b8a02914c08dc42c421d0dafda8af5b113a3f19141824b901402f"}},
        }])
        data = _gql_envelope_data(r)
        if not data:
            return {"list": [], "hasMore": False}
        if isinstance(r, dict) and _is_integrity_error(r):
            log.warning("Twitch 分区翻页被 integrity 校验拦截（page=%s slug=%s），如需完整翻页请配置 TWITCH_CLIENT_ID/SECRET 走 Helix", page, slug)
            return {"list": [], "hasMore": False}
        game = data.get("game") or {}
        streams = game.get("streams") if isinstance(game, dict) else None
        if not isinstance(streams, dict):
            return {"list": [], "hasMore": False}
        edges = streams.get("edges", []) or []
        rooms = []
        for e in edges:
            node = (e or {}).get("node", {}) or {}
            broadcaster = node.get("broadcaster", {}) or {}
            login = as_str(broadcaster.get("login", ""))
            if not login:
                continue
            rooms.append(room_dict(
                roomId=login, title=as_str(node.get("title", "")),
                nickname=as_str(broadcaster.get("displayName", "")), avatar="",
                cover=_tw_thumb(as_str(node.get("previewImageURL", ""))), area="",
                watching=as_str(node.get("viewersCount", "")), isLive=True, platform=self.site,
            ))
        next_cursor = as_str(edges[-1].get("cursor", "")) if edges else ""
        page_info = streams.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            # 照 pure_live parseConnection：pageInfo 缺失/无下一页时终止
            next_cursor = ""
        _save_cursor("gql_rooms", slug, page, next_cursor)
        # 【P1二轮：hasMore】next_cursor 已编码 pageInfo.hasNextPage（缺失/False 时
        # 被清空）→ 纯用上游信号；实测 GQL 带过来翻页会被 Kasada integrity 拦
        # （返回空页停在第 1 页），完整翻页需配 TWITCH_CLIENT_ID/SECRET 走 Helix。
        return {"list": rooms, "hasMore": bool(next_cursor)}

    async def get_recommend_rooms(self, page: int, page_size: int) -> dict[str, Any]:
        return await self._global_streams(page, min(max(page_size, 1), 100))

    async def _global_streams(self, page: int, ps: int) -> dict[str, Any]:
        # 全局推荐无游戏维度；streams 连接的 cursor 翻页同样受 integrity 限制，
        # 只返回首页数据（实测稳定可用）
        node_fields = "id viewersCount title previewImageURL broadcaster { id displayName login }"
        payload = [{
            "operationName": "Streams",
            "variables": {"first": ps},
            "query": f"query Streams($first: Int!) {{ streams(first: $first) {{ edges {{ node {{ {node_fields} }} }} }} }}",
        }]
        r = await self._gql(payload)
        data = _gql_envelope_data(r)
        if not data or (isinstance(r, dict) and _is_integrity_error(r)):
            return {"list": [], "hasMore": False}
        rooms = []
        streams = data.get("streams") or {}
        edges = streams.get("edges", []) or [] if isinstance(streams, dict) else []
        for e in edges:
            node = (e or {}).get("node", {}) or {}
            broadcaster = node.get("broadcaster", {}) or {}
            login = as_str(broadcaster.get("login", ""))
            if not login:
                continue
            rooms.append(room_dict(
                roomId=login, title=as_str(node.get("title", "")),
                nickname=as_str(broadcaster.get("displayName", "")), avatar="",
                cover=_tw_thumb(as_str(node.get("previewImageURL", ""))), area="",
                watching=as_str(node.get("viewersCount", "")), isLive=True, platform=self.site,
            ))
        return {"list": rooms, "hasMore": False}

    async def search_rooms(self, keyword: str, page: int, page_size: int) -> dict[str, Any]:
        # 注：Twitch 已把搜索接口改到需要 OAuth 的操作上，匿名 Client-ID 查不到任何
        # searchResults/searchChannels/typeaheadSearch，这里返回空列表兜底。
        try:
            payload = [{
                "operationName": "SearchResults",
                "variables": {"query": keyword, "first": 20, "languageCodes": [x.lower() for x in TWITCH_LANGS]},
                "query": "query SearchResults($query: String!, $first: Int!) { searchResults(first: $first, query: $query, type: CHANNEL) { items { title id displayName } } }",
            }]
            r = await self._gql(payload)
            rooms = []
            data = _gql_envelope_data(r)
            if data:
                for item in (data.get("searchResults", {}).get("items", []) or []):
                    if not isinstance(item, dict):
                        continue
                    rooms.append(room_dict(
                        roomId=as_str(item.get("id", "")), title=strip_tags(item.get("title", "")),
                        nickname=strip_tags(item.get("displayName", "")), cover="", isLive=True, platform=self.site,
                    ))
            return {"list": rooms, "hasMore": False}
        except Exception:
            return {"list": [], "hasMore": False}

    async def get_room_detail(self, room_id: str) -> dict[str, Any] | None:
        """GQL user(login) 查询：stream.title 是直播间标题、viewersCount 人气；
        user 为空（离线/不存在）返回 None 回退 parse。实测匿名 Client-ID 可用。"""
        try:
            login = str(room_id or "").strip()
            if not login:
                return None
            # P1：GraphQL 变量化——login 是 URL 路径可控参数，此前 % 插值进查询串
            # （靠删引号挡注入，反斜杠仍可拼坏查询）。$login 变量传参后任意字符都是
            # 纯数据，查询结构固定，注入面归零。
            payload = [{
                "query": 'query User($login: String!) { user(login: $login) { displayName profileImageURL(width: 150) stream { title viewersCount previewImageURL } } }',
                "variables": {"login": login},
            }]
            r = await self._gql(payload)
            # 实测单条查询也可能回数组（[{data:…}]），必须用双形状兼容的取值，
            # 否则 user 恒为 None、详情恒空、播放页拿不到 viewersCount。
            data = _gql_envelope_data(r)
            user = data.get("user") if isinstance(data, dict) else None
            if not isinstance(user, dict):
                return None
            stream = user.get("stream") or {}
            cover = _tw_thumb(as_str(stream.get("previewImageURL", "")))
            return room_dict(roomId=room_id, title=as_str(stream.get("title", "")), nickname=as_str(user.get("displayName", "")),
                             avatar=as_str(user.get("profileImageURL", "")), cover=cover,
                             watching=as_str(stream.get("viewersCount", "")), isLive=bool(stream), platform=self.site)
        except Exception:
            return None

    async def close(self) -> None:
        await self.http.close()

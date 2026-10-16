"""Tiny TTL cache used across the parse/stream/browse paths.

Kept dependency-free (no redis) so the relay stays a single small process;
a dict + monotonic timestamps is plenty for a per-room browse cache.
"""
from __future__ import annotations

import copy
import threading
import time
from typing import Any, Callable


class TTLCache:
    def __init__(self, default_ttl: float = 15.0, max_items: int = 2000) -> None:
        self._store: dict[str, tuple[float, Any]] = {}
        self._ttl = default_ttl
        self._max = max_items
        self._lock = threading.Lock()

    def get(self, key: str) -> Any | None:
        with self._lock:
            item = self._store.get(key)
            if item is None:
                return None
            expires, value = item
            if expires < time.monotonic():
                self._store.pop(key, None)
                return None
            # P1（缓存深拷贝）：缓存值是跨请求共享的可变结构（分区列表/解析结果 dict），
            # 下游会原地改（browse_api 给 cached cat 的 pic 包 Worker 代理等）——直接
            # 返回原对象会让缓存被单条路径污染 + 并发「边序列化边被改」的撕裂读。
            # 读出即深拷贝（不可变对象如 str 会被 deepcopy 原样返回，零开销路径）。
            return copy.deepcopy(value)

    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        expires = time.monotonic() + (ttl if ttl is not None else self._ttl)
        with self._lock:
            if len(self._store) >= self._max:
                # Drop the single oldest key instead of walking the whole map.
                try:
                    oldest = min(self._store, key=lambda k: self._store[k][0])
                    self._store.pop(oldest, None)
                except ValueError:
                    pass
            self._store[key] = (expires, value)

    async def get_or_set(self, key: str, factory: Callable[[], Any], ttl: float | None = None) -> Any:
        hit = self.get(key)
        if hit is not None:
            return hit
        value = await factory()
        if value is not None:
            self.set(key, value, ttl)
        return value

    def contains(self, key: str) -> bool:
        return self.get(key) is not None

    def clear(self) -> None:
        with self._lock:
            self._store.clear()


# Shared instances
STREAM_CACHE = TTLCache(default_ttl=20.0)
LIST_CACHE = TTLCache(default_ttl=60.0)
ROOM_CACHE = TTLCache(default_ttl=30.0)


class StaleTTLCache:
    """软/硬两段过期的缓存（浏览三接口专用，stale-if-error 兜底）。

    - 软过期（ttl）：get() 起不再命中——提醒调用方该去上游抓新的了；
    - 硬过期（ttl + grace）：到点才真正删除（读取时顺手清 + 周期 sweep 批量清）；
    - get_stale()：刷新失败时取「上次成功数据」兜底（仍在硬保留期内的值）；
    - 读出即深拷贝（与 TTLCache 同因：跨请求共享的可变结构会被下游原地改）。
    """

    def __init__(self, default_ttl: float = 180.0, max_items: int = 500,
                 grace: float = 24 * 3600.0, sweep_interval: float = 600.0) -> None:
        # key -> (soft_expire, hard_expire, value)
        self._store: dict[str, tuple[float, float, Any]] = {}
        self._ttl = default_ttl
        self._max = max_items
        self._grace = grace
        self._sweep_interval = sweep_interval
        self._last_sweep = 0.0
        self._lock = threading.Lock()
        self._now = time.monotonic  # 测试可注入假时钟

    def get(self, key: str) -> Any | None:
        with self._lock:
            item = self._store.get(key)
            if item is None:
                return None
            soft, hard, value = item
            now = self._now()
            if now >= hard:
                self._store.pop(key, None)
                return None
            if now >= soft:
                return None  # 软过期：留给 get_stale 兜底，不当场删
            return copy.deepcopy(value)

    def get_stale(self, key: str) -> Any | None:
        """硬保留期内一律可取（含已软过期的旧值）——仅供刷新失败时兜底。"""
        with self._lock:
            item = self._store.get(key)
            if item is None:
                return None
            _, hard, value = item
            if self._now() >= hard:
                self._store.pop(key, None)
                return None
            return copy.deepcopy(value)

    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        now = self._now()
        soft = now + (ttl if ttl is not None else self._ttl)
        with self._lock:
            # 周期清扫并入写入路径（保活进程里没有别的定时器，写入即清扫时机）
            if now - self._last_sweep >= self._sweep_interval:
                self._sweep_locked(now)
                self._last_sweep = now
            if len(self._store) >= self._max and key not in self._store:
                # 丢「最接近硬过期」的一条：已过期/最旧的优先出局
                try:
                    oldest = min(self._store, key=lambda k: self._store[k][1])
                    self._store.pop(oldest, None)
                except ValueError:
                    pass
            self._store[key] = (soft, soft + self._grace, value)

    def sweep(self) -> int:
        """清掉全部硬过期条目（测试/手动可调；常规由 set() 周期触发）。"""
        with self._lock:
            return self._sweep_locked(self._now())

    def _sweep_locked(self, now: float) -> int:
        dead = [k for k, (_, hard, _) in self._store.items() if now >= hard]
        for k in dead:
            self._store.pop(k, None)
        return len(dead)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()


# 浏览三接口专用（browse_api 统一包装）：软 TTL 分区 24h / 推荐 3min / 分区房间 5min，
# 硬过期 = 软 + 24h 保留（stale-if-error 兜底窗口）；上限 500 键（≈ 15~25MB 封顶）。
BROWSE_CACHE = StaleTTLCache(default_ttl=180.0, max_items=500)
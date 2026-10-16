"""Tiny TTL cache used across the parse/stream/browse paths.

Kept dependency-free (no redis) so the relay stays a single small process;
a dict + monotonic timestamps is plenty for a per-room browse cache.
"""
from __future__ import annotations

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
            return value

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
"""
common.base.cache

Persistent and in-memory caching utilities for Titan Tools.

Features:
 - CacheManager: hybrid memory + disk caching
 - TTL (time-to-live) expiry
 - Thread-safe, integrated with Titan logging
 - Pylance-safe typing for shelve interface using Protocol + cast()
"""

from __future__ import annotations
import os
import shelve
import threading
import time
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional, Protocol, cast

from .logging import get_logger
from .fs import ensure_dir

log = get_logger(__name__)


# ----------------------------------------------------------------------
# TYPE DEFINITIONS
# ----------------------------------------------------------------------

class ShelfProtocol(Protocol):
    """Protocol describing the minimal interface of a shelf-like object."""
    def __getitem__(self, key: str) -> tuple[float, Any]: ...
    def __setitem__(self, key: str, value: tuple[float, Any]) -> None: ...
    def __delitem__(self, key: str) -> None: ...
    def __contains__(self, key: object) -> bool: ...
    def clear(self) -> None: ...
    def sync(self) -> None: ...
    def close(self) -> None: ...

ShelfType = ShelfProtocol


# ----------------------------------------------------------------------
# SIMPLE IN-MEMORY CACHE
# ----------------------------------------------------------------------

@lru_cache(maxsize=256)
def memoize(key: str, value: Any) -> Any:
    """Simple LRU in-memory cache for small repeated values."""
    return value


# ----------------------------------------------------------------------
# CACHE MANAGER CLASS
# ----------------------------------------------------------------------

class CacheManager:
    """
    Hybrid cache combining memory and persistent disk storage.

    Example:
        cache = CacheManager("~/.cache/titan_tools/state.db")
        cache.set("scan_result", {"count": 100})
        print(cache.get("scan_result"))
    """

    def __init__(self, path: str | Path = "~/.cache/titan_tools/state.db", ttl: Optional[int] = None):
        self.path = Path(os.path.expanduser(path)).resolve()
        ensure_dir(self.path.parent)
        self.ttl = ttl  # seconds
        self._lock = threading.Lock()
        self._memory_cache: dict[str, tuple[float, Any]] = {}
        self._shelve: Optional[ShelfType] = None

        try:
            self._shelve = cast(ShelfType, shelve.open(str(self.path), writeback=True))
            log.debug(f"🧠 Cache opened at {self.path}")
        except Exception as e:
            log.warning(f"Failed to open cache file: {e}")

    # ------------------------------------------------------------------
    # CORE CACHE OPERATIONS
    # ------------------------------------------------------------------

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a value from memory or disk cache."""
        with self._lock:
            # 1️⃣ Memory cache
            if key in self._memory_cache:
                ts, val = self._memory_cache[key]
                if not self._expired(ts):
                    log.debug(f"Cache hit (memory): {key}")
                    return val
                log.debug(f"Cache expired (memory): {key}")
                del self._memory_cache[key]

            # 2️⃣ Disk cache
            if self._shelve and key in self._shelve:
                ts, val = self._shelve[key]
                if not self._expired(ts):
                    log.debug(f"Cache hit (disk): {key}")
                    self._memory_cache[key] = (ts, val)
                    return val
                log.debug(f"Cache expired (disk): {key}")
                del self._shelve[key]

        log.debug(f"Cache miss: {key}")
        return default

    def set(self, key: str, value: Any) -> None:
        """Store a value in memory and disk cache."""
        with self._lock:
            ts = time.time()
            self._memory_cache[key] = (ts, value)

            if self._shelve is not None:
                try:
                    self._shelve[key] = (ts, value)
                    self._shelve.sync()
                    log.debug(f"Cache set: {key}")
                except Exception as e:
                    log.error(f"Failed to write cache key '{key}': {e}")

    def delete(self, key: str) -> bool:
        """Delete a cache entry from both memory and disk."""
        removed = False
        with self._lock:
            if key in self._memory_cache:
                del self._memory_cache[key]
                removed = True
            if self._shelve and key in self._shelve:
                del self._shelve[key]
                self._shelve.sync()
                removed = True

        if removed:
            log.debug(f"🗑️ Cache key deleted: {key}")
        return removed

    def clear(self) -> None:
        """Clear both memory and disk cache."""
        with self._lock:
            self._memory_cache.clear()
            if self._shelve:
                self._shelve.clear()
                self._shelve.sync()
        log.info("🧹 Cache cleared.")

    # ------------------------------------------------------------------
    # UTILITIES
    # ------------------------------------------------------------------

    def _expired(self, timestamp: float) -> bool:
        """Check if a cache entry has expired."""
        if not self.ttl:
            return False
        return (time.time() - timestamp) > self.ttl

    def close(self) -> None:
        """Close the underlying cache safely."""
        if self._shelve:
            try:
                self._shelve.close()
                log.debug("Cache closed.")
            except Exception as e:
                log.warning(f"Error closing cache: {e}")

    def __enter__(self) -> CacheManager:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False


# ----------------------------------------------------------------------
# SELF TEST
# ----------------------------------------------------------------------

if __name__ == "__main__":
    log.info("✅ common.base.cache self-test:")

    cache = CacheManager(ttl=5)
    cache.set("example", {"msg": "Hello Titan!"})
    print("Cached value:", cache.get("example"))

    time.sleep(2)
    print("After 2s:", cache.get("example"))

    time.sleep(5)
    print("After expiry:", cache.get("example"))

    cache.delete("example")
    cache.clear()
    cache.close()

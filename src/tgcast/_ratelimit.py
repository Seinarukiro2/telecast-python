"""Token-bucket rate limiter with global + per-chat limits."""

from __future__ import annotations

import threading
import time
from collections import OrderedDict


class _TokenBucket:
    """Simple token-bucket rate limiter."""

    __slots__ = ("_rate", "_capacity", "_tokens", "_last")

    def __init__(self, rate: float, capacity: float | None = None) -> None:
        self._rate = rate
        self._capacity = capacity or rate
        self._tokens = self._capacity
        self._last = time.monotonic()

    def acquire(self) -> float:
        """Try to consume one token. Returns 0 if OK, or seconds to wait."""
        now = time.monotonic()
        elapsed = now - self._last
        self._last = now
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
        if self._tokens >= 1:
            self._tokens -= 1
            return 0.0
        return (1 - self._tokens) / self._rate


class RateLimiter:
    """Global + per-chat rate limiter with LRU eviction."""

    def __init__(
        self,
        global_rps: float = 25.0,
        per_chat_rps: float = 1.0,
        max_chat_entries: int = 10_000,
    ) -> None:
        self._global = _TokenBucket(global_rps)
        self._per_chat_rps = per_chat_rps
        self._max = max_chat_entries
        self._chats: OrderedDict[int, _TokenBucket] = OrderedDict()
        self._lock = threading.Lock()
        self._throttled_until: float = 0

    def acquire(self, chat_id: int) -> float:
        """Returns seconds to wait (0 = proceed now)."""
        now = time.monotonic()

        # Check 429 throttle
        if now < self._throttled_until:
            return self._throttled_until - now

        # Global limit
        gw = self._global.acquire()
        if gw > 0:
            return gw

        # Per-chat limit
        with self._lock:
            if chat_id in self._chats:
                self._chats.move_to_end(chat_id)
                bucket = self._chats[chat_id]
            else:
                if len(self._chats) >= self._max:
                    self._chats.popitem(last=False)
                bucket = _TokenBucket(self._per_chat_rps)
                self._chats[chat_id] = bucket

        return bucket.acquire()

    def throttle(self, seconds: float) -> None:
        """Apply global throttle (for Telegram 429 responses)."""
        self._throttled_until = time.monotonic() + seconds

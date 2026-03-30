import time
from telecast._ratelimit import RateLimiter


def test_first_acquire_instant():
    rl = RateLimiter(global_rps=100, per_chat_rps=10)
    wait = rl.acquire(1)
    assert wait == 0


def test_throttle():
    rl = RateLimiter(global_rps=100, per_chat_rps=10)
    rl.throttle(1.0)
    wait = rl.acquire(1)
    assert wait > 0


def test_per_chat_limit():
    rl = RateLimiter(global_rps=1000, per_chat_rps=1)
    rl.acquire(42)
    wait = rl.acquire(42)
    assert wait > 0  # second request should wait


def test_different_chats_independent():
    rl = RateLimiter(global_rps=1000, per_chat_rps=1)
    rl.acquire(1)
    wait = rl.acquire(2)  # different chat
    assert wait == 0

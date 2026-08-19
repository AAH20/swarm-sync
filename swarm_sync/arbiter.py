import time
import asyncio
from typing import Dict, Optional

class RateLimitQuotaExceeded(Exception):
    """Raised when an agent swarm exceeds downstream database/API token capacity."""
    pass

class ThunderingHerdArbiter:
    """
    Intelligent token-bucket arbiter preventing 100-300 concurrent agents
    from crashing downstream PostgreSQL connections or third-party APIs.
    """
    def __init__(self, max_concurrent: int = 50, rate_per_second: float = 100.0):
        self.max_concurrent = max_concurrent
        self.rate_per_second = rate_per_second
        self.tokens = float(max_concurrent)
        self.last_update = time.time()
        self._lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(max_concurrent)

    async def acquire(self, timeout_s: float = 5.0):
        try:
            await asyncio.wait_for(self._semaphore.acquire(), timeout=timeout_s)
        except asyncio.TimeoutError:
            raise RateLimitQuotaExceeded("Downstream connection pool saturated by concurrent agent burst")

        async with self._lock:
            now = time.time()
            elapsed = now - self.last_update
            self.tokens = min(float(self.max_concurrent), self.tokens + elapsed * self.rate_per_second)
            self.last_update = now

            if self.tokens < 1.0:
                self._semaphore.release()
                raise RateLimitQuotaExceeded("Thundering-herd rate ceiling exceeded")
            self.tokens -= 1.0

    def release(self):
        self._semaphore.release()

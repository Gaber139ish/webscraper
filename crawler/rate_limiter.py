import asyncio
import time
from urllib.parse import urlparse

class DomainRateLimiter:
    def __init__(self, default_delay_seconds: float = 0.5, domain_overrides: dict | None = None):
        self.default_delay_seconds = default_delay_seconds
        self.domain_overrides = domain_overrides or {}
        self._domain_to_last_ts: dict[str, float] = {}
        self._lock = asyncio.Lock()

    def _delay_for_domain(self, domain: str) -> float:
        return float(self.domain_overrides.get(domain, self.default_delay_seconds))

    async def wait_for_slot(self, url: str):
        domain = urlparse(url).netloc
        async with self._lock:
            now = time.monotonic()
            last = self._domain_to_last_ts.get(domain, 0.0)
            delay_needed = self._delay_for_domain(domain)
            wait_time = last + delay_needed - now
            if wait_time > 0:
                await asyncio.sleep(wait_time)
                now = time.monotonic()
            self._domain_to_last_ts[domain] = now
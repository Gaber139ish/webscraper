import asyncio
import httpx
from typing import Optional, Dict
from urllib.parse import urlparse, urljoin
from urllib import robotparser

class RobotsCache:
    def __init__(self, user_agent: Optional[str] = None):
        self.user_agent = user_agent or "*"
        self._host_to_parser: Dict[str, robotparser.RobotFileParser] = {}
        self._lock = asyncio.Lock()

    async def _fetch_and_build(self, base_url: str) -> robotparser.RobotFileParser:
        parsed = urlparse(base_url)
        robots_url = urljoin(f"{parsed.scheme}://{parsed.netloc}", "/robots.txt")
        rp = robotparser.RobotFileParser()
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.get(robots_url)
                if r.status_code >= 400:
                    # treat missing/denied as allow by default
                    rp.parse([])
                else:
                    rp.parse(r.text.splitlines())
        except Exception:
            rp.parse([])
        return rp

    async def _get_parser(self, url: str) -> robotparser.RobotFileParser:
        host = urlparse(url).netloc
        async with self._lock:
            if host in self._host_to_parser:
                return self._host_to_parser[host]
            rp = await self._fetch_and_build(url)
            self._host_to_parser[host] = rp
            return rp

    def is_allowed(self, url: str) -> bool:
        # fire-and-forget populate; fallback allow until loaded
        parser = self._host_to_parser.get(urlparse(url).netloc)
        if parser is None:
            # Kick off background populate without blocking
            asyncio.create_task(self._get_parser(url))
            return True
        return parser.can_fetch(self.user_agent, url)
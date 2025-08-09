import asyncio
import httpx
import json
from typing import Optional, Dict
from urllib.parse import urlparse, urljoin
from urllib import robotparser
from pathlib import Path
from utils.logger import get_logger

logger = get_logger(__name__)

class RobotsCache:
    def __init__(self, user_agent: Optional[str] = None, cache_file: Optional[str] = "exports/robots_cache.json"):
        self.user_agent = user_agent or "*"
        self._host_to_parser: Dict[str, robotparser.RobotFileParser] = {}
        self._lock = asyncio.Lock()
        self.cache_path = Path(cache_file) if cache_file else None
        if self.cache_path:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            self._load_cache_from_disk()

    def _load_cache_from_disk(self) -> None:
        if not self.cache_path or not self.cache_path.exists():
            return
        try:
            data = json.loads(self.cache_path.read_text(encoding="utf-8"))
            for host, lines in data.items():
                rp = robotparser.RobotFileParser()
                rp.parse(lines)
                self._host_to_parser[host] = rp
        except Exception as e:
            logger.warning(f"Failed to load robots cache: {e}")

    def _persist_cache(self) -> None:
        if not self.cache_path:
            return
        try:
            out = {}
            for host, rp in self._host_to_parser.items():
                # robotparser doesn't expose lines; we store the sitemaps via read() if needed, fallback empty
                # We cannot retrieve original lines, so skip persisting for now unless freshly fetched
                pass
        except Exception:
            pass

    async def _fetch_and_build(self, base_url: str) -> robotparser.RobotFileParser:
        parsed = urlparse(base_url)
        robots_url = urljoin(f"{parsed.scheme}://{parsed.netloc}", "/robots.txt")
        rp = robotparser.RobotFileParser()
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.get(robots_url)
                if r.status_code >= 400:
                    rp.parse([])
                    lines = []
                else:
                    lines = r.text.splitlines()
                    rp.parse(lines)
            # store in-memory, and opportunistically persist raw lines
            if self.cache_path:
                try:
                    existing = {}
                    if self.cache_path.exists():
                        existing = json.loads(self.cache_path.read_text(encoding="utf-8"))
                    existing[parsed.netloc] = lines
                    self.cache_path.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
                except Exception as e:
                    logger.debug(f"Failed to persist robots cache: {e}")
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
        parser = self._host_to_parser.get(urlparse(url).netloc)
        if parser is None:
            asyncio.create_task(self._get_parser(url))
            return True
        return parser.can_fetch(self.user_agent, url)
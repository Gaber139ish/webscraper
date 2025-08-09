import asyncio
import time
from typing import Any, Dict, Optional, Set, Tuple
from urllib.parse import urljoin, urldefrag, urlparse
from crawler.browser_driver import BrowserDriver
from crawler.api_sniffer import attach_sniffer
from parser.html_parser import parse_html
from pipeline.cleaner import normalize_parsed
from crawler.robots import RobotsCache
from utils.logger import get_logger

logger = get_logger(__name__)

async def run_crawl(cfg: Dict[str, Any], json_writer, sqlite_store) -> None:
    start_urls = cfg.get("start_urls", [])
    if not start_urls:
        logger.warning("No start_urls configured; skipping crawl")
        return

    concurrency: int = cfg.get("concurrency", 2)
    queue: asyncio.Queue[Tuple[Optional[str], Optional[int]]] = asyncio.Queue()
    for url in start_urls:
        await queue.put((url, 0))

    visited: Set[str] = set()
    robots = RobotsCache(user_agent=cfg.get("user_agent")) if cfg.get("crawl", {}).get("respect_robots", False) else None

    async with BrowserDriver(user_agent=cfg.get("user_agent"), headless=True) as drv:
        async def worker(name: str) -> None:
            ctx = await drv.new_context()
            page = await ctx.new_page()
            api_hits = []

            async def on_api(data):
                api_hits.append(data)

            if cfg.get("crawl", {}).get("intercept_api", True):
                await attach_sniffer(page, on_api)

            try:
                while True:
                    url, depth = await queue.get()
                    if url is None:
                        queue.task_done()
                        break

                    if (url in visited) or (depth is not None and depth > cfg.get("max_depth", 2)):
                        queue.task_done()
                        continue
                    visited.add(url)

                    try:
                        logger.info(f"[{name}] Visiting {url} (depth={depth})")
                        await page.goto(url, wait_until="networkidle")
                        await asyncio.sleep(cfg.get("crawl", {}).get("wait_after_load", 1.0))
                        html = await page.content()
                        parsed = parse_html(url, html)

                        parsed['scrape_meta'] = {
                            "url": url,
                            "depth": depth,
                            "timestamp": int(time.time()),
                            "api_hits": api_hits.copy()
                        }

                        parsed = normalize_parsed(parsed)
                        await json_writer.write(parsed)
                        await sqlite_store.insert(parsed)

                        for link in parsed.get("links", []):
                            normalized = normalize_url(link, url)
                            if normalized and should_follow(normalized, cfg, url, robots):
                                await queue.put((normalized, (depth or 0) + 1))

                        api_hits.clear()
                        await asyncio.sleep(cfg.get("rate_limit", {}).get("delay_seconds", 0.5))
                    except Exception as e:
                        logger.error(f"[{name}] crawl error for {url}: {e}")
                    finally:
                        queue.task_done()
            finally:
                await ctx.close()

        workers = [asyncio.create_task(worker(f"w{i}")) for i in range(concurrency)]
        await queue.join()
        for _ in range(concurrency):
            await queue.put((None, None))
        await asyncio.gather(*workers, return_exceptions=True)

def normalize_url(href: str, base: str) -> Optional[str]:
    try:
        href = href.strip()
        if href.startswith("javascript:") or href.startswith("mailto:"):
            return None
        joined = urljoin(base, href)
        clean, _ = urldefrag(joined)
        return clean
    except Exception:
        return None

def should_follow(url: str, cfg: Dict[str, Any], base_url: str, robots: Optional[RobotsCache] = None) -> bool:
    if not cfg.get("crawl", {}).get("follow_external", False):
        base_dom = urlparse(base_url).netloc
        if urlparse(url).netloc != base_dom:
            return False
    if robots is not None:
        try:
            if not robots.is_allowed(url):
                return False
        except Exception:
            return False
    return True

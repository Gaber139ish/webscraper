import asyncio
import time
import random
from urllib.parse import urljoin, urldefrag, urlparse
from crawler.browser_driver import BrowserDriver
from crawler.api_sniffer import attach_sniffer
from parser.html_parser import parse_html
from pipeline.cleaner import normalize_parsed
from crawler.robots import RobotsCache
from crawler.rate_limiter import DomainRateLimiter
from prom.metrics import PAGES_CRAWLED, PAGES_SKIPPED, CRAWL_ERRORS, API_HITS

try:
    from playwright_stealth import stealth_async
except Exception:
    stealth_async = None

async def run_crawl(cfg, json_writer, sqlite_store):
    start_urls = cfg.get("start_urls", [])
    if not start_urls:
        return

    concurrency = cfg.get("concurrency", 2)
    queue = asyncio.Queue()
    for url in start_urls:
        await queue.put((url, 0))

    visited_mem = set()

    rl_cfg = cfg.get("rate_limit", {})
    rate_limiter = DomainRateLimiter(
        default_delay_seconds=float(rl_cfg.get("delay_seconds", 0.5)),
        domain_overrides=rl_cfg.get("per_domain_delays", {})
    )

    proxy_list = cfg.get("proxies", {}).get("playwright", []) or []
    ua_list = cfg.get("user_agents", []) or ([cfg.get("user_agent")] if cfg.get("user_agent") else [])

    robots = RobotsCache(
        user_agent=(cfg.get("user_agent") or (ua_list[0] if ua_list else "*")),
        proxies=cfg.get("proxies", {}).get("httpx")
    ) if cfg.get("crawl", {}).get("respect_robots", False) else None

    async def get_next_proxy():
        if not proxy_list:
            return None
        proxy = proxy_list.pop(0)
        proxy_list.append(proxy)
        return proxy

    def pick_user_agent():
        if not ua_list:
            return None
        return random.choice(ua_list)

    async def worker(name: str):
        proxy = await get_next_proxy()
        ua = pick_user_agent()
        async with BrowserDriver(user_agent=ua, headless=True, proxy=proxy) as drv:
            ctx = await drv.new_context(user_agent=ua)
            page = await ctx.new_page()
            if stealth_async is not None and cfg.get("crawl", {}).get("stealth", True):
                try:
                    await stealth_async(page)
                except Exception:
                    pass

            api_hits = []

            async def on_api(data):
                API_HITS.inc()
                api_hits.append(data)

            if cfg.get("crawl", {}).get("intercept_api", True):
                await attach_sniffer(page, on_api)

            try:
                while True:
                    item = await queue.get()
                    url, depth = item
                    if url is None:
                        queue.task_done()
                        break

                    if (url in visited_mem) or (depth > cfg.get("max_depth", 2)):
                        PAGES_SKIPPED.inc()
                        queue.task_done()
                        continue

                    if await sqlite_store.has_url(url):
                        visited_mem.add(url)
                        PAGES_SKIPPED.inc()
                        queue.task_done()
                        continue

                    visited_mem.add(url)

                    try:
                        await rate_limiter.wait_for_slot(url)
                        # robots crawl-delay augmentation
                        if robots is not None:
                            delay = robots.crawl_delay(url)
                            if delay:
                                await asyncio.sleep(float(delay))
                        # request
                        resp = await page.goto(url, wait_until="networkidle")
                        await asyncio.sleep(cfg.get("crawl", {}).get("wait_after_load", 1.0))
                        html = await page.content()
                        parsed = parse_html(url, html)

                        # headers for ETag/Last-Modified if available
                        try:
                            headers = dict(resp.headers) if resp else {}
                        except Exception:
                            headers = {}

                        parsed['scrape_meta'] = {
                            "url": url,
                            "depth": depth,
                            "timestamp": int(time.time()),
                            "api_hits": api_hits.copy(),
                            "user_agent": ua,
                            "proxy": proxy,
                            "etag": headers.get("etag"),
                            "last_modified": headers.get("last-modified"),
                        }

                        parsed = normalize_parsed(parsed)
                        await json_writer.write(parsed)
                        await sqlite_store.insert(parsed)
                        PAGES_CRAWLED.inc()

                        for link in parsed.get("links", []):
                            normalized = normalize_url(link, url)
                            if normalized and should_follow(normalized, cfg, url, robots):
                                await queue.put((normalized, depth + 1))

                        api_hits.clear()
                    except Exception as e:
                        CRAWL_ERRORS.inc()
                        print("crawl error", url, e)
                    finally:
                        queue.task_done()
            finally:
                await ctx.close()

    workers = [asyncio.create_task(worker(f"w{i}")) for i in range(concurrency)]
    await queue.join()
    for _ in range(concurrency):
        await queue.put((None, None))
    await asyncio.gather(*workers, return_exceptions=True)

def normalize_url(href, base):
    try:
        href = href.strip()
        if href.startswith("javascript:") or href.startswith("mailto:"):
            return None
        joined = urljoin(base, href)
        clean, _ = urldefrag(joined)
        return clean
    except Exception:
        return None

def should_follow(url, cfg, base_url, robots=None):
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

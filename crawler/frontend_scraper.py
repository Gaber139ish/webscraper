import asyncio
import re
import time
from urllib.parse import urljoin, urldefrag, urlparse
from crawler.browser_driver import BrowserDriver
from crawler.api_sniffer import attach_sniffer
from parser.html_parser import parse_html
from aiofiles import open as aioopen
from tqdm.asyncio import tqdm

visited = set()

async def run_crawl(cfg, json_writer, sqlite_store):
    start = cfg.get("start_urls", [])
    concurrency = cfg.get("concurrency", 2)
    sem = asyncio.Semaphore(concurrency)
    queue = asyncio.Queue()
    for u in start:
        await queue.put((u, 0))

    async with BrowserDriver(user_agent=cfg.get("user_agent"), headless=True) as drv:
        contexts = []
        # We'll create one browser context per worker to isolate storage
        async def worker(name):
            async with sem:
                ctx = await drv.new_context()
                page = await ctx.new_page()
                api_hits = []

                async def on_api(data):
                    api_hits.append(data)

                await attach_sniffer(page, on_api)

                while not queue.empty():
                    url, depth = await queue.get()
                    if url in visited or depth > cfg.get("max_depth", 2):
                        queue.task_done()
                        continue
                    visited.add(url)
                    try:
                        await page.goto(url, wait_until="networkidle")
                        await asyncio.sleep(cfg.get("crawl", {}).get("wait_after_load", 1.0))
                        html = await page.content()
                        parsed = parse_html(url, html)

                        # combine with api hits and save
                        parsed['scrape_meta'] = {
                            "url": url,
                            "depth": depth,
                            "timestamp": int(time.time()),
                            "api_hits": api_hits.copy()
                        }

                        await json_writer.write(parsed)
                        await sqlite_store.insert(parsed)

                        # enqueue new links
                        for link in parsed.get("links", []):
                            normalized = normalize_url(link, url)
                            if normalized and should_follow(normalized, cfg, url):
                                await queue.put((normalized, depth+1))
                        api_hits.clear()
                        await asyncio.sleep(cfg.get("rate_limit", {}).get("delay_seconds", 0.5))
                    except Exception as e:
                        print("crawl error", url, e)
                    queue.task_done()
                await ctx.close()

        # Launch workers
        workers = [asyncio.create_task(worker(f"w{i}")) for i in range(concurrency)]
        await queue.join()
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

def normalize_url(href, base):
    try:
        href = href.strip()
        if href.startswith("javascript:") or href.startswith("mailto:"):
            return None
        joined = urljoin(base, href)
        clean, _ = urldefrag(joined)
        return clean
    except:
        return None

def should_follow(url, cfg, base_url):
    if not cfg.get("crawl", {}).get("follow_external", False):
        base_dom = urlparse(base_url).netloc
        if urlparse(url).netloc != base_dom:
            return False
    return True

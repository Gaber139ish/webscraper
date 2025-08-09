import asyncio
import time
from typing import Any, Dict, Optional, Set, Tuple, List
from urllib.parse import urljoin, urldefrag, urlparse
from crawler.browser_driver import BrowserDriver
from crawler.api_sniffer import attach_sniffer
from parser.html_parser import parse_html
from pipeline.cleaner import normalize_parsed
from crawler.robots import RobotsCache
from utils.logger import get_logger
from pathlib import Path

logger = get_logger(__name__)

async def infinite_scroll(page, max_iterations: int, wait_seconds: float) -> None:
    prev_height = -1
    for i in range(max_iterations):
        height = await page.evaluate("document.body.scrollHeight")
        if height == prev_height:
            break
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(wait_seconds)
        prev_height = height

async def click_more(page, selectors: List[str], max_clicks: int, wait_seconds: float) -> None:
    clicks = 0
    for sel in selectors:
        while clicks < max_clicks:
            try:
                btn = await page.query_selector(sel)
                if not btn:
                    break
                await btn.click()
                clicks += 1
                await asyncio.sleep(wait_seconds)
            except Exception:
                break

async def seed_from_forms(cfg: Dict[str, Any], drv: BrowserDriver, queue: asyncio.Queue) -> None:
    forms_cfg = (cfg.get("deep_crawl", {}) or {}).get("forms") or []
    if not forms_cfg:
        return
    ctx = await drv.new_context()
    page = await ctx.new_page()
    for action in forms_cfg:
        try:
            url = action.get("url")
            if not url:
                continue
            await page.goto(url, wait_until="domcontentloaded")
            queries: List[str] = []
            if action.get("queries"):
                queries = list(action["queries"])  # type: ignore
            elif action.get("queries_file"):
                try:
                    with open(action["queries_file"], "r", encoding="utf-8") as f:
                        queries = [line.strip() for line in f if line.strip()]
                except Exception as e:
                    logger.warning(f"Failed to read queries_file: {e}")
                    continue
            else:
                queries = [""]
            fields: Dict[str, str] = action.get("fields", {})
            submit_selector: Optional[str] = action.get("submit_selector")
            wait_after_submit: float = float(action.get("wait_after_submit", 1.0))
            max_results_per_query: int = int(action.get("max_results_per_query", 20))

            for q in queries:
                # fill fields (supports {query} placeholder)
                for selector, value in fields.items():
                    v = value.replace("{query}", q)
                    try:
                        el = await page.wait_for_selector(selector, timeout=3000)
                        await el.fill(v)
                    except Exception as e:
                        logger.debug(f"form fill failed for {selector}: {e}")
                if submit_selector:
                    try:
                        el = await page.query_selector(submit_selector)
                        if el:
                            await el.click()
                    except Exception:
                        pass
                await asyncio.sleep(wait_after_submit)
                html = await page.content()
                parsed = parse_html(page.url, html)
                # enqueue top results links
                count = 0
                for link in parsed.get("links", []):
                    normalized = normalize_url(link, page.url)
                    if normalized:
                        await queue.put((normalized, 0))
                        count += 1
                        if count >= max_results_per_query:
                            break
        except Exception as e:
            logger.warning(f"form seed error: {e}")
    await ctx.close()

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

    headless: bool = bool(cfg.get("headless", True))
    proxy = cfg.get("proxy")

    allow_domains = set(cfg.get("crawl", {}).get("allow_domains", []) or [])
    deny_domains = set(cfg.get("crawl", {}).get("deny_domains", []) or [])
    deny_extensions = set(cfg.get("crawl", {}).get("deny_extensions", [
        ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico",
        ".pdf", ".zip", ".gz", ".tar", ".rar", ".7z", ".mp3", ".mp4"
    ]))

    per_domain_delay = float(cfg.get("rate_limit", {}).get("per_domain_delay_seconds", 0))
    per_domain_concurrency = int(cfg.get("rate_limit", {}).get("per_domain_concurrency", 0))

    domain_semaphores: Dict[str, asyncio.Semaphore] = {}
    domain_last_access: Dict[str, float] = {}
    domain_lock = asyncio.Lock()

    snapshots_dir = Path(cfg.get("output", {}).get("snapshots_dir", "exports/snapshots"))
    save_html = bool(cfg.get("crawl", {}).get("save_html_snapshot", False))
    save_screenshot = bool(cfg.get("crawl", {}).get("save_screenshot", False))
    if save_html or save_screenshot:
        snapshots_dir.mkdir(parents=True, exist_ok=True)

    deep_cfg = cfg.get("deep_crawl", {}) or {}
    infinite_cfg = deep_cfg.get("infinite_scroll", {}) or {}
    click_more_selectors = deep_cfg.get("click_more_selectors", []) or []

    async def acquire_domain_slot(domain: str):
        if per_domain_concurrency > 0:
            async with domain_lock:
                if domain not in domain_semaphores:
                    domain_semaphores[domain] = asyncio.Semaphore(per_domain_concurrency)
            sem = domain_semaphores[domain]
        else:
            sem = None

        if sem:
            await sem.acquire()
        if per_domain_delay > 0:
            now = time.time()
            async with domain_lock:
                last = domain_last_access.get(domain)
                if last is not None:
                    wait = per_domain_delay - (now - last)
                    if wait > 0:
                        await asyncio.sleep(wait)
                domain_last_access[domain] = time.time()
        return sem

    def release_domain_slot(domain: str, sem: Optional[asyncio.Semaphore]):
        if sem:
            sem.release()

    async with BrowserDriver(user_agent=cfg.get("user_agent"), headless=headless, proxy=proxy) as drv:
        # seed from configured forms before normal crawl
        await seed_from_forms(cfg, drv, queue)

        async def worker(name: str) -> None:
            ctx = await drv.new_context()
            page = await ctx.new_page()
            api_hits = []

            async def on_api(data):
                api_hits.append(data)

            if cfg.get("crawl", {}).get("intercept_api", True):
                await attach_sniffer(page, on_api)

            max_retries: int = int(cfg.get("crawl", {}).get("max_retries", 2))
            backoff_base: float = float(cfg.get("crawl", {}).get("backoff_base", 0.75))

            try:
                while True:
                    url, depth = await queue.get()
                    if url is None:
                        queue.task_done()
                        break

                    if (url in visited) or (depth is not None and depth > cfg.get("max_depth", 2)):
                        queue.task_done()
                        continue

                    parsed_url = urlparse(url)
                    dom = parsed_url.netloc
                    if allow_domains and dom not in allow_domains:
                        queue.task_done();
                        continue
                    if dom in deny_domains:
                        queue.task_done();
                        continue
                    for ext in deny_extensions:
                        if parsed_url.path.lower().endswith(ext):
                            queue.task_done();
                            break
                    else:
                        pass
                    if queue._unfinished_tasks and parsed_url.path.lower().endswith(tuple(deny_extensions)):
                        continue

                    visited.add(url)

                    domain_sem = await acquire_domain_slot(dom)
                    try:
                        logger.info(f"[{name}] Visiting {url} (depth={depth})")
                        attempt = 0
                        while True:
                            try:
                                await page.goto(url, wait_until="networkidle")
                                break
                            except Exception as nav_err:
                                if attempt >= max_retries:
                                    raise nav_err
                                sleep_s = backoff_base * (2 ** attempt)
                                logger.warning(f"[{name}] goto failed (attempt {attempt+1}/{max_retries+1}): {nav_err}; retrying in {sleep_s:.2f}s")
                                await asyncio.sleep(sleep_s)
                                attempt += 1

                        await asyncio.sleep(cfg.get("crawl", {}).get("wait_after_load", 1.0))

                        # deep crawl helpers
                        if infinite_cfg.get("enabled", False):
                            await infinite_scroll(page, int(infinite_cfg.get("max_iterations", 8)), float(infinite_cfg.get("wait_seconds", 0.8)))
                        if click_more_selectors:
                            await click_more(page, click_more_selectors, int(deep_cfg.get("max_clicks", 10)), float(deep_cfg.get("click_wait_seconds", 0.8)))

                        html = await page.content()
                        parsed = parse_html(url, html)

                        parsed['scrape_meta'] = {
                            "url": url,
                            "depth": depth,
                            "timestamp": int(time.time()),
                            "api_hits": api_hits.copy(),
                            "schema_version": "1.0"
                        }

                        parsed = normalize_parsed(parsed)
                        await json_writer.write(parsed)
                        await sqlite_store.insert(parsed)

                        ts = parsed['scrape_meta']["timestamp"]
                        base_name = f"{parsed_url.netloc}_{ts}"
                        if save_html:
                            (snapshots_dir / f"{base_name}.html").write_text(html, encoding="utf-8")
                        if save_screenshot:
                            try:
                                await page.screenshot(path=str(snapshots_dir / f"{base_name}.png"), full_page=True)
                            except Exception as ss_err:
                                logger.debug(f"screenshot failed: {ss_err}")

                        for link in parsed.get("links", []):
                            normalized = normalize_url(link, url)
                            if normalized and should_follow(normalized, cfg, url, robots):
                                await queue.put((normalized, (depth or 0) + 1))

                        api_hits.clear()
                        await asyncio.sleep(cfg.get("rate_limit", {}).get("delay_seconds", 0.5))
                    except Exception as e:
                        logger.error(f"[{name}] crawl error for {url}: {e}")
                    finally:
                        release_domain_slot(dom, domain_sem)
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
    allow_domains = set(cfg.get("crawl", {}).get("allow_domains", []) or [])
    deny_domains = set(cfg.get("crawl", {}).get("deny_domains", []) or [])
    dom = urlparse(url).netloc
    if allow_domains and dom not in allow_domains:
        return False
    if dom in deny_domains:
        return False
    if robots is not None:
        try:
            if not robots.is_allowed(url):
                return False
        except Exception:
            return False
    return True

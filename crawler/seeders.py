import asyncio
import httpx
from typing import List, Set
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET
import feedparser

async def fetch_text(url: str, client: httpx.AsyncClient) -> str | None:
    try:
        r = await client.get(url, timeout=20.0)
        r.raise_for_status()
        return r.text
    except Exception:
        return None

async def discover_sitemaps(base_url: str, client: httpx.AsyncClient) -> List[str]:
    # Try robots.txt first
    sitemaps: List[str] = []
    parsed = urlparse(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    text = await fetch_text(robots_url, client)
    if text:
        for line in text.splitlines():
            if line.lower().startswith("sitemap:"):
                sm = line.split(":", 1)[1].strip()
                sitemaps.append(sm)
    # Fallback common locations
    if not sitemaps:
        for path in ("/sitemap.xml", "/sitemap_index.xml"):
            sitemaps.append(f"{parsed.scheme}://{parsed.netloc}{path}")
    return sitemaps

def _parse_sitemap_index(xml_text: str) -> List[str]:
    out: List[str] = []
    try:
        root = ET.fromstring(xml_text)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        for loc in root.findall(".//sm:loc", ns):
            if loc.text:
                out.append(loc.text.strip())
    except Exception:
        pass
    return out

async def gather_from_sitemaps(base_url: str, max_urls: int = 2000) -> List[str]:
    results: List[str] = []
    seen: Set[str] = set()
    async with httpx.AsyncClient() as client:
        sitemaps = await discover_sitemaps(base_url, client)
        for sm_url in sitemaps:
            xml = await fetch_text(sm_url, client)
            if not xml:
                continue
            # Try as index
            child_sitemaps = _parse_sitemap_index(xml)
            if child_sitemaps:
                for c in child_sitemaps:
                    if len(results) >= max_urls:
                        break
                    xml2 = await fetch_text(c, client)
                    if not xml2:
                        continue
                    urls = _parse_urls_from_sitemap(xml2)
                    for u in urls:
                        if u not in seen:
                            seen.add(u)
                            results.append(u)
                            if len(results) >= max_urls:
                                break
            else:
                urls = _parse_urls_from_sitemap(xml)
                for u in urls:
                    if u not in seen:
                        seen.add(u)
                        results.append(u)
                        if len(results) >= max_urls:
                            break
    return results

def _parse_urls_from_sitemap(xml_text: str) -> List[str]:
    urls: List[str] = []
    try:
        root = ET.fromstring(xml_text)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        for loc in root.findall(".//sm:url/sm:loc", ns):
            if loc.text:
                urls.append(loc.text.strip())
    except Exception:
        pass
    return urls

async def gather_from_rss(feed_url: str, max_items: int = 500) -> List[str]:
    try:
        # feedparser is sync
        d = await asyncio.to_thread(feedparser.parse, feed_url)
        urls: List[str] = []
        for e in d.entries[:max_items]:
            link = getattr(e, 'link', None)
            if link:
                urls.append(link)
        return urls
    except Exception:
        return []
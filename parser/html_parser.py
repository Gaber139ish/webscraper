from bs4 import BeautifulSoup
from urllib.parse import urlparse

try:
    import trafilatura
except Exception:
    trafilatura = None


def text_only_bs(soup):
    for script in soup(["script", "style", "noscript", "iframe"]):
        script.extract()
    return soup.get_text(separator="\n", strip=True)


def parse_html(url, html):
    title = ""
    body_text = ""

    if trafilatura is not None:
        try:
            downloaded = trafilatura.extract(html, include_comments=False, include_tables=False, no_fallback=False)
            if downloaded:
                body_text = downloaded.strip()
            # title with BeautifulSoup fallback
            soup = BeautifulSoup(html, "lxml")
            title = (soup.title.string.strip() if soup.title else "")
        except Exception:
            soup = BeautifulSoup(html, "lxml")
            title = (soup.title.string.strip() if soup.title else "")
            body_text = text_only_bs(soup)
    else:
        soup = BeautifulSoup(html, "lxml")
        title = (soup.title.string.strip() if soup.title else "")
        body_text = text_only_bs(soup)

    # links and meta via BS
    soup = BeautifulSoup(html, "lxml")
    links = [a["href"] for a in soup.find_all("a", href=True)]
    meta = {}
    for m in soup.find_all("meta"):
        if m.get("name"):
            meta[m.get("name")] = m.get("content")
        elif m.get("property"):
            meta[m.get("property")] = m.get("content")

    domain = urlparse(url).netloc
    return {
        "url": url,
        "domain": domain,
        "title": title,
        "text": body_text,
        "links": links,
        "meta": meta
    }

from bs4 import BeautifulSoup
from urllib.parse import urlparse

def text_only(soup):
    for script in soup(["script", "style", "noscript", "iframe"]):
        script.extract()
    return soup.get_text(separator="\n", strip=True)

def parse_html(url, html):
    soup = BeautifulSoup(html, "lxml")
    title = (soup.title.string.strip() if soup.title else "")
    body_text = text_only(soup)
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        links.append(href)
    meta = {}
    for m in soup.find_all("meta"):
        if m.get("name"):
            meta[m.get("name")] = m.get("content")
        elif m.get("property"):
            meta[m.get("property")] = m.get("content")
    # simple content classification
    domain = urlparse(url).netloc
    return {
        "url": url,
        "domain": domain,
        "title": title,
        "text": body_text,
        "links": links,
        "meta": meta
    }

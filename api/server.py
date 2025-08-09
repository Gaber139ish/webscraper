from fastapi import FastAPI, Query
from typing import List, Optional
import aiosqlite
import json


app = FastAPI(title="Coiney Scraper API")

DB_PATH = "./exports/dataset.db"


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/pages", response_model=List[dict])
async def list_pages(
    domain: Optional[str] = None,
    q: Optional[str] = Query(None, description="Search in title or text"),
    limit: int = 50,
    offset: int = 0,
):
    where = []
    params: List[object] = []
    if domain:
        where.append("domain = ?")
        params.append(domain)
    if q:
        where.append("(title LIKE ? OR text LIKE ?)")
        like = f"%{q}%"
        params.extend([like, like])
    sql = "SELECT url, domain, title, text, meta, scrape_meta FROM pages"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    rows: List[dict] = []
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(sql, params) as cursor:
            async for url, dom, title, text, meta, scrape_meta in cursor:
                rows.append({
                    "url": url,
                    "domain": dom,
                    "title": title,
                    "text": text,
                    "meta": json.loads(meta or "{}"),
                    "scrape_meta": json.loads(scrape_meta or "{}"),
                })
    return rows

# To run: uvicorn api.server:app --reload --port 8000

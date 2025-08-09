import aiosqlite
import json
from typing import Any, Dict, Optional
from utils.logger import get_logger

logger = get_logger(__name__)

class SQLiteStore:
    def __init__(self, path: str):
        self.path = path
        self.db: Optional[aiosqlite.Connection] = None

    async def initialize(self) -> None:
        self.db = await aiosqlite.connect(self.path)
        await self.db.execute("""
        CREATE TABLE IF NOT EXISTS pages (
            id INTEGER PRIMARY KEY,
            url TEXT UNIQUE,
            domain TEXT,
            title TEXT,
            text TEXT,
            meta TEXT,
            scrape_meta TEXT
        )
        """)
        await self.db.execute("""
        CREATE INDEX IF NOT EXISTS idx_pages_domain ON pages(domain)
        """)
        await self.db.commit()

    async def insert(self, parsed: Dict[str, Any]) -> None:
        try:
            await self.db.execute("""
                INSERT OR IGNORE INTO pages (url, domain, title, text, meta, scrape_meta)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                parsed.get("url"),
                parsed.get("domain"),
                parsed.get("title"),
                parsed.get("text"),
                json.dumps(parsed.get("meta") or {}),
                json.dumps(parsed.get("scrape_meta") or {})
            ))
            await self.db.commit()
        except Exception as e:
            logger.error(f"sqlite insert error: {e}")

    async def close(self) -> None:
        if self.db is not None:
            await self.db.close()
            self.db = None

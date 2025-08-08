import aiosqlite
import json

class SQLiteStore:
    def __init__(self, path):
        self.path = path
        self.db = None

    async def initialize(self):
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
        # Add a helpful index for domain queries
        await self.db.execute("""
        CREATE INDEX IF NOT EXISTS idx_pages_domain ON pages(domain)
        """)
        await self.db.commit()

    async def insert(self, parsed):
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
            print("sqlite insert error:", e)

    async def has_url(self, url: str) -> bool:
        try:
            async with self.db.execute("SELECT 1 FROM pages WHERE url = ? LIMIT 1", (url,)) as cursor:
                row = await cursor.fetchone()
                return row is not None
        except Exception as e:
            print("sqlite has_url error:", e)
            return False

    async def close(self):
        if self.db is not None:
            await self.db.close()
            self.db = None

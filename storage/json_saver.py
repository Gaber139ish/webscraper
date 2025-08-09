import json
import aiofiles
import asyncio


class JSONLWriter:
    def __init__(self, path):
        self.path = path
        self.lock = asyncio.Lock()
        # ensure file exists
        open(self.path, "a").close()

    async def write(self, obj):
        async with self.lock:
            async with aiofiles.open(self.path, "a") as f:
                await f.write(json.dumps(obj, ensure_ascii=False) + "\n")

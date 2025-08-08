from playwright.async_api import async_playwright

class BrowserDriver:
    def __init__(self, user_agent=None, headless=True, proxy: str | None = None):
        self.user_agent = user_agent
        self.headless = headless
        self.proxy = proxy
        self.playwright = None
        self.browser = None

    async def __aenter__(self):
        self.playwright = await async_playwright().start()
        launch_kwargs = {"headless": self.headless}
        if self.proxy:
            launch_kwargs["proxy"] = {"server": self.proxy}
        self.browser = await self.playwright.chromium.launch(**launch_kwargs)
        return self

    async def new_context(self, user_agent: str | None = None):
        return await self.browser.new_context(user_agent=user_agent or self.user_agent or "")

    async def __aexit__(self, exc_type, exc, tb):
        await self.browser.close()
        await self.playwright.stop()

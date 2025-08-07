from playwright.async_api import async_playwright

class BrowserDriver:
    def __init__(self, user_agent=None, headless=True):
        self.user_agent = user_agent
        self.headless = headless
        self.playwright = None
        self.browser = None

    async def __aenter__(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=self.headless)
        return self

    async def new_context(self):
        return await self.browser.new_context(user_agent=self.user_agent or "")

    async def __aexit__(self, exc_type, exc, tb):
        await self.browser.close()
        await self.playwright.stop()

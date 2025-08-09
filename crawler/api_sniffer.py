from playwright.async_api import Page
from utils.logger import get_logger


logger = get_logger(__name__)


def is_api_request(request):
    rtype = request.resource_type
    url = request.url.lower()
    if rtype in ("xhr", "fetch"):
        return True
    if "/api/" in url or url.endswith(".json") or "graphql" in url:
        return True
    return False


async def attach_sniffer(page: Page, on_api):
    # on_api: coroutine func taking (req, resp_body_opt)
    async def handle_response(response):
        try:
            req = response.request
            if is_api_request(req):
                text = None
                try:
                    text = await response.text()
                except Exception:
                    text = None
                await on_api({
                    "url": req.url,
                    "method": req.method,
                    "headers": dict(req.headers),
                    "status": response.status,
                    "response_text": text,
                })
        except Exception as e:
            logger.debug(f"sniffer error: {e}")

    page.on("response", handle_response)

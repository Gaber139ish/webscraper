from crawler.frontend_scraper import should_follow, normalize_url


def test_should_follow_allow_deny():
    cfg = {"crawl": {"follow_external": True, "allow_domains": ["good.com"], "deny_domains": ["bad.com"]}}
    base = "https://good.com/page"
    assert should_follow("https://good.com/next", cfg, base, robots=None) is True
    assert should_follow("https://bad.com/next", cfg, base, robots=None) is False
    assert should_follow("https://other.com/next", cfg, base, robots=None) is False


def test_normalize_url_drops_mailto_js():
    base = "https://example.com/a/b"
    assert normalize_url("mailto:abc@example.com", base) is None
    assert normalize_url("javascript:void(0)", base) is None
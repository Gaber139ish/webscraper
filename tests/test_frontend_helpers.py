from crawler.frontend_scraper import normalize_url, should_follow


def test_normalize_url():
    base = "https://example.com/a/b"
    assert normalize_url("/c#frag", base) == "https://example.com/c"
    assert normalize_url("mailto:foo@example.com", base) is None
    assert normalize_url("javascript:void(0)", base) is None


def test_should_follow_same_domain_only():
    cfg = {"crawl": {"follow_external": False}}
    base = "https://example.com/page"
    assert should_follow("https://example.com/next", cfg, base, robots=None) is True
    assert should_follow("https://other.com/next", cfg, base, robots=None) is False

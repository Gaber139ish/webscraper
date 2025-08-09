from crawler.github_code_scraper import GitHubCodeScraper


def test_is_code_file_extensions():
    s = GitHubCodeScraper()
    assert s._is_code_file("foo.py") is True
    assert s._is_code_file("foo.txt") is False
from prometheus_client import start_http_server, Counter, Summary
import time
from typing import Callable

PAGES_CRAWLED = Counter('pages_crawled_total', 'Total pages crawled')
PAGES_SKIPPED = Counter('pages_skipped_total', 'Total pages skipped')
CRAWL_ERRORS = Counter('crawl_errors_total', 'Total crawl errors')
API_HITS = Counter('api_hits_total', 'Total API/XHR hits captured')
CRAWL_LATENCY = Summary('crawl_latency_seconds', 'Latency of page crawls')


def start_metrics_server(port: int = 8000):
    start_http_server(port)


def timed(fn: Callable):
    def wrapper(*args, **kwargs):
        start = time.time()
        try:
            return fn(*args, **kwargs)
        finally:
            CRAWL_LATENCY.observe(time.time() - start)
    return wrapper
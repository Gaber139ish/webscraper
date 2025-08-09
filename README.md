Coiney Scraper — Playwright-based crawler with XHR/API capture.

Quick start:

- Install deps:
  - `python -m pip install -r requirements.txt`
  - `python -m playwright install chromium`
- Configure `config.yaml`
- Run:
  - Crawl websites: `python cli.py --mode crawl`
  - GitHub code dataset: `python cli.py --mode github`
  - Both: `python cli.py --mode both`

Configuration (`config.yaml`):

- **start_urls**: list of seed URLs
- **max_depth**: crawl depth (default 2)
- **concurrency**: number of crawler workers (default 2)
- **user_agent**: UA string sent by the browser
- **headless**: run browser headless (default true). You can override via CLI `--no-headless`.
- **proxy**: optional Playwright proxy dict, e.g. `{ server: "http://host:port", username: "", password: "" }`
- **output.jsonl**: path to JSONL dataset
- **output.sqlite**: path to SQLite database
- **crawl.follow_external**: follow links to other domains (default false)
- **crawl.respect_robots**: respect robots.txt (default true)
- **crawl.wait_after_load**: seconds to wait after page load (default 1.0)
- **crawl.intercept_api**: capture XHR/fetch and GraphQL responses (default true)
- **crawl.max_retries**: navigation retries on failures (default 2)
- **crawl.backoff_base**: base seconds for exponential backoff (default 0.75)
- **rate_limit.delay_seconds**: delay between page visits per worker
- **github**: optional GitHub code scraping config. You can also set `GITHUB_TOKEN` env var.

Notes:
- Add proxies, rotating UA, CAPTCHA handlers, and legal checks for scale.
- For HuggingFace/LLM labeling, export JSONL and build dataset mapping later.
- Set `LOG_LEVEL=DEBUG` to see verbose logs.

Testing:

- Run tests locally: `pytest -q`

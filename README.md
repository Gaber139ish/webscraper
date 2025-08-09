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
- **output.snapshots_dir**: directory to store HTML/screenshot snapshots (default `exports/snapshots`)
- **crawl.follow_external**: follow links to other domains (default false)
- **crawl.respect_robots**: respect robots.txt (default true)
- **crawl.wait_after_load**: seconds to wait after page load (default 1.0)
- **crawl.intercept_api**: capture XHR/fetch and GraphQL responses (default true)
- **crawl.max_retries**: navigation retries on failures (default 2)
- **crawl.backoff_base**: base seconds for exponential backoff (default 0.75)
- **crawl.allow_domains / crawl.deny_domains**: optional domain allow/deny lists
- **crawl.deny_extensions**: list of path extensions to skip (images, archives, media)
- **crawl.save_html_snapshot / crawl.save_screenshot**: save HTML and/or screenshots per page
- **rate_limit.delay_seconds**: global delay between page visits per worker
- **rate_limit.per_domain_delay_seconds**: delay per domain
- **rate_limit.per_domain_concurrency**: concurrent requests per domain
- **github**: GitHub code scraping options. Alternatively set `GITHUB_TOKEN` env var.

API server:

- A small HTTP API is provided to query content in SQLite.
- Run: `uvicorn api.server:app --reload --port 8000`
- Endpoints:
  - `GET /health`
  - `GET /pages?domain=example.com&q=keyword&limit=50&offset=0`

Notes:
- Add proxies, rotating UA, CAPTCHA handlers, and legal checks for scale.
- For HuggingFace/LLM labeling, export JSONL and build dataset mapping later.
- Set `LOG_LEVEL=DEBUG` to see verbose logs.

Testing and quality:

- Run unit tests: `pytest -q`
- Lint: `flake8`
- Type-check: `mypy`

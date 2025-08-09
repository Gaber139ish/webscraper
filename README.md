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
- **deep_crawl.infinite_scroll**: auto-scroll pages to load content
- **deep_crawl.click_more_selectors**: CSS selectors to click “load more” buttons
- **deep_crawl.max_clicks / deep_crawl.click_wait_seconds**: click behavior tuning
- **deep_crawl.forms**: form seeding rules to explore behind search boxes
- **github**: GitHub code scraping options. Alternatively set `GITHUB_TOKEN` env var.

Deep web crawling:

- Enable infinite scroll: set `deep_crawl.infinite_scroll.enabled: true` and tune iterations/wait.
- Click “load more” buttons: add CSS selectors to `deep_crawl.click_more_selectors`.
- Form seeding: configure entries under `deep_crawl.forms` with `url`, `fields`, `queries` (or `queries_file`), and `submit_selector`. Captured links from results pages will be queued.
- Always ensure you comply with site terms and applicable laws. Use `respect_robots: true` and domain allow/deny lists.

API server:

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

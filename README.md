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
  - Build datasets after run: add `--build-datasets`

Notes:
- Add proxies, rotating UA, CAPTCHA handlers, and legal checks for scale.
- For HuggingFace/LLM labeling, export JSONL and build dataset mapping later.
- New:
  - Proxy & UA rotation via `proxies.playwright`, `proxies.httpx`, and `user_agents`.
  - Per-domain rate limiting via `rate_limit.per_domain_delays`.
  - Robots.txt respect toggle: `crawl.respect_robots` and crawl-delay support.
  - Trafilatura-based extraction with BS fallback.
  - Seeding via sitemaps/RSS: `--seed-sitemaps` and `--seed-rss <feeds...>`.
  - Datasets: language filtering, MinHash + optional SimHash dedup, token chunking, JSONL + Parquet outputs.
  - Code datasets: SPDX license detection (file headers) + repo-aware splits.
  - Metrics: Prometheus exporter (`--metrics-port 9464`).
  - Observability: `observability/docker-compose.yml` to run Prometheus (9090) + Grafana (3000) scraping `host.docker.internal:9464`.
  - Analytics: `analytics/duckdb_report.py` summarizes Parquet datasets.

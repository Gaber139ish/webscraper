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

Notes:
- Add proxies, rotating UA, CAPTCHA handlers, and legal checks for scale.
- For HuggingFace/LLM labeling, export JSONL and build dataset mapping later.

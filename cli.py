import asyncio
import yaml
import os
import argparse
from pathlib import Path

from crawler.frontend_scraper import run_crawl
from storage.json_saver import JSONLWriter
from storage.sqlite_db import SQLiteStore
from crawler.github_code_scraper import GitHubCodeScraper
from dataset.builder import build_datasets
from prom.metrics import start_metrics_server
from crawler.seeders import gather_from_sitemaps, gather_from_rss

CONFIG_PATH = "config.yaml"

async def run_github_mode(cfg, json_writer, sqlite_store):
    gh_cfg = cfg.get("github") or {}
    if not gh_cfg:
        return
    token = gh_cfg.get("token")
    proxies = cfg.get("proxies", {}).get("httpx")
    gh_scraper = GitHubCodeScraper(
        token=token,
        output_dir=gh_cfg.get("output_dir", "exports/github_code"),
        extensions=gh_cfg.get("extensions"),
        max_file_size=gh_cfg.get("max_file_size", 200_000),
        concurrency=gh_cfg.get("concurrency", 6),
        proxies=proxies
    )

    repos = await gh_scraper.search_repos(
        query=gh_cfg.get("query", "machine learning"),
        per_page=gh_cfg.get("per_page", 5),
        pages=gh_cfg.get("pages", 1)
    )
    for repo in repos:
        owner = repo["owner"]["login"]
        name = repo["name"]
        print(f"Processing {owner}/{name}")
        saved = await gh_scraper.repo_to_jsonl(owner, name, jsonl_path=cfg["output"]["jsonl"], max_files=gh_cfg.get("max_files_per_repo"))
        await sqlite_store.insert({
            "url": repo["html_url"],
            "domain": "github.com",
            "title": repo["full_name"],
            "text": repo.get("description") or "",
            "meta": {
                "stars": repo.get("stargazers_count"),
                "forks": repo.get("forks_count"),
                "languages_url": repo.get("languages_url")
            },
            "scrape_meta": {"source": "github_code_scraper", "files_saved": len(saved)}
        })

async def main():
    parser = argparse.ArgumentParser(description="Coiney Scraper CLI")
    parser.add_argument("--config", default=CONFIG_PATH, help="Path to config.yaml")
    parser.add_argument("--mode", choices=["crawl", "github", "both"], default="both", help="Which pipeline to run")
    parser.add_argument("--build-datasets", action="store_true", help="Build training datasets after scraping")
    parser.add_argument("--metrics-port", type=int, default=0, help="Start Prometheus metrics server on this port (0 disables)")
    parser.add_argument("--seed-sitemaps", action="store_true", help="Seed start URLs via sitemaps of given start_urls roots")
    parser.add_argument("--seed-rss", nargs='*', default=[], help="RSS/Atom feed URLs to seed from")
    args = parser.parse_args()

    with open(args.config, "r") as f:
        cfg = yaml.safe_load(f)

    os.makedirs("exports", exist_ok=True)

    if args.metrics_port and args.metrics_port > 0:
        start_metrics_server(args.metrics_port)

    # optional seeding
    if args.seed_sitemaps:
        extra = []
        for root in cfg.get("start_urls", []):
            urls = await gather_from_sitemaps(root)
            extra.extend(urls)
        cfg["start_urls"] = list(dict.fromkeys(cfg.get("start_urls", []) + extra))
    if args.seed_rss:
        extra = []
        for feed in args.seed_rss:
            extra.extend(await gather_from_rss(feed))
        cfg["start_urls"] = list(dict.fromkeys(cfg.get("start_urls", []) + extra))

    json_writer = JSONLWriter(cfg["output"]["jsonl"])
    sqlite_store = SQLiteStore(cfg["output"]["sqlite"]) 
    await sqlite_store.initialize()

    try:
        if args.mode in ("crawl", "both"):
            await run_crawl(cfg, json_writer, sqlite_store)
        if args.mode in ("github", "both") and cfg.get("github"):
            await run_github_mode(cfg, json_writer, sqlite_store)
        if args.build_datasets:
            build_datasets(cfg)
    finally:
        await sqlite_store.close()

if __name__ == "__main__":
    asyncio.run(main())

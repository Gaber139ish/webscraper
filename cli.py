import asyncio
import yaml
import os
import argparse
from pathlib import Path
from typing import Any, Dict

from crawler.frontend_scraper import run_crawl
from storage.json_saver import JSONLWriter
from storage.sqlite_db import SQLiteStore
from crawler.github_code_scraper import GitHubCodeScraper
from utils.logger import get_logger

CONFIG_PATH = "config.yaml"
logger = get_logger(__name__)

async def run_github_mode(cfg: Dict[str, Any], json_writer: JSONLWriter, sqlite_store: SQLiteStore):
    gh_cfg = cfg.get("github") or {}
    if not gh_cfg:
        return
    token = gh_cfg.get("token")
    gh_scraper = GitHubCodeScraper(
        token=token,
        output_dir=gh_cfg.get("output_dir", "exports/github_code"),
        extensions=gh_cfg.get("extensions"),
        max_file_size=gh_cfg.get("max_file_size", 200_000),
        concurrency=gh_cfg.get("concurrency", 6)
    )

    repos = await gh_scraper.search_repos(
        query=gh_cfg.get("query", "machine learning"),
        per_page=gh_cfg.get("per_page", 5),
        pages=gh_cfg.get("pages", 1)
    )
    for repo in repos:
        owner = repo["owner"]["login"]
        name = repo["name"]
        logger.info(f"Processing {owner}/{name}")
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
    parser.add_argument("--headless", dest="headless", action="store_true", default=True, help="Run browser headless (default)")
    parser.add_argument("--no-headless", dest="headless", action="store_false", help="Run browser with UI")
    args = parser.parse_args()

    with open(args.config, "r") as f:
        cfg = yaml.safe_load(f)

    # CLI overrides
    cfg["headless"] = args.headless

    os.makedirs("exports", exist_ok=True)

    json_writer = JSONLWriter(cfg["output"]["jsonl"])
    sqlite_store = SQLiteStore(cfg["output"]["sqlite"]) 
    await sqlite_store.initialize()

    try:
        if args.mode in ("crawl", "both"):
            await run_crawl(cfg, json_writer, sqlite_store)
        if args.mode in ("github", "both") and cfg.get("github"):
            await run_github_mode(cfg, json_writer, sqlite_store)
    finally:
        await sqlite_store.close()

if __name__ == "__main__":
    asyncio.run(main())

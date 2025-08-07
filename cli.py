import asyncio
import yaml
import os
from crawler.frontend_scraper import run_crawl
from storage.json_saver import JSONLWriter
from storage.sqlite_db import SQLiteStore
from pathlib import Path

CONFIG_PATH = "config.yaml"

async def main():
    with open(CONFIG_PATH, "r") as f:
        cfg = yaml.safe_load(f)

    os.makedirs("exports", exist_ok=True)

    json_writer = JSONLWriter(cfg["output"]["jsonl"])
    sqlite = SQLiteStore(cfg["output"]["sqlite"])
    await sqlite.initialize()

    await run_crawl(cfg, json_writer, sqlite)

# after you create json_writer, sqlite etc.
from crawler.github_code_scraper import GitHubCodeScraper

# config.yaml should have a "github" section (see below)
if "github" in cfg:
    gh_cfg = cfg["github"]
    token = gh_cfg.get("token")
    gh_scraper = GitHubCodeScraper(
        token=token,
        output_dir=gh_cfg.get("output_dir", "exports/github_code"),
        extensions=gh_cfg.get("extensions"),
        max_file_size=gh_cfg.get("max_file_size", 200_000),
        concurrency=gh_cfg.get("concurrency", 6)
    )

    # Option A: search then download top repos
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
        # also insert a small repo-level record into sqlite if you want:
        await sqlite.insert({
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

if __name__ == "__main__":
    asyncio.run(main())

import os
import asyncio
import httpx
import time
import base64
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from utils.logger import get_logger

DEFAULT_EXTENSIONS = [
    ".py",
    ".js",
    ".ts",
    ".java",
    ".cpp",
    ".c",
    ".h",
    ".cs",
    ".go",
    ".rs",
    ".sh",
    ".rb",
    ".php",
    ".swift",
    ".kt",
    ".m",
    ".scala",
    ".lua",
    ".r",
    ".jl",
    ".pl",
    ".sql",
    ".json",
    ".yaml",
    ".yml",
]

logger = get_logger(__name__)


class GitHubCodeScraper:
    def __init__(
        self,
        token: Optional[str] = None,
        output_dir: str = "exports/github_code",
        extensions: Optional[List[str]] = None,
        max_file_size: int = 200_000,
        concurrency: int = 6,
    ):
        """
        max_file_size: bytes (default 200 KB)
        extensions: list of extensions to keep; None -> DEFAULT_EXTENSIONS
        concurrency: number of simultaneous downloads
        """
        self.token = token or os.getenv("GITHUB_TOKEN")
        self.base_api = "https://api.github.com"
        self.headers: Dict[str, str] = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "CoineyScraper/1.0",
        }
        if self.token:
            self.headers["Authorization"] = f"token {self.token}"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.extensions: Set[str] = set(extensions or DEFAULT_EXTENSIONS)
        self.max_file_size = max_file_size
        self.semaphore = asyncio.Semaphore(concurrency)

    async def _get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        async with httpx.AsyncClient(headers=self.headers, timeout=30.0) as client:
            r = await client.get(url, params=params)
            if r.status_code == 403 and "X-RateLimit-Reset" in r.headers:
                reset_time = int(r.headers["X-RateLimit-Reset"])
                sleep_time = reset_time - int(time.time()) + 1
                logger.warning(f"[GitHub] rate limited; sleeping {sleep_time}s")
                await asyncio.sleep(max(sleep_time, 1))
                return await self._get_json(url, params=params)
            r.raise_for_status()
            data: Dict[str, Any] = r.json()
            return data

    async def _get_text(self, url: str) -> str:
        async with httpx.AsyncClient(headers=self.headers, timeout=60.0) as client:
            r = await client.get(url)
            if r.status_code == 403 and "X-RateLimit-Reset" in r.headers:
                reset_time = int(r.headers["X-RateLimit-Reset"])
                sleep_time = reset_time - int(time.time()) + 1
                logger.warning(f"[GitHub] rate limited; sleeping {sleep_time}s")
                await asyncio.sleep(max(sleep_time, 1))
                return await self._get_text(url)
            r.raise_for_status()
            return r.text

    async def search_repos(
        self,
        query: str = "language:python",
        per_page: int = 10,
        pages: int = 1,
    ) -> List[Dict[str, Any]]:
        url = f"{self.base_api}/search/repositories"
        results: List[Dict[str, Any]] = []
        for page in range(1, pages + 1):
            data = await self._get_json(
                url,
                params={
                    "q": query,
                    "sort": "stars",
                    "order": "desc",
                    "per_page": per_page,
                    "page": page,
                },
            )
            for it in data.get("items", []):
                results.append(it)
        return results

    async def get_repo_tree(self, owner: str, repo: str, branch: Optional[str] = None):
        """
        Use the git/trees API with recursive=1 to list all files.
        """
        meta = await self._get_json(f"{self.base_api}/repos/{owner}/{repo}")
        default_branch = branch or meta.get("default_branch", "main")
        tree_url = f"{self.base_api}/repos/{owner}/{repo}/git/trees/{default_branch}"
        data = await self._get_json(tree_url, params={"recursive": "1"})
        return data.get("tree", []), default_branch, meta

    def _is_code_file(self, path: str) -> bool:
        for ext in self.extensions:
            if path.lower().endswith(ext):
                return True
        return False

    async def _download_file_raw(self, owner: str, repo: str, branch: str, path: str) -> Optional[str]:
        """
        Use raw.githubusercontent.com for direct raw file content retrieval.
        """
        raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"
        try:
            text = await self._get_text(raw_url)
            return text
        except Exception:
            try:
                contents = await self._get_json(
                    f"{self.base_api}/repos/{owner}/{repo}/contents/{path}",
                    params={"ref": branch},
                )
                if contents and contents.get("encoding") == "base64":
                    raw = base64.b64decode(contents["content"]).decode(
                        "utf-8",
                        errors="replace",
                    )
                    return raw
                return None
            except Exception:
                return None

    async def download_repo_code(
        self,
        owner: str,
        repo: str,
        dest_folder: Optional[str] = None,
        max_files: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Download files from repo that match extensions and are under max_file_size.
        Returns a list of metadata dicts for saved files.
        """
        tree, branch, repo_meta = await self.get_repo_tree(owner, repo)
        files = [t for t in tree if t.get("type") == "blob" and self._is_code_file(t.get("path", ""))]
        logger.info(f"[GitHubCodeScraper] {owner}/{repo}: {len(files)} candidate files (ext filter)")

        if max_files:
            files = files[:max_files]

        saved: List[Dict[str, Any]] = []
        dest_root = Path(dest_folder or self.output_dir) / owner / repo
        dest_root.mkdir(parents=True, exist_ok=True)

        async def worker(entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            async with self.semaphore:
                path = entry["path"]
                size = entry.get("size", 0)
                if size and size > self.max_file_size:
                    return None
                text = await self._download_file_raw(owner, repo, branch, path)
                if not text:
                    return None
                if any(ord(c) == 0 for c in text[:2000]):
                    return None
                file_path = dest_root / path
                file_path.parent.mkdir(parents=True, exist_ok=True)
                try:
                    file_path.write_text(text, encoding="utf-8", errors="replace")
                except Exception:
                    file_path.write_bytes(text.encode("utf-8", errors="replace"))
                meta: Dict[str, Any] = {
                    "repo": f"{owner}/{repo}",
                    "owner": owner,
                    "repo_name": repo,
                    "path": path,
                    "size": len(text.encode("utf-8")),
                    "branch": branch,
                    "raw_url": f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}",
                    "repo_meta": {
                        "stars": repo_meta.get("stargazers_count"),
                        "license": repo_meta.get("license", {}),
                    },
                }
                meta_path = file_path.with_suffix(file_path.suffix + ".json")
                meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
                return {"meta": meta, "text": text}

        coros = [worker(entry) for entry in files]
        for fut in asyncio.as_completed(coros):
            try:
                res = await fut
                if res:
                    saved.append(res)
            except Exception as e:
                logger.error(f"download worker error: {e}")
        logger.info(f"[GitHubCodeScraper] saved {len(saved)} files for {owner}/{repo}")
        return saved

    async def repo_to_jsonl(
        self,
        owner: str,
        repo: str,
        jsonl_path: Optional[str] = None,
        max_files: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Downloads and writes code entries to JSONL at jsonl_path.
        """
        path_obj = Path(jsonl_path) if jsonl_path else (self.output_dir / "code_dataset.jsonl")
        saved = await self.download_repo_code(owner, repo, max_files=max_files)
        async with asyncio.Lock():
            with open(path_obj, "a", encoding="utf-8") as f:
                for entry in saved:
                    out = {
                        "repo": entry["meta"]["repo"],
                        "path": entry["meta"]["path"],
                        "branch": entry["meta"]["branch"],
                        "size": entry["meta"]["size"],
                        "raw_url": entry["meta"]["raw_url"],
                        "text": entry["text"],
                    }
                    f.write(json.dumps(out, ensure_ascii=False) + "\n")
        return saved

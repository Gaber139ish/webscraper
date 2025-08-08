import os
import json
import random
from pathlib import Path
from typing import Iterable

from pipeline.cleaner import clean_text

RANDOM_SEED = 42


def _iter_jsonl(path: Path) -> Iterable[dict]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def _write_jsonl(path: Path, rows: Iterable[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _basic_card(name: str, total: int, notes: str = "") -> str:
    return f"""
# {name}

Auto-generated dataset for AI training.

- Instances: {total}
- Format: JSON Lines (`.jsonl`)
- Fields:
  - text: string
  - meta: object (source metadata)

{notes}
""".strip()


def build_web_dataset(cfg: dict):
    src_jsonl = Path(cfg["output"]["jsonl"])  # unified crawl/code jsonl
    ds_cfg = cfg.get("datasets", {})
    out_dir = Path(ds_cfg.get("output_dir", "exports/datasets")) / ds_cfg.get("web_dataset_name", "web_text")
    val_ratio = float(ds_cfg.get("val_ratio", 0.05))
    min_len = int(ds_cfg.get("min_text_length", 200))

    rows = []
    for obj in _iter_jsonl(src_jsonl) or []:
        text = clean_text(obj.get("text", ""))
        if len(text) < min_len:
            continue
        rows.append({
            "text": text,
            "meta": {
                "url": obj.get("url"),
                "domain": obj.get("domain"),
                "title": obj.get("title"),
            }
        })

    random.Random(RANDOM_SEED).shuffle(rows)
    n_total = len(rows)
    n_val = max(1, int(n_total * val_ratio)) if n_total > 0 else 0
    val = rows[:n_val]
    train = rows[n_val:]

    _write_jsonl(out_dir / "train.jsonl", train)
    _write_jsonl(out_dir / "valid.jsonl", val)
    (out_dir / "README.md").write_text(_basic_card("Web Text Dataset", n_total), encoding="utf-8")


def build_code_dataset(cfg: dict):
    # The GitHub scraper appends code entries into the same jsonl; filter by presence of fields
    src_jsonl = Path(cfg["output"]["jsonl"])  # unified output
    ds_cfg = cfg.get("datasets", {})
    out_dir = Path(ds_cfg.get("output_dir", "exports/datasets")) / ds_cfg.get("code_dataset_name", "code_text")
    val_ratio = float(ds_cfg.get("val_ratio", 0.05))

    rows = []
    for obj in _iter_jsonl(src_jsonl) or []:
        # Heuristic: code entries from GitHub writer include raw_url and path
        if "raw_url" in obj and "path" in obj and "text" in obj:
            code = obj.get("text")
            if not code:
                continue
            rows.append({
                "text": code,
                "meta": {
                    "repo": obj.get("repo"),
                    "path": obj.get("path"),
                    "raw_url": obj.get("raw_url"),
                }
            })

    random.Random(RANDOM_SEED).shuffle(rows)
    n_total = len(rows)
    n_val = max(1, int(n_total * val_ratio)) if n_total > 0 else 0
    val = rows[:n_val]
    train = rows[n_val:]

    _write_jsonl(out_dir / "train.jsonl", train)
    _write_jsonl(out_dir / "valid.jsonl", val)
    (out_dir / "README.md").write_text(_basic_card("Code Text Dataset", n_total), encoding="utf-8")


def build_datasets(cfg: dict):
    build_web_dataset(cfg)
    build_code_dataset(cfg)
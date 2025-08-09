import os
import json
import random
from pathlib import Path
from typing import Iterable, List, Dict

from pipeline.cleaner import clean_text
from pipeline.lang_filter import keep_text_by_language
from pipeline.simhash_dedupe import dedupe_simhash

# Optional heavy deps guarded
try:
    import tiktoken
except Exception:
    tiktoken = None

try:
    from datasketch import MinHash, MinHashLSH
except Exception:
    MinHash = None
    MinHashLSH = None

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception:
    pa = None
    pq = None

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


def _write_parquet(path: Path, rows: List[dict]):
    if not rows or pa is None or pq is None:
        return
    table = pa.table({
        "text": pa.array([r.get("text", "") for r in rows], pa.string()),
        "meta": pa.array([json.dumps(r.get("meta", {}), ensure_ascii=False) for r in rows], pa.string()),
    })
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, str(path))


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


def _minhash(text: str, num_perm: int = 64):
    mh = MinHash(num_perm=num_perm)
    for token in set(text.split()):
        mh.update(token.encode("utf-8"))
    return mh


def _dedup_lsh(rows: List[dict], threshold: float = 0.9) -> List[dict]:
    if MinHash is None or MinHashLSH is None:
        return rows
    lsh = MinHashLSH(threshold=threshold, num_perm=64)
    kept: List[dict] = []
    for r in rows:
        text = r.get("text", "")
        mh = _minhash(text)
        dup = False
        for _ in lsh.query(mh):
            dup = True
            break
        if not dup:
            lsh.insert(str(len(kept)), mh)
            kept.append(r)
    return kept


def _chunk_text(text: str, max_tokens: int = 600, model: str = "gpt2") -> List[str]:
    if tiktoken is None:
        words = text.split()
        chunks = []
        for i in range(0, len(words), max_tokens):
            chunks.append(" ".join(words[i:i+max_tokens]))
        return chunks
    enc = tiktoken.get_encoding(model)
    toks = enc.encode(text)
    chunks = []
    for i in range(0, len(toks), max_tokens):
        chunk = enc.decode(toks[i:i+max_tokens])
        chunks.append(chunk)
    return chunks


def _detect_spdx(text: str) -> str:
    # Very lightweight SPDX tag detection in headers
    # SPDX-License-Identifier: MIT
    import re
    m = re.search(r"SPDX-License-Identifier:\s*([A-Za-z0-9+.-]+)", text)
    return (m.group(1).lower() if m else "")


def build_web_dataset(cfg: dict):
    src_jsonl = Path(cfg["output"]["jsonl"])  # unified crawl/code jsonl
    ds_cfg = cfg.get("datasets", {})
    out_dir = Path(ds_cfg.get("output_dir", "exports/datasets")) / ds_cfg.get("web_dataset_name", "web_text")
    val_ratio = float(ds_cfg.get("val_ratio", 0.05))
    min_len = int(ds_cfg.get("min_text_length", 200))
    allowed_langs = ds_cfg.get("languages_allowed")
    chunk_tokens = int(ds_cfg.get("chunk_tokens", 0))
    use_simhash = bool(ds_cfg.get("use_simhash", False))
    simhash_bits = int(ds_cfg.get("simhash_bits", 64))
    simhash_hamming = int(ds_cfg.get("simhash_hamming_threshold", 3))

    rows = []
    for obj in _iter_jsonl(src_jsonl) or []:
        if obj.get("repo") and obj.get("raw_url"):
            continue
        text = clean_text(obj.get("text", ""))
        if len(text) < min_len:
            continue
        if not keep_text_by_language(text, allowed_langs):
            continue
        if chunk_tokens and chunk_tokens > 0:
            for ch in _chunk_text(text, max_tokens=chunk_tokens):
                if len(ch) < min_len:
                    continue
                rows.append({"text": ch, "meta": {"url": obj.get("url"), "domain": obj.get("domain"), "title": obj.get("title")}})
        else:
            rows.append({
                "text": text,
                "meta": {"url": obj.get("url"), "domain": obj.get("domain"), "title": obj.get("title")}
            })

    # Dedup: MinHash LSH first, then optional SimHash
    rows = _dedup_lsh(rows)
    if use_simhash:
        rows = dedupe_simhash(rows, f=simhash_bits, threshold=simhash_hamming)

    random.Random(RANDOM_SEED).shuffle(rows)
    n_total = len(rows)
    n_val = max(1, int(n_total * val_ratio)) if n_total > 0 else 0
    val = rows[:n_val]
    train = rows[n_val:]

    _write_jsonl(out_dir / "train.jsonl", train)
    _write_jsonl(out_dir / "valid.jsonl", val)
    _write_parquet(out_dir / "train.parquet", train)
    _write_parquet(out_dir / "valid.parquet", val)
    (out_dir / "README.md").write_text(_basic_card("Web Text Dataset", n_total), encoding="utf-8")


def build_code_dataset(cfg: dict):
    src_jsonl = Path(cfg["output"]["jsonl"])  # unified output
    ds_cfg = cfg.get("datasets", {})
    out_dir = Path(ds_cfg.get("output_dir", "exports/datasets")) / ds_cfg.get("code_dataset_name", "code_text")
    val_ratio = float(ds_cfg.get("val_ratio", 0.05))
    chunk_tokens = int(ds_cfg.get("chunk_tokens_code", 0))
    allowed_licenses = set(ds_cfg.get("licenses_allowed", ["mit", "apache-2.0", "bsd-3-clause", "bsd-2-clause", "mpl-2.0"]))
    use_simhash = bool(ds_cfg.get("use_simhash", False))
    simhash_bits = int(ds_cfg.get("simhash_bits", 64))
    simhash_hamming = int(ds_cfg.get("simhash_hamming_threshold", 3))

    repo_to_rows: Dict[str, List[dict]] = {}

    for obj in _iter_jsonl(src_jsonl) or []:
        if not ("raw_url" in obj and "path" in obj and "text" in obj):
            continue
        # license detection order: obj.license/meta.license -> SPDX tag in code -> allow empty
        license_key = (obj.get("license") or obj.get("meta", {}).get("license") or "").lower()
        if not license_key:
            license_key = _detect_spdx(obj.get("text", ""))
        if license_key and license_key not in allowed_licenses:
            continue
        code = obj.get("text")
        if not code:
            continue
        repo = obj.get("repo") or obj.get("meta", {}).get("repo") or ""
        if chunk_tokens and chunk_tokens > 0:
            for ch in _chunk_text(code, max_tokens=chunk_tokens):
                repo_to_rows.setdefault(repo, []).append({"text": ch, "meta": {"repo": repo, "path": obj.get("path"), "raw_url": obj.get("raw_url"), "license": license_key}})
        else:
            repo_to_rows.setdefault(repo, []).append({"text": code, "meta": {"repo": repo, "path": obj.get("path"), "raw_url": obj.get("raw_url"), "license": license_key}})

    repos = list(repo_to_rows.keys())
    random.Random(RANDOM_SEED).shuffle(repos)
    n_repos_val = max(1, int(len(repos) * val_ratio)) if repos else 0
    repos_val = set(repos[:n_repos_val])

    train, val = [], []
    for r, lst in repo_to_rows.items():
        (val if r in repos_val else train).extend(lst)

    # Dedup
    train = _dedup_lsh(train)
    val = _dedup_lsh(val)
    if use_simhash:
        train = dedupe_simhash(train, f=simhash_bits, threshold=simhash_hamming)
        val = dedupe_simhash(val, f=simhash_bits, threshold=simhash_hamming)

    _write_jsonl(out_dir / "train.jsonl", train)
    _write_jsonl(out_dir / "valid.jsonl", val)
    _write_parquet(out_dir / "train.parquet", train)
    _write_parquet(out_dir / "valid.parquet", val)
    (out_dir / "README.md").write_text(_basic_card("Code Text Dataset", len(train) + len(val)), encoding="utf-8")


def build_datasets(cfg: dict):
    build_web_dataset(cfg)
    build_code_dataset(cfg)
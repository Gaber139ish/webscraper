import json
from pathlib import Path
from collections import Counter


def jsonl_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as f:
        return sum(1 for _ in f)


def length_stats(path: Path) -> dict:
    lengths = []
    if not path.exists():
        return {"count": 0, "avg_len": 0, "p50": 0, "p90": 0}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
            except Exception:
                continue
            text = obj.get("text", "")
            lengths.append(len(text))
    if not lengths:
        return {"count": 0, "avg_len": 0, "p50": 0, "p90": 0}
    lengths.sort()
    n = len(lengths)
    return {
        "count": n,
        "avg_len": sum(lengths) / n,
        "p50": lengths[int(0.5 * (n - 1))],
        "p90": lengths[int(0.9 * (n - 1))]
    }
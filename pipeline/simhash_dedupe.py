from typing import List, Tuple

try:
    from simhash import Simhash
except Exception:
    Simhash = None


def compute_simhash(text: str, f: int = 64) -> int:
    if Simhash is None:
        # fallback: naive hash masked to f bits
        return hash(text) & ((1 << f) - 1)
    return Simhash(text).value & ((1 << f) - 1)


def hamming_distance(x: int, y: int) -> int:
    return (x ^ y).bit_count()


def dedupe_simhash(rows: List[dict], f: int = 64, threshold: int = 3) -> List[dict]:
    if not rows:
        return rows
    seen: List[Tuple[int, int]] = []  # list of (simhash, idx_kept)
    kept: List[dict] = []
    for r in rows:
        s = compute_simhash(r.get("text", ""), f=f)
        is_dup = any(hamming_distance(s, sh) <= threshold for sh, _ in seen)
        if not is_dup:
            seen.append((s, len(kept)))
            kept.append(r)
    return kept
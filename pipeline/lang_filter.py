import langid
from typing import Optional

def detect_language(text: str) -> str:
    if not text:
        return ""
    lang, _ = langid.classify(text)
    return lang


def keep_text_by_language(text: str, allowed: Optional[list[str]] = None) -> bool:
    if not allowed:
        return True
    lang = detect_language(text)
    return lang in set(allowed)
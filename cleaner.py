import re

def clean_text(text):
    if not text:
        return ""
    # remove excessive whitespace
    t = re.sub(r'\s+', ' ', text)
    # remove long gibberish control chars
    t = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]+', ' ', t)
    return t.strip()

def normalize_parsed(parsed):
    parsed["text"] = clean_text(parsed.get("text", ""))
    parsed["title"] = (parsed.get("title") or "").strip()
    return parsed

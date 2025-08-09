import pytest
from pipeline.cleaner import clean_text, normalize_parsed


def test_clean_text_basic():
    assert clean_text("  a  b\n\n c\t") == "a b c"


def test_clean_text_controls():
    dirty = "a\x00b\x07c\x1Fd"
    assert clean_text(dirty) == "a b c d"


def test_normalize_parsed():
    parsed = {"text": "  hello\nworld ", "title": "  Title  "}
    out = normalize_parsed(parsed)
    assert out["text"] == "hello world"
    assert out["title"] == "Title"
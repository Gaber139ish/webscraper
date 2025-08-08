import json
import shutil
from pathlib import Path
import unittest

from dataset.builder import build_datasets

class TestDatasetBuilder(unittest.TestCase):
    def setUp(self):
        self.tmp = Path("/workspace/exports/test_datasets").absolute()
        if self.tmp.exists():
            shutil.rmtree(self.tmp)
        self.tmp.mkdir(parents=True)
        self.jsonl_path = Path("/workspace/exports/test_input.jsonl")
        self.jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        # create a mix of web and code entries
        rows = [
            {"url": "https://a", "domain": "a", "title": "t", "text": "x" * 300},
            {"url": "https://b", "domain": "b", "title": "t", "text": "y" * 250},
            {"repo": "o/r", "path": "p.py", "raw_url": "https://raw", "text": "print('hi')"},
        ]
        with self.jsonl_path.open("w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
        self.cfg = {
            "output": {"jsonl": str(self.jsonl_path), "sqlite": str(self.tmp / "db.sqlite")},
            "datasets": {
                "output_dir": str(self.tmp),
                "web_dataset_name": "web",
                "code_dataset_name": "code",
                "val_ratio": 0.2,
                "min_text_length": 200,
            }
        }

    def tearDown(self):
        if self.tmp.exists():
            shutil.rmtree(self.tmp)
        if self.jsonl_path.exists():
            self.jsonl_path.unlink()

    def test_build_datasets(self):
        build_datasets(self.cfg)
        web_train = self.tmp / "web" / "train.jsonl"
        web_valid = self.tmp / "web" / "valid.jsonl"
        code_train = self.tmp / "code" / "train.jsonl"
        code_valid = self.tmp / "code" / "valid.jsonl"
        self.assertTrue(web_train.exists())
        self.assertTrue(web_valid.exists())
        self.assertTrue(code_train.exists())
        self.assertTrue(code_valid.exists())
        # web: 2 eligible, with 20% val -> 1 val, 1 train
        self.assertEqual(sum(1 for _ in web_train.open()), 1)
        self.assertEqual(sum(1 for _ in web_valid.open()), 1)
        # code: 1 eligible -> val min 1
        self.assertEqual(sum(1 for _ in code_train.open()), 0)
        self.assertEqual(sum(1 for _ in code_valid.open()), 1)

if __name__ == "__main__":
    unittest.main()
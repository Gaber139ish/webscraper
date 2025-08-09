import duckdb
from pathlib import Path
import json


def run_report(datasets_dir: str = "exports/datasets"):
    con = duckdb.connect()
    datasets = Path(datasets_dir)

    def report_one(name: str):
        train = datasets / name / "train.parquet"
        valid = datasets / name / "valid.parquet"
        if not train.exists() and not valid.exists():
            print(f"No parquet files for {name}")
            return
        con.execute("CREATE OR REPLACE VIEW data AS SELECT * FROM read_parquet(?, union_by_name=true)", [str(train if train.exists() else valid)])
        if valid.exists() and train.exists():
            con.execute("CREATE OR REPLACE VIEW data AS SELECT * FROM read_parquet(?, ?, union_by_name=true)", [str(train), str(valid)])
        total = con.execute("SELECT count(*) FROM data").fetchone()[0]
        avg_len = con.execute("SELECT avg(length(text)) FROM data").fetchone()[0]
        print(f"Dataset {name}: rows={total}, avg_len={avg_len:.1f}")

    for name in [p.name for p in datasets.iterdir() if p.is_dir()]:
        report_one(name)

if __name__ == "__main__":
    run_report()
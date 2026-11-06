"""Generate reservoir answers with the pinned real DuckDB Windows wheels."""
import argparse
import hashlib
import json
import platform
from pathlib import Path
import duckdb

parser = argparse.ArgumentParser()
parser.add_argument("--version", choices=("1.5.5", "2.0.0"), required=True)
args = parser.parse_args()
if duckdb.__version__ != args.version:
    raise SystemExit(f"expected {args.version}, got {duckdb.__version__}")
if platform.system() != "Windows":
    raise SystemExit("this fixture pins MSVC std::shuffle; generate another profile on Linux")
con = duckdb.connect()
con.execute("SET threads=1")
cases = []
for population, amounts in (
    (0, ("0 ROWS", "17 ROWS", "0%", "17%", "100%")),
    (7, ("0 ROWS", "1 ROWS", "17 ROWS", "0%", "50%", "100%")),
    (8193, ("17 ROWS", "2051 ROWS", "9000 ROWS", "0.01%", "17%", "100%")),
    (100001, ("17 ROWS", "0.01%", "17%")),
    (200001, ("17 ROWS", "2051 ROWS", "17%")),
):
    for amount in amounts:
        for seed in (0, 42, 999):
            suffix = f"reservoir({amount}) REPEATABLE({seed})"
            ids = [r[0] for r in con.execute(f"SELECT range FROM range({population}) USING SAMPLE {suffix}").fetchall()]
            digest = lambda values: hashlib.sha256("".join(f"{v}\n" for v in values).encode()).hexdigest()
            cases.append(dict(population=population, suffix=suffix, rows=len(ids), ordered_sha256=digest(ids), set_sha256=digest(sorted(ids))))
# UNION ALL sources supply smaller vectors without relying on a Python UDF.
for amount in ("17 ROWS", "17%"):
    for seed in (0, 42, 999):
        population, batch = 8193, 137
        source = " UNION ALL ".join(f"SELECT range AS id FROM range({start},{min(start+batch,population)})" for start in range(0,population,batch))
        suffix = f"reservoir({amount}) REPEATABLE({seed})"
        ids = [r[0] for r in con.execute(f"SELECT id FROM ({source}) USING SAMPLE {suffix}").fetchall()]
        cases.append(dict(population=population, batch_rows=batch, suffix=suffix, rows=len(ids), ordered_sha256=digest(ids), set_sha256=digest(sorted(ids))))
out = Path(__file__).resolve().parents[1] / "corpus" / f"reservoir-duckdb-{args.version}-msvc.json"
out.write_text(json.dumps(dict(version=args.version, profile="duckdb-msvc", threads=1, batch_rows=2048, cases=cases), indent=2)+"\n", encoding="utf-8")
print(out)

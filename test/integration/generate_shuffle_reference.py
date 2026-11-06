"""Run the independently compiled libstdc++ oracle and record bounded hashes."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("--oracle", type=Path, required=True)
parser.add_argument("--compiler", required=True)
args = parser.parse_args()
cases = []
for seed in (0, 42, 999):
    for count in (1, 2, 17, 2051, 70000):
        lines = subprocess.check_output([str(args.oracle), str(seed), str(count)], text=True).splitlines()
        values, next_value = lines[:-1], int(lines[-1].split("=")[1])
        assert len(values) == count
        cases.append(dict(seed=seed, count=count, next_uint32=next_value, sha256=hashlib.sha256("".join(v+"\n" for v in values).encode()).hexdigest()))
out = Path(__file__).resolve().parents[1] / "corpus/reservoir-shuffle-libstdcpp.json"
out.write_text(json.dumps(dict(profile="duckdb", compiler=args.compiler, cases=cases), indent=2)+"\n", encoding="utf-8")
print(out)

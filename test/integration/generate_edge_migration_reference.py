"""Generate reference answers with the pinned DuckDB, never with fssql."""
import hashlib
import argparse
import re
import subprocess
import json
from pathlib import Path
import tempfile
import duckdb

ROOT = Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser()
parser.add_argument("--version", choices=("2.0.0", "1.5.5"), default="2.0.0")
parser.add_argument("--caller-root", type=Path)
args = parser.parse_args()
VERSION = args.version
if duckdb.__version__ != VERSION:
    raise SystemExit(f"DuckDB {VERSION} required, got {duckdb.__version__}")
con = duckdb.connect()
con.execute("SET TimeZone='UTC'")
con.execute("SET threads=1")
query = r"""SELECT CAST(255 AS UTINYINT) AS u,
CAST('12.30' AS DECIMAL(10,2)) AS d, CAST('A\x00\xFF\x5C\x22,\x0A' AS BLOB) AS b,
CAST('2026-01-02 03:04:05.123456+00' AS TIMESTAMPTZ) AS tz,
CAST('2026-01-02 03:04:05.123456789' AS TIMESTAMP_NS) AS ns, 'x' AS note
UNION ALL SELECT NULL,NULL,NULL,NULL,NULL,NULL
UNION ALL SELECT CAST(0 AS UTINYINT),CAST(0 AS DECIMAL(10,2)),CAST('' AS BLOB),NULL,NULL,''"""
result = {
    "reference_version": VERSION, "timezone": "UTC", "threads": 1,
    "query": query, "query_sha256": hashlib.sha256(query.encode()).hexdigest(),
    "types": [r[1] for r in con.execute("DESCRIBE " + query).fetchall()],
    "csv": {},
	"samples": [],
    "cast_tz": con.execute("SELECT CAST(CAST('2026-01-02 03:04:05.123456+00' AS TIMESTAMPTZ) AS VARCHAR)").fetchone()[0],
    "external_encryption": {"status": "NOT_RUN", "reason": "Java consumer and custom extension interoperability not executed"},
}
if args.caller_root:
    mod_bytes = (args.caller_root / "go.mod").read_bytes()
    mod = mod_bytes.decode("utf-8")
    binding = re.search(r"github.com/duckdb/duckdb-go/v2\s+(\S+)", mod)
    extension = args.caller_root / "duckdb/extensions/v1.5.5/linux_amd64/encrypted_parquet.duckdb_extension"
    result["caller"] = {
        "commit": subprocess.check_output(["git", "-C", str(args.caller_root), "rev-parse", "HEAD"], text=True).strip(),
        "go_binding": binding.group(1) if binding else None,
        "go_mod_sha256": hashlib.sha256(mod_bytes).hexdigest(),
        "working_tree_diff_sha256": hashlib.sha256(subprocess.check_output(["git", "-C", str(args.caller_root), "diff", "--binary"])).hexdigest(),
        "encrypted_extension_platform": "linux_amd64",
        "encrypted_extension_sha256": hashlib.sha256(extension.read_bytes()).hexdigest(),
        "encrypted_extension_interop": "DEFERRED_BY_USER",
    }
con.execute("CREATE TEMP TABLE frozen AS " + query)
con.execute("CREATE TEMP VIEW output AS SELECT u FROM frozen")
con.execute("CREATE OR REPLACE TEMP VIEW output AS SELECT note FROM frozen")
try:
    con.execute("CREATE OR REPLACE VIEW output AS SELECT missing FROM frozen")
    raise AssertionError("invalid replacement was accepted")
except duckdb.BinderException:
    pass
result["control"] = {"count": con.execute("SELECT count(*) FROM frozen").fetchone()[0], "notes": con.execute("SELECT note FROM output").fetchall()}
result["null_safe"] = con.execute("SELECT NULL IS NOT DISTINCT FROM NULL,1 IS DISTINCT FROM NULL,1 IS DISTINCT FROM 1").fetchone()
for method in ("bernoulli", "system", "reservoir"):
    for seed in (0, 42, 999):
        suffix = f"{method}(17%) REPEATABLE({seed})" if method != "reservoir" else f"reservoir(17 ROWS) REPEATABLE({seed})"
        rows = con.execute(f"SELECT range AS id FROM range(8193) USING SAMPLE {suffix}").fetchall()
        result["samples"].append({"method": method, "suffix": suffix, "seed": seed, "ids": [r[0] for r in rows]})
with tempfile.TemporaryDirectory(dir=ROOT / ".cache") as tmp:
    for force in (False, True):
        target = (Path(tmp) / ("force.csv" if force else "plain.csv")).as_posix()
        con.execute(f"COPY ({query}) TO '{target}' (FORMAT CSV" + (", FORCE_QUOTE *" if force else "") + ")")
        result["csv"]["force" if force else "plain"] = Path(target).read_bytes().decode()
filename = "edge-agent-migration-reference.json" if VERSION == "2.0.0" else "edge-agent-caller-reference.json"
out = ROOT / "test/corpus" / filename
out.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(out)

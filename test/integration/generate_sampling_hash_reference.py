"""Canonical MD5 sampling differential fixture using the caller's SQL shape."""
import argparse
import json
from pathlib import Path
import duckdb

parser = argparse.ArgumentParser()
parser.add_argument("--version", choices=("1.5.5", "2.0.0"), required=True)
args = parser.parse_args()
if duckdb.__version__ != args.version:
    raise SystemExit("wrong DuckDB version")
con = duckdb.connect()
con.execute("SET TimeZone='UTC'")
values = [
    ("汉字😀é;:", "12.300000", "2026-01-02 03:04:05.123456"),
    ("", "-0.000001", "1969-12-31 23:59:59.999999"),
    (None, None, None),
    ("NULL", "99999999999999999999999999999999.123456", "2000-02-29 00:00:00"),
    ("é", "0.000000", "1970-01-01 00:00:00.000001"),
    ("é", "0.000000", "1970-01-01 00:00:00.000001"),
    ("\n'a:;", "-12.300000", "2026-01-02 03:04:05.999999"),
    ("汉字😀é;:", "12.300000", "2026-01-02 03:04:05.123456"),
]
literal = lambda v: "NULL" if v is None else "'" + v.replace("'", "''") + "'"
source = " UNION ALL ".join(f"SELECT CAST({i} AS BIGINT) AS id, CAST({literal(s)} AS VARCHAR) AS txt, CAST({literal(d)} AS DECIMAL(38,6)) AS d, CAST({literal(tm)} AS TIMESTAMP) AS tm" for i, (s, d, tm) in enumerate(values))
fields = [("txt", "S", "CAST(txt AS VARCHAR)"), ("d", "D", "CAST(CAST(d AS DECIMAL(38,6)) AS VARCHAR)"), ("tm", "M", "strftime(CAST(tm AS TIMESTAMP), '%Y-%m-%d %H:%M:%S.%g')")]
encoded = [f"CASE WHEN {name} IS NULL THEN 'N;' ELSE '{marker}' || CAST(LENGTH({expr}) AS VARCHAR) || ':' || {expr} || ';' END" for name, marker, expr in fields]
base = "WITH source_rows AS (" + source + "), canonical AS (SELECT id, '6:random;2:42;' || " + " || ".join(encoded) + " AS payload FROM source_rows), hashed AS (SELECT id,payload,md5(payload) AS h FROM canonical) "
canonical_sql = base + "SELECT id,payload,h FROM hashed ORDER BY id"
canonical = con.execute(canonical_sql).fetchall()
boundary = canonical[0][2]
cases = []
for tail in ("ORDER BY h,id LIMIT 4", "WHERE h < '80000000000000000000000000000000' ORDER BY id", f"WHERE h < '{boundary}' ORDER BY id", f"WHERE h <= '{boundary}' ORDER BY id"):
    sql = base + "SELECT id FROM hashed " + tail
    cases.append(dict(sql=sql, ids=[r[0] for r in con.execute(sql).fetchall()]))
out = Path(__file__).resolve().parents[1] / f"corpus/sampling-hash-duckdb-{args.version}.json"
out.write_text(json.dumps(dict(version=args.version, canonical_sql=canonical_sql, canonical=canonical, cases=cases), ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
print(out)

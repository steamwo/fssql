#!/usr/bin/env python3
import json, subprocess, sys
import duckdb

def go_results():
    p = subprocess.run(["go", "run", "./cmd/aggregate-compat"], check=True, text=True, capture_output=True)
    return json.loads(p.stdout)

def split_i128(v: int):
    if v < 0:
        v += 1 << 128
    lo = v & ((1 << 64) - 1)
    hi_u = (v >> 64) & ((1 << 64) - 1)
    hi = hi_u if hi_u < (1 << 63) else hi_u - (1 << 64)
    return {"hi": hi, "lo": lo}

def duck_results():
    con = duckdb.connect()
    sum_row = con.execute("SELECT sum(x)::VARCHAR, typeof(sum(x)) FROM (VALUES (9223372036854775807::BIGINT), (1::BIGINT)) t(x)").fetchone()
    row = con.execute("SELECT count(x), avg(x), min(x), max(x) FROM (VALUES (1::BIGINT), (2::BIGINT)) t(x)").fetchone()
    null_sum = con.execute("SELECT sum(x) IS NULL FROM (VALUES (NULL::BIGINT),(NULL::BIGINT)) t(x)").fetchone()[0]
    empty_count = con.execute("SELECT count(*) FROM (SELECT 1 WHERE false) t").fetchone()[0]
    distinct = con.execute("SELECT DISTINCT x FROM (VALUES (1::BIGINT),(1),(NULL),(NULL),(2)) t(x) ORDER BY x NULLS FIRST").fetchall()
    distinct_norm = sorted("NULL" if x[0] is None else str(x[0]) for x in distinct)
    sorted_rows = con.execute("SELECT x FROM (VALUES (3::BIGINT),(NULL),(1),(2)) t(x) ORDER BY x DESC NULLS LAST").fetchall()
    sorted_norm = ["NULL" if x[0] is None else str(x[0]) for x in sorted_rows]
    return {"sum_bigint_type": sum_row[1].upper(), "sum_bigint": split_i128(int(sum_row[0])), "count": row[0], "avg": row[1], "min": row[2], "max": row[3], "null_only_sum_is_null": bool(null_sum), "empty_count": empty_count, "distinct": distinct_norm, "sorted_desc_nulls_last": sorted_norm}

def main():
    ours = go_results(); duck = duck_results()
    if ours != duck:
        print(json.dumps({"ours":ours,"duckdb":duck},indent=2,sort_keys=True), file=sys.stderr)
        raise SystemExit("Phase 2 DuckDB differential mismatch")
    print(json.dumps({"status":"PASS","duckdb_version":duckdb.__version__,"result":ours}, indent=2, sort_keys=True))

if __name__ == "__main__": main()

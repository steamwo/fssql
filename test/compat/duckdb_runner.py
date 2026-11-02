#!/usr/bin/env python3
import datetime
import decimal
import json
import sys

try:
    import duckdb
except Exception as exc:
    print(json.dumps({"error": f"duckdb import failed: {exc}"}))
    raise SystemExit(2)


def canonical(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return format(value, ".17g")
    if isinstance(value, decimal.Decimal):
        text = format(value, "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text or "0"
    if isinstance(value, (datetime.date, datetime.datetime, datetime.time)):
        return value.isoformat()
    return str(value)


def main():
    request = json.load(sys.stdin)
    queries = request.get("queries", [])
    con = duckdb.connect(database=":memory:")
    results = []
    for sql in queries:
        try:
            rows = con.execute(sql).fetchall()
            values = []
            for row in rows:
                values.extend(canonical(value) for value in row)
            results.append({"values": values})
        except Exception as exc:
            results.append({"error": str(exc)})
    json.dump({"duckdb_version": duckdb.__version__, "results": results}, sys.stdout)


if __name__ == "__main__":
    main()

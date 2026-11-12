#!/usr/bin/env python3
import datetime
import decimal
import json
import math
import sys

# Go sends JSON as UTF-8, independently of the host's console code page.
sys.stdin.reconfigure(encoding="utf-8")
sys.stdout.reconfigure(encoding="utf-8")

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


def typed_result(con, sql):
    # SQL-side text conversion preserves nanoseconds and exact DECIMAL/HUGEINT.
    # Do not fetch timestamps through Python datetime (microsecond precision).
    source = con.sql(sql)
    names = source.columns
    types = [str(t) for t in source.types]
    aliases = [f"__c{i}" for i in range(len(types))]
    projections = [
        f"hex({name})" if typ == "BLOB" else
        f"printf('%.17g', CAST({name} AS DOUBLE))" if typ in ("FLOAT", "DOUBLE") else
        f"CAST({name} AS VARCHAR)"
        for name, typ in zip(aliases, types)
    ]
    query = (
        f"SELECT {','.join(projections)} FROM ({sql.rstrip().rstrip(';')}) "
        f"AS __typed({','.join(aliases)})"
    )
    rows = con.sql(query).fetchall()
    normalized = []
    for row in rows:
        values = []
        for value, typ in zip(row, types):
            if value is not None and typ in ("FLOAT", "DOUBLE"):
                number = float(value)
                if math.isnan(number):
                    value = "nan"
                elif math.isinf(number):
                    value = "inf" if number > 0 else "-inf"
                else:
                    value = "0" if number == 0 else format(number, ".17g")
            if value is not None and typ.startswith("DECIMAL("):
                # SQL CAST omits the leading zero for all-fractional DECIMALs.
                # Canonicalize notation without changing exact digits or scale.
                if value.startswith("."):
                    value = "0" + value
                elif value.startswith("-."):
                    value = "-0" + value[1:]
            values.append(value)
        normalized.append(values)
    return {"names": names, "types": types, "rows": normalized}


def error_category(exc):
    return {
        "ParserException": "parser",
        "BinderException": "binder",
        "CatalogException": "binder",
        "ConversionException": "conversion",
        "OutOfRangeException": "overflow",
    }.get(type(exc).__name__, "unknown")


def main():
    request = json.load(sys.stdin)
    queries = request.get("queries", [])
    con = duckdb.connect(database=":memory:")
    # 语义基线固定为 UTC；主机本地时区不能让 timezone()/显示规则产生差异。
    con.execute("SET TimeZone='UTC'")
    threads = request.get("threads", 0)
    if threads:
        con.execute("SET threads=%d" % threads)
    results = []
    for sql in queries:
        try:
            if request.get("typed"):
                results.append(typed_result(con, sql))
                continue
            cursor = con.execute(sql)
            rows = cursor.fetchall()
            values = []
            for row in rows:
                values.extend(canonical(value) for value in row)
            columns = len(cursor.description) if cursor.description else 0
            results.append({"values": values, "columns": columns})
        except Exception as exc:
            results.append({"error": str(exc), "error_category": error_category(exc)})
    json.dump({"duckdb_version": duckdb.__version__, "results": results}, sys.stdout)


if __name__ == "__main__":
    main()

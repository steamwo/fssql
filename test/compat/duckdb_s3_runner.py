#!/usr/bin/env python3
"""DuckDB httpfs 侧的 S3 参考 runner。

从 stdin 读取 JSON：
  {
    "endpoint": "http://127.0.0.1:9000",
    "region": "us-east-1",
    "access_key": "...",
    "secret_key": "...",
    "statements": ["COPY ... TO 's3://...' (FORMAT PARQUET)"],
    "queries": ["SELECT ..."]
  }

凭据只经 stdin 传递，不出现在进程命令行；输出 JSON 到 stdout。
"""

import datetime
import decimal
import json
import sys
from urllib.parse import urlparse

# Go 以 UTF-8 写 JSON；Windows 控制台代码页不能参与 SQL 文本解码。
sys.stdin.reconfigure(encoding="utf-8")
sys.stdout.reconfigure(encoding="utf-8")

try:
    import duckdb
except Exception as exc:  # pragma: no cover
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
        return format(value, "f")
    if isinstance(value, (datetime.date, datetime.datetime, datetime.time)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def query_rows(connection, sql):
    cursor = connection.execute(sql)
    rows = cursor.fetchall()
    values = []
    for row in rows:
        values.extend(canonical(value) for value in row)
    return {
        "values": values,
        "columns": len(cursor.description) if cursor.description else 0,
        "error": "",
    }


def configure(connection, request):
    connection.execute("SET TimeZone='UTC'")
    connection.execute("SET threads=1")
    connection.execute("LOAD httpfs")
    endpoint = urlparse(request["endpoint"])
    host = endpoint.hostname
    port = endpoint.port or (443 if endpoint.scheme == "https" else 9000)
    connection.execute("SET s3_endpoint=?", ["%s:%d" % (host, port)])
    connection.execute("SET s3_use_ssl=?", ["true" if endpoint.scheme == "https" else "false"])
    connection.execute("SET s3_url_style='path'")
    connection.execute("SET s3_access_key_id=?", [request["access_key"]])
    connection.execute("SET s3_secret_access_key=?", [request["secret_key"]])
    connection.execute("SET s3_region=?", [request.get("region", "us-east-1")])


def main():
    request = json.load(sys.stdin)
    connection = duckdb.connect(database=":memory:")
    configure(connection, request)

    statements = []
    for sql in request.get("statements", []):
        try:
            connection.execute(sql)
            statements.append({"error": ""})
        except Exception as exc:
            statements.append({"error": str(exc)})

    results = []
    for sql in request.get("queries", []):
        try:
            results.append(query_rows(connection, sql))
        except Exception as exc:
            results.append({"values": [], "columns": 0, "error": str(exc)})

    json.dump(
        {
            "duckdb_version": duckdb.__version__,
            "statements": statements,
            "results": results,
        },
        sys.stdout,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        raise SystemExit(1)

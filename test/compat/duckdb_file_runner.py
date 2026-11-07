#!/usr/bin/env python3
"""文件互操作参考 runner。

用法：
  python duckdb_file_runner.py read <csv|json|ndjson|parquet> <path>
  python duckdb_file_runner.py write <directory>
  python duckdb_file_runner.py write-parquet-zstd <duckdb|pyarrow> <path>

read 读取一个文件并输出规范化值；write 用 DuckDB 写出 CSV/NDJSON/Parquet
（DuckDB 2.0 的 FORMAT JSON 实际也是逐行 NDJSON），再读回并输出值。
write-parquet-zstd 用指定生成器写出真实 ZSTD 文件并返回读回值、版本和 codec。
输出 JSON 到 stdout；任何失败写入 error 字段并以非零状态退出。
"""

import datetime
import decimal
import json
import os
import sys

sys.stdout.reconfigure(encoding="utf-8")

try:
    import duckdb
except Exception as exc:  # pragma: no cover - dependency failure is reported to Go
    print(json.dumps({"error": f"duckdb import failed: {exc}"}))
    raise SystemExit(2)


FIXTURE_SQL = """
CREATE TABLE interop_fixture(
    id BIGINT,
    label VARCHAR,
    amount BIGINT,
    ratio DOUBLE,
    day DATE,
    ts TIMESTAMP,
    flag BOOLEAN
);
INSERT INTO interop_fixture VALUES
    (1, 'east', 10, 0.5, DATE '2026-01-02', TIMESTAMP '2026-01-02 03:04:05.123456', true),
    (2, 'east', NULL, NULL, NULL, NULL, false),
    (3, 'west', 30, 1.25, DATE '2026-01-03', TIMESTAMP '2026-01-03 04:05:06.000001', true),
    (4, '汉字😊', 0, 2.5, DATE '2024-02-29', TIMESTAMP '2024-02-29 23:59:59', false),
    (5, 'quote,"newline\n', NULL, 0.001, DATE '1970-01-01', TIMESTAMP '1969-12-31 23:59:59.999999', true);
"""

# 时间列投影为 VARCHAR，避免 Python datetime 与 Go time.Time 的格式差异。
SELECT_COLUMNS = """
    id, label, amount, ratio,
    CAST(day AS VARCHAR) AS day,
    CAST(ts AS VARCHAR) AS ts,
    flag
"""


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
    return {"values": values, "columns": len(cursor.description) if cursor.description else 0}


# typed Parquet 投影：所有值转成精确文本/十六进制，避免 Python 绑定对
# DECIMAL 尾零、UBIGINT 和纳秒时间戳的表示差异。
TYPED_SELECT = """
    id,
    CAST(dec_value AS VARCHAR) AS dec_value,
    CAST(wide_value AS VARCHAR) AS wide_value,
    CAST(ubig_value AS VARCHAR) AS ubig_value,
    CAST(ns_value AS VARCHAR) AS ns_value,
    hex(raw_value) AS raw_hex
"""

TYPED_FIXTURE_SQL = """
CREATE TABLE typed_fixture(
    id BIGINT,
    dec_value DECIMAL(18,2),
    wide_value DECIMAL(38,6),
    ubig_value UBIGINT,
    ns_value TIMESTAMP_NS,
    raw_value BLOB
);
INSERT INTO typed_fixture VALUES
    (1, 12.34, 123456789012345678901234567890.123456, 18446744073709551615,
        TIMESTAMP_NS '2026-01-02 03:04:05.123456789', from_hex('4100FF')),
    (2, NULL, NULL, NULL, NULL, NULL),
    (3, -0.01, -0.000001, 0,
        TIMESTAMP_NS '1969-12-31 23:59:59.999999999', from_hex(''));
"""


def sql_string(value):
    return "'" + value.replace("'", "''") + "'"


def source_sql(fmt, path):
    if fmt == "csv":
        return "read_csv_auto(%s, header=true)" % sql_string(path)
    if fmt in ("json", "ndjson"):
        return "read_json_auto(%s)" % sql_string(path)
    if fmt == "parquet":
        return "read_parquet(%s)" % sql_string(path)
    raise ValueError("unsupported format %s" % fmt)


def main():
    if len(sys.argv) < 2:
        raise ValueError("missing mode")
    mode = sys.argv[1]
    connection = duckdb.connect(database=":memory:")
    connection.execute("SET TimeZone='UTC'")
    connection.execute("SET threads=1")

    if mode == "read":
        if len(sys.argv) != 4:
            raise ValueError("read requires <format> <path>")
        fmt, path = sys.argv[2], sys.argv[3]
        result = query_rows(connection, "SELECT %s FROM %s ORDER BY id" % (SELECT_COLUMNS, source_sql(fmt, path)))
        print(json.dumps(result))
        return

    if mode == "read-typed":
        if len(sys.argv) != 3:
            raise ValueError("read-typed requires <path>")
        path = sys.argv[2]
        result = query_rows(
            connection,
            "SELECT %s FROM read_parquet(%s) ORDER BY id" % (TYPED_SELECT, sql_string(path)),
        )
        print(json.dumps(result))
        return

    if mode == "write-parquet-zstd":
        if len(sys.argv) != 4:
            raise ValueError("write-parquet-zstd requires <duckdb|pyarrow> <path>")
        producer, path = sys.argv[2], sys.argv[3]
        if producer not in ("duckdb", "pyarrow"):
            raise ValueError("unsupported Parquet producer %s" % producer)

        # 两种生成器共用同一 typed fixture；PyArrow 只负责独立编码，避免维护两份期望值。
        import pyarrow as pa
        import pyarrow.parquet as pq

        connection.execute(FIXTURE_SQL)
        if producer == "duckdb":
            connection.execute(
                "COPY (SELECT * FROM interop_fixture ORDER BY id) TO %s (FORMAT PARQUET, COMPRESSION ZSTD)"
                % sql_string(path)
            )
            version = duckdb.__version__
        else:
            table = connection.sql("SELECT * FROM interop_fixture ORDER BY id").to_arrow_table()
            pq.write_table(
                table,
                path,
                compression="zstd",
                row_group_size=2,
                use_dictionary=True,
                version="2.6",
                data_page_version="1.0",
            )
            version = pa.__version__

        # 不信任写入选项名称；逐 row group、逐列核实生成文件的真实 codec。
        metadata = pq.ParquetFile(path).metadata
        codecs = sorted(
            {
                metadata.row_group(group).column(column).compression
                for group in range(metadata.num_row_groups)
                for column in range(metadata.num_columns)
            }
        )
        if codecs != ["ZSTD"]:
            raise AssertionError("Parquet compression=%r, want ['ZSTD']" % codecs)
        result = query_rows(
            connection,
            "SELECT %s FROM read_parquet(%s) ORDER BY id" % (SELECT_COLUMNS, sql_string(path)),
        )
        result.update({"producer": producer, "version": version, "compression": codecs})
        print(json.dumps(result))
        return

    if mode == "write-typed":
        if len(sys.argv) != 3:
            raise ValueError("write-typed requires <path>")
        path = sys.argv[2]
        connection.execute(TYPED_FIXTURE_SQL)
        connection.execute(
            "COPY (SELECT * FROM typed_fixture ORDER BY id) TO %s (FORMAT PARQUET, COMPRESSION SNAPPY)"
            % sql_string(path)
        )
        result = query_rows(
            connection,
            "SELECT %s FROM read_parquet(%s) ORDER BY id" % (TYPED_SELECT, sql_string(path)),
        )
        result["duckdb_version"] = duckdb.__version__
        print(json.dumps(result))
        return

    if mode == "write":
        if len(sys.argv) != 3:
            raise ValueError("write requires <directory>")
        directory = sys.argv[2]
        connection.execute(FIXTURE_SQL)
        writers = {
            "csv": ("csv", "FORMAT CSV, HEADER"),
            "ndjson": ("ndjson", "FORMAT NDJSON"),
            "parquet": ("parquet", "FORMAT PARQUET, COMPRESSION SNAPPY"),
        }
        formats = {}
        for fmt, (extension, options) in writers.items():
            path = os.path.join(directory, "duckdb." + extension)
            connection.execute(
                "COPY (SELECT * FROM interop_fixture ORDER BY id) TO %s (%s)"
                % (sql_string(path), options)
            )
            formats[fmt] = query_rows(
                connection, "SELECT %s FROM %s ORDER BY id" % (SELECT_COLUMNS, source_sql(fmt, path))
            )
        print(json.dumps({"formats": formats, "duckdb_version": duckdb.__version__}))
        return

    raise ValueError("unknown mode %s" % mode)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        raise SystemExit(1)

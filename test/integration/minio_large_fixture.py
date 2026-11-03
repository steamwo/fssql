"""生成有界批次的大文件，并用 DuckDB 为真实 MinIO 测试计算独立参考结果。"""

import hashlib
import json
import os
from pathlib import Path
import time

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


def canonical_row(row):
    """编码跨 Python/Go 一致的结果行，显式区分 NULL、整数、浮点和字符串。"""
    values = []
    for value in row:
        if value is None:
            values.append(b"N;")
        elif isinstance(value, bool):
            values.append(b"B1;" if value else b"B0;")
        elif isinstance(value, int):
            values.append(b"I" + str(value).encode() + b";")
        elif isinstance(value, float):
            values.append(b"F" + format(value, ".9g").encode() + b";")
        else:
            raw = str(value).encode("utf-8")
            values.append(b"S" + str(len(raw)).encode() + b":" + raw + b";")
    return b"".join(values) + b"\n"


def reference_digest(conn, sql, ordered):
    """逐批计算结果摘要，非排序查询采用与返回顺序无关的 256 位行摘要和。"""
    conn.execute(sql)
    digest = hashlib.sha256()
    total = 0
    count = 0
    while True:
        rows = conn.fetchmany(4096)
        if not rows:
            break
        for row in rows:
            encoded = canonical_row(row)
            if ordered:
                digest.update(encoded)
            else:
                total = (total + int.from_bytes(hashlib.sha256(encoded).digest(), "little")) % (1 << 256)
            count += 1
    return count, digest.hexdigest() if ordered else total.to_bytes(32, "little").hex()


def splitmix64(state):
    """提供跨语言固定的随机序列，避免测试参考依赖 Python/Go 各自的随机库。"""
    mask = (1 << 64) - 1
    state = (state + 0x9E3779B97F4A7C15) & mask
    value = state
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & mask
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & mask
    return state, value ^ (value >> 31)


def reservoir_reference(rows, size=1000):
    """对有序 ID 流计算确定性的 reservoir 抽样参考摘要。"""
    sample = list(range(size))
    state = 42
    for index in range(size, rows):
        bound = index + 1
        limit = ((1 << 64) - 1) - (((1 << 64) - 1) % bound)
        while True:
            state, value = splitmix64(state)
            if value < limit:
                break
        slot = value % bound
        if slot < size:
            sample[slot] = index
    digest = hashlib.sha256()
    for value in sample:
        digest.update(canonical_row((value,)))
    return digest.hexdigest()


def generate_files(root, rows):
    """每批最多 32768 行，生成无压缩 Parquet、CSV、NDJSON 和多文件样本。"""
    marker = root / "fixture-version.json"
    expected = {"rows": rows, "version": 1, "payload_bytes": 256}
    if marker.exists() and json.loads(marker.read_text()) == expected:
        return
    root.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([(name, pa.int64()) for name in ["id", "grp", "value", "nullable_value", "sort_key"]] + [("payload", pa.string())])
    writer = pq.ParquetWriter(root / "data.parquet", schema, compression="NONE", use_dictionary=False, data_page_version="1.0")
    (root / "parts").mkdir(exist_ok=True)
    csv = (root / "data.csv").open("w", encoding="utf-8", newline="", buffering=4 << 20)
    ndjson = (root / "data.ndjson").open("w", encoding="utf-8", newline="", buffering=4 << 20)
    array_rows = []
    try:
        for base in range(0, rows, 32768):
            ids = list(range(base, min(base + 32768, rows)))
            payloads = [hashlib.sha256(str(i).encode()).hexdigest() * 4 for i in ids]
            values = {"id": ids, "grp": [i % 1000 for i in ids], "value": [i % 1000 - 500 for i in ids],
                      "nullable_value": [None if i % 17 == 0 else i % 1000 - 500 for i in ids],
                      "sort_key": [(i * 104729) % rows for i in ids], "payload": payloads}
            table = pa.table(values, schema=schema)
            writer.write_table(table, row_group_size=32768)
            pq.write_table(table.select(["id", "grp", "value"]), root / "parts" / ("part-%04d.parquet" % (base // 32768)), compression="NONE", use_dictionary=False)
            for offset, identifier in enumerate(ids):
                record = {key: column[offset] for key, column in values.items()}
                csv.write(",".join("" if record[key] is None else str(record[key]) for key in values) + "\n")
                ndjson.write(json.dumps(record, separators=(",", ":")) + "\n")
                if identifier < 10000:
                    array_rows.append({"id": identifier, "grp": record["grp"], "value": record["value"]})
            print("generated rows=%d/%d" % (min(base + 32768, rows), rows), flush=True)
    finally:
        writer.close()
        csv.close()
        ndjson.close()
    (root / "small.json").write_text(json.dumps(array_rows), encoding="utf-8")
    pq.write_table(pa.table({"grp": list(range(1000)), "weight": list(range(1, 1001))}), root / "dim.parquet", compression="NONE", use_dictionary=False)
    pq.write_table(pa.table({"grp": list(range(100))}), root / "dim-small.parquet", compression="NONE", use_dictionary=False)
    marker.write_text(json.dumps(expected), encoding="utf-8")


def build_cases(bucket, rows):
    """定义正常、强制 spill、边界和失败场景，不将不支持的抽样语法算作支持。"""
    p = "read_parquet('s3://%s/large/data.parquet')" % bucket
    d = "read_parquet('s3://%s/large/dim.parquet')" % bucket
    small = "read_parquet('s3://%s/large/dim-small.parquet')" % bucket
    cases = []

    def add(name, sql, ordered=False, **kwargs):
        """使用统一的有界资源默认值注册一个场景。"""
        cases.append(dict(name=name, sql=sql, ordered=ordered, memory_limit=64 << 20,
                          spill_budget=4 << 20, kind="compare", **kwargs))

    add("scan_narrow", "SELECT id, grp, value FROM " + p)
    add("scan_wide", "SELECT id, payload FROM " + p)
    add("projection_limit", "SELECT id, payload FROM " + p + " LIMIT 100")
    cases[-1]["kind"] = "limit"
    add("filter_range", "SELECT id, value FROM " + p + " WHERE id >= %d AND id < %d" % (rows // 2, rows // 2 + 1000))
    add("filter_empty", "SELECT id FROM " + p + " WHERE id < 0")
    add("expressions_nulls", "SELECT id, id + 1, value * 2, nullable_value IS NULL FROM " + p + " WHERE id < 10000")
    add("global_aggregate", "SELECT COUNT(*), COUNT(nullable_value), SUM(value), AVG(value), MIN(value), MAX(value) FROM " + p)
    add("group_having", "SELECT grp, COUNT(*), SUM(value), AVG(value) FROM " + p + " GROUP BY grp HAVING COUNT(*) >= 100 ORDER BY grp", True)
    add("distinct", "SELECT DISTINCT grp FROM " + p + " ORDER BY grp", True)
    add("sort_spill", "SELECT sort_key, id FROM " + p + " ORDER BY sort_key, id", True, needs_spill=True)
    add("sort_in_memory", "SELECT sort_key, id FROM " + p + " ORDER BY sort_key, id", True, no_spill=True)
    cases[-1].update(memory_limit=256 << 20, spill_budget=128 << 20)
    add("topn", "SELECT id, value FROM " + p + " ORDER BY value DESC, id ASC LIMIT 100", True)
    add("offset", "SELECT id, value FROM " + p + " ORDER BY id LIMIT 100 OFFSET 100000", True)
    add("cte", "WITH a AS (SELECT grp, value FROM " + p + " WHERE id < 10000) SELECT grp, SUM(value) FROM a GROUP BY grp ORDER BY grp", True)
    add("scalar_aggregate_unsupported", "SELECT COUNT(*) FROM " + p + " WHERE id < (SELECT MAX(grp) FROM " + d + ")")
    cases[-1].update(kind="unsupported", error_contains="outside stable")
    add("scalar_subquery", "SELECT id FROM " + p + " WHERE id < (SELECT MAX(grp) FROM " + d + ") ORDER BY id", True)
    add("inner_join", "SELECT COUNT(*), SUM(d.weight) FROM " + p + " f JOIN " + d + " d ON f.grp = d.grp")
    add("left_join_nulls", "SELECT COUNT(*), COUNT(d.grp) FROM " + p + " f LEFT JOIN " + small + " d ON f.grp = d.grp WHERE f.id < 10000")
    add("semi_join", "SELECT COUNT(*) FROM " + p + " f SEMI JOIN " + small + " d ON f.grp = d.grp")
    add("anti_join", "SELECT COUNT(*) FROM " + p + " f ANTI JOIN " + small + " d ON f.grp = d.grp")
    add("right_join", "WITH a AS (SELECT id, grp FROM " + p + " WHERE id < 100) SELECT COUNT(*), COUNT(a.id) FROM a RIGHT JOIN " + d + " d ON a.grp = d.grp")
    add("full_join", "WITH a AS (SELECT id, grp FROM " + p + " WHERE id < 50), b AS (SELECT grp FROM " + d + " WHERE grp >= 25) SELECT COUNT(*), COUNT(a.id), COUNT(b.grp) FROM a FULL JOIN b ON a.grp = b.grp")
    add("cross_join", "WITH a AS (SELECT id FROM " + p + " WHERE id < 100) SELECT COUNT(*) FROM a CROSS JOIN " + small + " d")
    add("aggregate_spill", "SELECT id, COUNT(*), SUM(value) FROM " + p + " WHERE id < 100000 GROUP BY id ORDER BY id", True, needs_spill=True)
    cases[-1]["spill_budget"] = 1 << 20
    add("aggregate_in_memory", "SELECT id, COUNT(*), SUM(value) FROM " + p + " WHERE id < 100000 GROUP BY id ORDER BY id", True, no_spill=True)
    cases[-1].update(memory_limit=128 << 20, spill_budget=64 << 20)
    add("derived_table_unsupported", "SELECT COUNT(*) FROM (SELECT id FROM " + p + " WHERE id < 100000) a JOIN (SELECT id FROM " + p + " WHERE id < 100000) b ON a.id = b.id")
    cases[-1].update(kind="unsupported", error_contains="sql parser")
    add("join_spill", "WITH a AS (SELECT id FROM " + p + " WHERE id < 100000), b AS (SELECT id FROM " + p + " WHERE id < 100000) SELECT COUNT(*) FROM a JOIN b ON a.id = b.id", needs_spill=True)
    cases[-1]["spill_budget"] = 1 << 20
    add("join_in_memory", "WITH a AS (SELECT id FROM " + p + " WHERE id < 100000), b AS (SELECT id FROM " + p + " WHERE id < 100000) SELECT COUNT(*) FROM a JOIN b ON a.id = b.id", no_spill=True)
    cases[-1].update(memory_limit=128 << 20, spill_budget=64 << 20)
    add("csv_scan", "SELECT column0, column2 FROM read_csv('s3://%s/large/data.csv') WHERE column0 >= %d" % (bucket, rows - 1000))
    add("csv_limit_wide", "SELECT column0, column5 FROM read_csv('s3://%s/large/data.csv') LIMIT 10000" % bucket)
    cases[-1]["kind"] = "limit"
    add("csv_large_memory_first_chunk", "SELECT column0, column5 FROM read_csv('s3://%s/large/data.csv')" % bucket)
    cases[-1].update(kind="first_chunk", memory_limit=1 << 30)
    add("ndjson_scan", "SELECT id, value FROM read_ndjson('s3://%s/large/data.ndjson') WHERE id >= %d" % (bucket, rows - 1000))
    add("ndjson_large_memory_first_chunk", "SELECT id, payload FROM read_ndjson('s3://%s/large/data.ndjson')" % bucket)
    cases[-1].update(kind="first_chunk", memory_limit=1 << 30)
    add("json_array", "SELECT id, grp, value FROM read_json('s3://%s/large/small.json') WHERE grp < 50" % bucket)
    add("multi_file", "SELECT COUNT(*), SUM(value) FROM read_parquet('s3://%s/large/parts/*.parquet')" % bucket)
    add("modulo_sample", "SELECT id FROM " + p + " WHERE id % 100 = 0")
    cases[-1].update(kind="unsupported", error_contains="unsupported bigint operator")
    add("arithmetic_sample", "SELECT id FROM " + p + " WHERE id = CAST(id / 100 AS BIGINT) * 100")
    cases[-1].update(kind="unsupported", error_contains="sql parser")
    add("stratified_sample", "SELECT id FROM " + p + " WHERE grp = 42")
    add("reservoir_sample", "SELECT id FROM " + p + " ORDER BY id", True)
    cases[-1]["kind"] = "reservoir"
    for name, suffix in [("native_sample_unsupported", " TABLESAMPLE reservoir(1000 ROWS)"), ("using_sample_unsupported", " USING SAMPLE 1000 ROWS")]:
        add(name, "SELECT id FROM " + p + suffix)
        cases[-1].update(kind="unsupported", error_contains="sql parser")
    add("random_function_unsupported", "SELECT random() FROM " + p + " LIMIT 1")
    cases[-1].update(kind="unsupported", error_contains="random")
    for name, kind in [("cancel", "cancel"), ("consumer_error", "consumer_error"), ("memory_limit", "memory_limit")]:
        add(name, "SELECT id, payload FROM " + p)
        cases[-1]["kind"] = kind
    cases[-1]["memory_limit"] = 512 << 10
    add("temp_quota", "SELECT sort_key, id FROM " + p + " ORDER BY sort_key", True)
    cases[-1].update(kind="temp_quota", temp_limit=64 << 10, spill_budget=64 << 10)
    add("concurrent_queries", "SELECT COUNT(*), SUM(value) FROM " + p + " WHERE id < 100000")
    cases[-1].update(kind="concurrent", memory_limit=128 << 20)
    add("repeat_queries", "SELECT COUNT(*), SUM(value) FROM " + p + " WHERE id < 10000")
    cases[-1]["kind"] = "repeat"
    add("copy_large", "SELECT id, value, payload FROM " + p + " WHERE id < 100000")
    cases[-1].update(kind="copy", copy_source="SELECT id, value, payload FROM " + p + " WHERE id < 100000")
    return cases


def main():
    """准备本仓库中的数据与独立参考值，向现有专用 bucket 上传测试对象。"""
    repo = Path(__file__).resolve().parents[2]
    cache = repo / ".cache/agent-verification"
    settings = json.loads((cache / "minio-test-env.json").read_text(encoding="utf-8"))
    bucket = settings["FSSQL_MINIO_BUCKET"]
    if not bucket.startswith("fssql-agent-test-"):
        raise RuntimeError("Only a dedicated test bucket is allowed")
    rows = int(os.environ.get("FSSQL_LARGE_ROWS", "1000000"))
    if rows < 200000:
        raise RuntimeError("Large matrix requires at least 200000 rows")
    root = cache / "large-data"
    generate_files(root, rows)
    client = boto3.client("s3", endpoint_url=settings["FSSQL_MINIO_ENDPOINT"],
                          aws_access_key_id=settings["FSSQL_MINIO_ACCESS_KEY"],
                          aws_secret_access_key=settings["FSSQL_MINIO_SECRET_KEY"], region_name="us-east-1",
                          config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))
    files = {}
    for path in sorted(root.rglob("*")):
        if path.suffix not in [".csv", ".parquet", ".ndjson", ".json"] or path.name == "fixture-version.json":
            continue
        relative = path.relative_to(root).as_posix()
        if relative.startswith("parts/") or relative in ["data.csv", "data.parquet", "data.ndjson", "small.json", "dim.parquet", "dim-small.parquet"]:
            try:
                head = client.head_object(Bucket=bucket, Key="large/" + relative)
            except ClientError as error:
                if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                    raise
                head = {}
            if head.get("ContentLength") != path.stat().st_size or head.get("Metadata", {}).get("fssql-test-rows") != str(rows):
                client.upload_file(str(path), bucket, "large/" + relative,
                                   ExtraArgs={"Metadata": {"fssql-test-rows": str(rows)}},
                                   Config=TransferConfig(max_concurrency=2))
            files[relative] = path.stat().st_size
            print("ready %s bytes=%d" % (relative, files[relative]), flush=True)
    cases = build_cases(bucket, rows)
    cached = {}
    target = cache / "large-manifest.json"
    if target.exists():
        previous = json.loads(target.read_text(encoding="utf-8"))
        if previous.get("rows") == rows and previous.get("files") == files:
            cached = {(c["sql"], c["ordered"], c["kind"]): c for c in previous["cases"] if "expected_digest" in c}
    conn = duckdb.connect()
    conn.execute("SET memory_limit='512MB'")
    conn.execute("SET threads=4")
    for case in cases:
        if case["kind"] in ["unsupported", "cancel", "consumer_error", "memory_limit", "temp_quota", "first_chunk"]:
            continue
        start = time.perf_counter()
        old = cached.get((case["sql"], case["ordered"], case["kind"]))
        if old is not None:
            case.update(expected_rows=old["expected_rows"], expected_digest=old["expected_digest"])
            case["reference_cached"] = True
        elif case["kind"] == "reservoir":
            case.update(expected_rows=1000, expected_digest=reservoir_reference(rows))
        else:
            sql = case["sql"].replace("s3://%s/large/" % bucket, root.as_posix() + "/")
            sql = sql.replace("read_ndjson(", "read_json_auto(")
            count, digest = reference_digest(conn, sql, case["ordered"])
            case.update(expected_rows=count, expected_digest=digest)
        case["reference_seconds"] = old.get("reference_seconds", 0) if old is not None else round(time.perf_counter() - start, 6)
        print("reference %s rows=%s seconds=%.3f" % (case["name"], case.get("expected_rows"), case["reference_seconds"]), flush=True)
    conn.close()
    manifest = {"rows": rows, "files": files, "duckdb_version": duckdb.__version__, "cases": cases}
    target.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8", newline="\n")
    evidence = repo / ".cache/agent-verification/large"
    evidence.mkdir(parents=True, exist_ok=True)
    (evidence / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8", newline="\n")
    print("fixture ready: %d rows, %d bytes, %d cases" % (rows, sum(files.values()), len(cases)), flush=True)


if __name__ == "__main__":
    main()

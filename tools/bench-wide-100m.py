#!/usr/bin/env python3
"""100M-row x 64-column JOIN/TopN end-to-end comparison for fssql vs DuckDB.

Methodology follows docs/join-topn-performance-2026-11-11.md where practical:
single worker/thread, 256 MiB memory, 2 warmups, 7 measured samples, complete
result consumption, median + MAD, and a 10% MAD validity gate.

The logical 100M-row fact table is 100 hard links to one deterministic 1M-row
64-column Parquet seed. This measures 1e8 logical rows under warm-cache
conditions without requiring 100x physical disk capacity. It is not a cold
storage throughput benchmark for 100 independent physical files.
"""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import threading
import time

import duckdb
import pyarrow

MIB = 1 << 20
GIB = 1 << 30
ROWS_PER_SHARD = 1_000_000
SHARDS = 100
LOGICAL_ROWS = ROWS_PER_SHARD * SHARDS
WARMUPS = 2
SAMPLES = 7
MEMORY_LIMIT = 256 * MIB
SPILL_BUDGET = 128 * MIB
TEMP_LIMIT = 8 * GIB
QUERY_TIMEOUT_SECONDS = 900

CASES = [
    {
        "name": "inner_join_count_sum",
        "label": "INNER JOIN + COUNT/SUM",
        "sql": "SELECT COUNT(*), SUM(d.weight) FROM read_parquet('{parts}') f JOIN read_parquet('{dim}') d ON f.grp = d.grp",
        "rows": 1,
    },
    {
        "name": "left_join_count",
        "label": "LEFT JOIN + COUNT",
        "sql": "SELECT COUNT(*), COUNT(d.grp) FROM read_parquet('{parts}') f LEFT JOIN read_parquet('{dim_small}') d ON f.grp = d.grp",
        "rows": 1,
    },
    {
        "name": "numeric_topn_k100",
        "label": "数值 TopN，K=100",
        "sql": "SELECT id, value FROM read_parquet('{parts}') ORDER BY value DESC, id ASC LIMIT 100",
        "rows": 100,
    },
    {
        "name": "varchar_topn_k100",
        "label": "VARCHAR TopN，K=100",
        "sql": "SELECT id, payload FROM read_parquet('{parts}') ORDER BY payload ASC, id ASC LIMIT 100",
        "rows": 100,
    },
]

OLD_BASELINE = {
    "inner_join_count_sum": {"fssql_ms": 85.3, "duckdb_ms": 17.2, "ratio": 4.97, "valid": True},
    "left_join_count": {"fssql_ms": 109.7, "duckdb_ms": 26.1, "ratio": 4.21, "valid": True},
    "numeric_topn_k100": {"fssql_ms": 83.5, "duckdb_ms": 18.1, "ratio": 4.61, "valid": True},
    "varchar_topn_k100": {"fssql_ms": 349.8, "duckdb_ms": 96.3, "ratio": None, "valid": False},
}

VERIFY_TEST = r'''package gosql

import (
    "context"
    "encoding/json"
    "fmt"
    "os"
    "testing"
)

func TestWide100MVerifyCase(t *testing.T) {
    path := os.Getenv("FSSQL_COMPARE_CASE")
    out := os.Getenv("FSSQL_COMPARE_OUT")
    if path == "" || out == "" {
        t.Skip("set FSSQL_COMPARE_CASE and FSSQL_COMPARE_OUT")
    }
    raw, err := os.ReadFile(path)
    if err != nil { t.Fatal(err) }
    var scene compareCase
    if err := json.Unmarshal(raw, &scene); err != nil { t.Fatal(err) }
    config := compareConfig(t, scene)
    db, err := Open(config)
    if err != nil { t.Fatal(err) }
    defer db.Close()

    rows := make([][]any, 0, max(1, int(scene.ExpectedRows)))
    _, err = db.QueryChunks(context.Background(), scene.SQL, func(chunk *Chunk) error {
        for r := 0; r < chunk.RowCount(); r++ {
            row := make([]any, chunk.ColumnCount())
            for c := 0; c < chunk.ColumnCount(); c++ {
                isNull, err := chunk.IsNull(r, c)
                if err != nil { return err }
                if isNull {
                    row[c] = nil
                    continue
                }
                typ, err := chunk.Type(c)
                if err != nil { return err }
                switch typ.ID {
                case "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "DATE", "TIMESTAMP":
                    v, ok, err := chunk.Int64(r, c)
                    if err != nil || !ok {
                        return fmt.Errorf("int64 col %d: ok=%v err=%v", c, ok, err)
                    }
                    row[c] = v
                case "FLOAT", "DOUBLE":
                    v, ok, err := chunk.Float64(r, c)
                    if err != nil || !ok {
                        return fmt.Errorf("float64 col %d: ok=%v err=%v", c, ok, err)
                    }
                    row[c] = v
                case "VARCHAR":
                    v, ok, err := chunk.String(r, c)
                    if err != nil || !ok {
                        return fmt.Errorf("string col %d: ok=%v err=%v", c, ok, err)
                    }
                    row[c] = v
                case "BOOLEAN":
                    v, ok, err := chunk.Bool(r, c)
                    if err != nil || !ok {
                        return fmt.Errorf("bool col %d: ok=%v err=%v", c, ok, err)
                    }
                    row[c] = v
                default:
                    return fmt.Errorf("unsupported verification type %s at col %d", typ.ID, c)
                }
            }
            rows = append(rows, row)
        }
        return nil
    })
    if err != nil { t.Fatal(err) }
    if int64(len(rows)) != scene.ExpectedRows {
        t.Fatalf("rows=%d want=%d", len(rows), scene.ExpectedRows)
    }
    encoded, err := json.Marshal(rows)
    if err != nil { t.Fatal(err) }
    if err := os.WriteFile(out, append(encoded, '\n'), 0o600); err != nil { t.Fatal(err) }
}
'''


def run(cmd, *, cwd=None, env=None, timeout=None):
    return subprocess.run(
        cmd, cwd=cwd, env=env, text=True, capture_output=True,
        timeout=timeout, check=True
    )


def cpu_model():
    try:
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
            if line.lower().startswith("model name"):
                return line.split(":", 1)[1].strip()
    except Exception:
        pass
    return platform.processor() or "unknown"


def total_memory_bytes():
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    except Exception:
        pass
    return None


def rss_bytes():
    try:
        for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    except Exception:
        pass
    return None


def dir_bytes(path: Path):
    total = 0
    if not path.exists():
        return 0
    for p in path.rglob("*"):
        try:
            if p.is_file():
                total += p.stat().st_size
        except FileNotFoundError:
            pass
    return total


class Sampler:
    def __init__(self, tempdir: Path):
        self.tempdir = tempdir
        self.stop = threading.Event()
        self.rss_base = rss_bytes() or 0
        self.rss_peak = self.rss_base
        self.temp_peak = 0
        self.thread = threading.Thread(target=self._loop, daemon=True)

    def _loop(self):
        while not self.stop.is_set():
            value = rss_bytes()
            if value:
                self.rss_peak = max(self.rss_peak, value)
            self.temp_peak = max(self.temp_peak, dir_bytes(self.tempdir))
            self.stop.wait(0.02)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join()
        return False


def sql_quote(path: Path):
    return str(path.resolve()).replace("'", "''")


def generate_fixture(root: Path):
    if root.exists():
        shutil.rmtree(root)
    parts = root / "parts"
    parts.mkdir(parents=True)
    seed = root / "seed.parquet"
    dim = root / "dim.parquet"
    dim_small = root / "dim-small.parquet"

    bigint_cols = [
        "CAST(i AS BIGINT) AS id",
        "CAST(i % 1000 AS BIGINT) AS grp",
    ]
    for j in range(2, 16):
        bigint_cols.append(
            f"CASE WHEN i % 10 = 0 THEN NULL ELSE CAST((i * {j + 3}) % 10000019 AS BIGINT) END AS b{j:02d}"
        )

    double_cols = [
        "CAST(((i * 48271) % 1000003) AS DOUBLE) / 4.0 AS value",
    ]
    for j in range(1, 16):
        double_cols.append(
            f"CASE WHEN i % 10 = 0 THEN NULL ELSE CAST(((i * {j + 11}) % 1000003) AS DOUBLE) / {j + 1}.0 END AS d{j:02d}"
        )

    varchar_cols = [
        "concat('acct-', lpad(CAST(i % 100000 AS VARCHAR), 6, '0'), '-中文') AS payload",
    ]
    for j in range(1, 16):
        varchar_cols.append(
            f"CASE WHEN i % 10 = 0 THEN NULL ELSE concat('s{j:02d}-', lpad(CAST((i + {j}) % 1024 AS VARCHAR), 4, '0')) END AS s{j:02d}"
        )

    bool_cols = []
    for j in range(16):
        bool_cols.append(
            f"CASE WHEN i % 10 = 0 THEN NULL ELSE ((i + {j}) % 2 = 0) END AS flag{j:02d}"
        )

    cols = bigint_cols + double_cols + varchar_cols + bool_cols
    if len(cols) != 64:
        raise RuntimeError(f"schema has {len(cols)} columns, expected 64")

    con = duckdb.connect()
    try:
        con.execute("SET threads=1")
        con.execute("SET memory_limit='1GB'")
        select_sql = "SELECT " + ",\n".join(cols) + f"\nFROM range({ROWS_PER_SHARD}) AS t(i)"
        con.execute(
            f"COPY ({select_sql}) TO '{sql_quote(seed)}' "
            "(FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 32768)"
        )
        con.execute(
            f"COPY (SELECT CAST(i AS BIGINT) AS grp, CAST(i AS DOUBLE) * 0.5 + 1.0 AS weight "
            f"FROM range(1000) t(i)) TO '{sql_quote(dim)}' "
            "(FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 32768)"
        )
        con.execute(
            f"COPY (SELECT CAST(i AS BIGINT) AS grp FROM range(500) t(i)) TO '{sql_quote(dim_small)}' "
            "(FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 32768)"
        )
    finally:
        con.close()

    for i in range(SHARDS):
        os.link(seed, parts / f"part-{i:03d}.parquet")

    inodes = {p.stat().st_ino for p in parts.glob("*.parquet")}
    return {
        "root": root,
        "parts": parts,
        "seed": seed,
        "dim": dim,
        "dim_small": dim_small,
        "seed_bytes": seed.stat().st_size,
        "hardlink_inodes": len(inodes),
        "part_files": SHARDS,
        "schema_columns": len(cols),
    }


def write_verify_test(repo: Path):
    path = repo / "pkg/gosql/zz_wide100m_verify_test.go"
    path.write_text(VERIFY_TEST, encoding="utf-8", newline="\n")
    return path


def build_binary(repo: Path, evidence: Path):
    write_verify_test(repo)
    binary = evidence / "gosql-wide100m.test"
    env = dict(os.environ)
    env["GOCACHE"] = str(evidence / "go-build")
    result = subprocess.run(
        ["go", "test", "-c", "-o", str(binary), "./pkg/gosql"],
        cwd=repo, env=env, text=True, capture_output=True, timeout=900
    )
    (evidence / "build.log").write_text(
        result.stdout + "\n--- STDERR ---\n" + result.stderr,
        encoding="utf-8"
    )
    if result.returncode != 0:
        raise RuntimeError("go test -c failed; see build.log")
    return binary


def case_payload(case, sql):
    return {
        "name": case["name"],
        "sql": sql,
        "source": "local",
        "expected_rows": case["rows"],
        "memory_limit": MEMORY_LIMIT,
        "spill_budget": SPILL_BUDGET,
        "temp_limit": TEMP_LIMIT,
        "workers": 1,
    }


def run_fssql(repo: Path, binary: Path, root: Path, evidence: Path, case, sql, tag, verify=False):
    case_dir = evidence / "cases"
    raw_dir = evidence / "raw"
    log_dir = evidence / "logs"
    for directory in (case_dir, raw_dir, log_dir):
        directory.mkdir(exist_ok=True)
    case_path = case_dir / f"{tag}.json"
    out_path = raw_dir / f"{tag}.json"
    case_path.write_text(
        json.dumps(case_payload(case, sql), indent=2) + "\n",
        encoding="utf-8"
    )
    if out_path.exists():
        out_path.unlink()

    env = dict(os.environ)
    env["FSSQL_COMPARE_CASE"] = str(case_path)
    env["FSSQL_COMPARE_OUT"] = str(out_path)
    env["FSSQL_COMPARE_ROOT"] = str(root)
    env["GOCACHE"] = str(evidence / "go-build")
    temp = evidence / "fssql-tmp"
    temp.mkdir(exist_ok=True)
    env["TMPDIR"] = str(temp)
    env["TEMP"] = str(temp)
    env["TMP"] = str(temp)

    test_name = "^TestWide100MVerifyCase$" if verify else "^TestDuckDBCompareCase$"
    started = time.perf_counter()
    try:
        result = subprocess.run(
            [str(binary), "-test.run", test_name, "-test.v"],
            cwd=repo, env=env, text=True, capture_output=True,
            timeout=QUERY_TIMEOUT_SECONDS
        )
        elapsed = time.perf_counter() - started
        (log_dir / f"{tag}.log").write_text(
            result.stdout + "\n--- STDERR ---\n" + result.stderr,
            encoding="utf-8"
        )
    except subprocess.TimeoutExpired as exc:
        elapsed = time.perf_counter() - started
        (log_dir / f"{tag}.log").write_text(
            (exc.stdout or "") + "\n--- STDERR ---\n" + (exc.stderr or "") +
            f"\nTIMEOUT after {elapsed:.3f}s\n",
            encoding="utf-8"
        )
        raise RuntimeError(f"fssql timeout after {elapsed:.3f}s")
    if result.returncode != 0 or not out_path.exists():
        raise RuntimeError(
            f"fssql failed exit={result.returncode} elapsed={elapsed:.3f}s; see logs/{tag}.log"
        )
    return json.loads(out_path.read_text(encoding="utf-8").splitlines()[0])


def run_duckdb(case, sql, evidence: Path, tag: str):
    tempdir = evidence / "duckdb-temp" / tag
    if tempdir.exists():
        shutil.rmtree(tempdir)
    tempdir.mkdir(parents=True)
    con = duckdb.connect()
    try:
        con.execute("SET threads=1")
        con.execute("SET memory_limit='256MB'")
        con.execute("SET temp_directory=?", [str(tempdir)])
        con.execute("SET preserve_insertion_order=false")
        cpu_before = time.process_time()
        with Sampler(tempdir) as sampler:
            started = time.perf_counter()
            reader = con.sql(sql).to_arrow_reader(2048)
            rows = 0
            for batch in reader:
                rows += batch.num_rows
            reader.close()
            seconds = time.perf_counter() - started
        cpu_seconds = time.process_time() - cpu_before
        if rows != case["rows"]:
            raise RuntimeError(f"DuckDB rows={rows} want={case['rows']}")
        return {
            "name": case["name"],
            "source": "local",
            "seconds": seconds,
            "rows": rows,
            "cpu_seconds": cpu_seconds,
            "cpu_percent": (cpu_seconds / seconds * 100.0) if seconds else None,
            "rss_base_bytes": sampler.rss_base,
            "rss_peak_bytes": sampler.rss_peak,
            "temp_peak_bytes": sampler.temp_peak,
            "memory_limit_bytes": MEMORY_LIMIT,
        }
    finally:
        con.close()
        shutil.rmtree(tempdir, ignore_errors=True)


def duckdb_rows(sql):
    con = duckdb.connect()
    try:
        con.execute("SET threads=1")
        con.execute("SET memory_limit='256MB'")
        return [list(row) for row in con.execute(sql).fetchall()]
    finally:
        con.close()


def same_value(a, b):
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, bool) or isinstance(b, bool):
        return a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        if isinstance(a, float) or isinstance(b, float):
            return math.isclose(float(a), float(b), rel_tol=1e-12, abs_tol=1e-9)
        return int(a) == int(b)
    return a == b


def same_rows(left, right):
    if len(left) != len(right):
        return False
    for lrow, rrow in zip(left, right):
        if len(lrow) != len(rrow):
            return False
        if not all(same_value(a, b) for a, b in zip(lrow, rrow)):
            return False
    return True


def median_mad(values):
    median = statistics.median(values)
    mad = statistics.median(abs(value - median) for value in values)
    return median, mad, (mad / median if median else None)


def summarize(samples, correctness):
    summary = []
    for case in CASES:
        row = {
            "case": case["name"],
            "label": case["label"],
            "engines": {},
            "correctness": correctness.get(case["name"], {}),
        }
        for engine in ("fssql", "duckdb"):
            records = [
                record for record in samples
                if record["case"] == case["name"]
                and record["engine"] == engine
                and not record.get("error")
            ]
            times = [record["seconds"] for record in records]
            if not times:
                continue
            median, mad, mad_ratio = median_mad(times)
            cpu_values = [
                record["cpu_percent"] for record in records
                if record.get("cpu_percent") is not None
            ]
            row["engines"][engine] = {
                "n": len(times),
                "median": median,
                "mad": mad,
                "mad_ratio": mad_ratio,
                "min": min(times),
                "max": max(times),
                "rss_peak_bytes_max": max(
                    (record.get("rss_peak_bytes") or 0) for record in records
                ),
                "temp_peak_bytes_max": max(
                    (record.get("temp_peak_bytes") or 0) for record in records
                ),
                "managed_peak_bytes_max": max(
                    (record.get("managed_peak_bytes") or 0) for record in records
                ),
                "cpu_percent_median": (
                    statistics.median(cpu_values) if cpu_values else None
                ),
            }
        if "fssql" in row["engines"] and "duckdb" in row["engines"]:
            row["time_ratio"] = (
                row["engines"]["fssql"]["median"] /
                row["engines"]["duckdb"]["median"]
            )
            row["valid"] = (
                row["correctness"].get("match") is True
                and row["engines"]["fssql"]["n"] == SAMPLES
                and row["engines"]["duckdb"]["n"] == SAMPLES
                and row["engines"]["fssql"]["mad_ratio"] <= 0.10
                and row["engines"]["duckdb"]["mad_ratio"] <= 0.10
            )
        else:
            row["valid"] = False
        summary.append(row)
    return summary


def fmt_seconds(value):
    if value is None:
        return "—"
    if value < 1:
        return f"{value * 1000:.1f}ms"
    return f"{value:.3f}s"


def fmt_pct(value):
    return "—" if value is None else f"{value * 100:.1f}%"


def fmt_mib(value):
    return "—" if not value else f"{value / MIB:.1f}"


def report_text(meta, summary):
    lines = [
        "# JOIN / TopN 亿级宽表 DuckDB 对比复测（2026-11-28）",
        "",
        f"生产基线：{meta['base_sha']}；benchmark 分支 HEAD：{meta['head_sha']}。",
        "",
        "## 口径",
        "",
        "- 查询集合沿用 2026-11-11 报告的四类：INNER JOIN + COUNT/SUM、LEFT JOIN + COUNT、数值 TopN(K=100)、VARCHAR TopN(K=100)。",
        "- fssql 使用 QueryChunks 完整消费；DuckDB 使用 Arrow reader 完整消费。双方单 worker/thread、256 MiB 内存，串行执行。",
        "- 每个用例 2 次预热、7 次正式样本；每轮两个引擎都执行，起始顺序交替。表中使用中位数；MAD/中位数 > 10% 判为无效。",
        "- DuckDB 2.0.0，PyArrow 24.0.0；本轮运行在 GitHub Actions Ubuntu runner，不与 2026-11-11 的 Windows/i7-10510U 绝对时间直接比较。",
        "",
        "## 数据",
        "",
        f"- 逻辑事实表：{LOGICAL_ROWS:,} 行 × 64 列（16 BIGINT、16 DOUBLE、16 VARCHAR、16 BOOLEAN）。查询键/排序列非 NULL，其余宽列约 10% NULL。",
        f"- 物理 fixture：1,000,000 行确定性 Snappy Parquet seed（{meta['seed_bytes'] / MIB:.2f} MiB）+ 100 个硬链接；hardlink inode 数={meta['hardlink_inodes']}。",
        "- 因此这是实际 1e8 逻辑行扫描/解码/算子测试，但不是 100 份独立物理数据的冷盘吞吐测试。沿用旧基准的 warm-cache 思路，不主动清 OS page cache。",
        "- 维表：1000 行；LEFT JOIN 小维表：500 行。事实表 grp=0..999 均匀分布。",
        "",
        "## 环境",
        "",
        f"- OS：{meta['platform']}",
        f"- CPU：{meta['cpu_model']}；逻辑 CPU={meta['cpu_count']}",
        (
            f"- RAM：{meta['memory_bytes'] / GIB:.1f} GiB"
            if meta.get("memory_bytes") else "- RAM：未采集"
        ),
        f"- Go：{meta['go_version']}",
        f"- DuckDB：{meta['duckdb_version']}；PyArrow：{meta['pyarrow_version']}",
        "",
        "## 结果",
        "",
        "| 查询 | fssql 中位 | DuckDB 中位 | fssql / DuckDB | fssql MAD | DuckDB MAD | 值级校验 | 状态 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for row in summary:
        fssql = row["engines"].get("fssql", {})
        duck = row["engines"].get("duckdb", {})
        lines.append(
            "| {label} | {fm} | {dm} | {ratio} | {fmad} | {dmad} | {check} | {status} |".format(
                label=row["label"],
                fm=fmt_seconds(fssql.get("median")),
                dm=fmt_seconds(duck.get("median")),
                ratio=(
                    "—" if row.get("time_ratio") is None
                    else f"{row['time_ratio']:.2f}x"
                ),
                fmad=fmt_pct(fssql.get("mad_ratio")),
                dmad=fmt_pct(duck.get("mad_ratio")),
                check=(
                    "PASS" if row.get("correctness", {}).get("match") else "FAIL"
                ),
                status=("有效" if row.get("valid") else "无效"),
            )
        )

    lines += [
        "",
        "## 资源峰值",
        "",
        "| 查询 | fssql RSS MiB | fssql 受管峰值 MiB | fssql temp MiB | DuckDB RSS MiB | DuckDB temp MiB |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summary:
        fssql = row["engines"].get("fssql", {})
        duck = row["engines"].get("duckdb", {})
        lines.append(
            f"| {row['label']} | {fmt_mib(fssql.get('rss_peak_bytes_max'))} | "
            f"{fmt_mib(fssql.get('managed_peak_bytes_max'))} | "
            f"{fmt_mib(fssql.get('temp_peak_bytes_max'))} | "
            f"{fmt_mib(duck.get('rss_peak_bytes_max'))} | "
            f"{fmt_mib(duck.get('temp_peak_bytes_max'))} |"
        )

    lines += [
        "",
        "## 与 2026-11-11 基线的关系",
        "",
        "旧基线是 200,000 行、本地 Windows；本轮是 100,000,000 行 × 64 列、Linux runner。只能比较同轮 fssql/DuckDB 比值和稳定性，不能把绝对毫秒数做跨机回归。",
        "",
        "| 查询 | 2026-11-11 比值 | 本轮比值 | 备注 |",
        "| --- | ---: | ---: | --- |",
    ]
    for row in summary:
        old = OLD_BASELINE[row["case"]]
        old_ratio = (
            f"{old['ratio']:.2f}x"
            if old["ratio"] is not None else "无有效倍数"
        )
        new_ratio = (
            f"{row['time_ratio']:.2f}x"
            if row.get("time_ratio") is not None else "—"
        )
        note = (
            "本轮有效"
            if row.get("valid")
            else "本轮未过完整样本/值校验/10% MAD 门槛"
        )
        lines.append(
            f"| {row['label']} | {old_ratio} | {new_ratio} | {note} |"
        )

    valid_rows = [row for row in summary if row.get("valid")]
    lines += ["", "## 结论", ""]
    if valid_rows:
        ratios = [row["time_ratio"] for row in valid_rows]
        lines += [
            f"- 有效用例 {len(valid_rows)}/{len(summary)}；有效用例的 fssql/DuckDB 中位比值范围为 {min(ratios):.2f}x–{max(ratios):.2f}x。",
            "- 某项被标为无效时，不发布该项性能倍数；原始样本仍保留用于诊断。",
            "- 100 个分片是同一 seed 的硬链接，结果适合比较 1e8 逻辑行规模下的解码、JOIN/TopN 与 warm-cache 路径；不能据此外推冷存储吞吐或 100 份独立宽表的 I/O 容量。",
        ]
    else:
        lines += [
            "- 没有用例同时满足完整 7 样本、值级校验和 10% MAD 稳定性门槛，因此本轮不发布有效性能倍数。"
        ]

    lines += [
        "",
        "## 证据",
        "",
        "- samples.jsonl：全部正式样本；all-runs.jsonl：预热 + 正式样本。",
        "- correctness.json：fssql 与 DuckDB 值级结果对比。",
        "- summary.json：中位数、MAD、比值、资源峰值和有效性。",
        "- metadata.json：commit、环境、fixture 指纹和逻辑规模。",
        "- logs/、raw/、cases/：fssql 单次执行日志与原始输出。",
        "",
    ]
    return "\n".join(lines)


def main():
    repo = Path(__file__).resolve().parents[1]
    evidence = repo / "artifacts/bench-wide-100m"
    if evidence.exists():
        shutil.rmtree(evidence)
    evidence.mkdir(parents=True)

    fixture = generate_fixture(evidence / "fixture")
    parts_uri = fixture["parts"].resolve().as_uri() + "/*.parquet"
    dim_uri = fixture["dim"].resolve().as_uri()
    dim_small_uri = fixture["dim_small"].resolve().as_uri()
    paths = {
        "parts": parts_uri,
        "dim": dim_uri,
        "dim_small": dim_small_uri,
    }

    con = duckdb.connect()
    try:
        logical_rows = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{parts_uri}')"
        ).fetchone()[0]
        schema_columns = len(
            con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{fixture['seed'].resolve().as_uri()}')"
            ).fetchall()
        )
    finally:
        con.close()
    if logical_rows != LOGICAL_ROWS or schema_columns != 64:
        raise RuntimeError(
            f"fixture mismatch rows={logical_rows} cols={schema_columns}"
        )

    binary = build_binary(repo, evidence)

    all_runs = []
    samples = []
    for case_index, case in enumerate(CASES):
        sql = case["sql"].format(**paths)
        for round_index in range(WARMUPS + SAMPLES):
            measured = round_index >= WARMUPS
            engines = ["fssql", "duckdb"]
            if (round_index + case_index) % 2:
                engines.reverse()
            for engine in engines:
                tag = (
                    f"{case['name']}-"
                    f"{'sample' if measured else 'warmup'}-"
                    f"{round_index:02d}-{engine}"
                )
                record = {
                    "case": case["name"],
                    "engine": engine,
                    "round": round_index,
                    "measured": measured,
                }
                try:
                    if engine == "fssql":
                        raw = run_fssql(
                            repo, binary, fixture["root"], evidence,
                            case, sql, tag, verify=False
                        )
                    else:
                        raw = run_duckdb(case, sql, evidence, tag)
                    record.update(raw)
                    record["case"] = case["name"]
                    record["engine"] = engine
                except Exception as exc:
                    record["error"] = f"{type(exc).__name__}: {exc}"
                all_runs.append(record)
                with (evidence / "all-runs.jsonl").open(
                    "a", encoding="utf-8"
                ) as output:
                    output.write(
                        json.dumps(record, ensure_ascii=False) + "\n"
                    )
                if measured:
                    samples.append(record)
                    with (evidence / "samples.jsonl").open(
                        "a", encoding="utf-8"
                    ) as output:
                        output.write(
                            json.dumps(record, ensure_ascii=False) + "\n"
                        )
                print(
                    json.dumps(
                        {
                            "case": case["name"],
                            "engine": engine,
                            "measured": measured,
                            "round": round_index,
                            "seconds": record.get("seconds"),
                            "error": record.get("error"),
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )

    correctness = {}
    for case in CASES:
        sql = case["sql"].format(**paths)
        tag = f"verify-{case['name']}"
        try:
            fssql_rows = run_fssql(
                repo, binary, fixture["root"], evidence,
                case, sql, tag, verify=True
            )
            duck_rows = duckdb_rows(sql)
            correctness[case["name"]] = {
                "match": same_rows(fssql_rows, duck_rows),
                "fssql_rows": fssql_rows,
                "duckdb_rows": duck_rows,
            }
        except Exception as exc:
            correctness[case["name"]] = {
                "match": False,
                "error": f"{type(exc).__name__}: {exc}",
            }
        print(
            "VERIFY",
            case["name"],
            correctness[case["name"]]["match"],
            flush=True,
        )

    expected_scalars = {
        "inner_join_count_sum": [[100_000_000, 25_075_000_000.0]],
        "left_join_count": [[100_000_000, 50_000_000]],
    }
    for name, expected in expected_scalars.items():
        check = correctness.get(name, {})
        if check.get("match"):
            check["analytical_expected"] = expected
            check["analytical_expected_match"] = same_rows(
                check["duckdb_rows"], expected
            )
            check["match"] = (
                check["match"] and check["analytical_expected_match"]
            )

    summary = summarize(samples, correctness)

    try:
        base_sha = run(
            ["git", "merge-base", "HEAD", "origin/main"],
            cwd=repo
        ).stdout.strip()
    except Exception:
        base_sha = "unknown"
    head_sha = run(
        ["git", "rev-parse", "HEAD"], cwd=repo
    ).stdout.strip()
    go_version = run(
        ["go", "version"], cwd=repo
    ).stdout.strip()

    meta = {
        "base_sha": base_sha,
        "head_sha": head_sha,
        "platform": platform.platform(),
        "cpu_model": cpu_model(),
        "cpu_count": os.cpu_count(),
        "memory_bytes": total_memory_bytes(),
        "go_version": go_version,
        "duckdb_version": duckdb.__version__,
        "pyarrow_version": pyarrow.__version__,
        "logical_rows": LOGICAL_ROWS,
        "schema_columns": 64,
        "rows_per_shard": ROWS_PER_SHARD,
        "shards": SHARDS,
        "seed_bytes": fixture["seed_bytes"],
        "hardlink_inodes": fixture["hardlink_inodes"],
        "warmups": WARMUPS,
        "samples": SAMPLES,
        "memory_limit_bytes": MEMORY_LIMIT,
        "spill_budget_bytes": SPILL_BUDGET,
        "temp_limit_bytes": TEMP_LIMIT,
    }
    (evidence / "metadata.json").write_text(
        json.dumps(meta, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8"
    )
    (evidence / "correctness.json").write_text(
        json.dumps(correctness, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8"
    )
    (evidence / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8"
    )
    report = report_text(meta, summary)
    (evidence / "report.md").write_text(
        report, encoding="utf-8", newline="\n"
    )
    print(report, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

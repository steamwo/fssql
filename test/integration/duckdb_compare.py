"""在本地文件与真实 MinIO 上交替对比 fssql 与 DuckDB 的端到端查询耗时。

只调用公共 Go API 和 DuckDB Python 绑定；不修改引擎代码。
每个场景默认 2 次预热、7 个样本，两引擎交替执行，保留全部原始样本。
"""

import argparse
import ctypes
import json
import os
from pathlib import Path
import random
import statistics
import subprocess
import sys
import threading
import time
from ctypes import wintypes
from urllib.parse import urlparse

import duckdb

MIB = 1 << 20

# CORE_CASES 是本轮计时对比的九个端到端场景。
CORE_CASES = [
    {"name": "scan_narrow", "sql": "SELECT id, grp, value FROM read_parquet('{data}')", "rows": 1000000},
    {"name": "scan_wide", "sql": "SELECT id, payload FROM read_parquet('{data}')", "rows": 1000000},
    {"name": "filter_range", "sql": "SELECT id, value FROM read_parquet('{data}') WHERE id >= 500000 AND id < 501000", "rows": 1000},
    {"name": "sort_spill", "sql": "SELECT sort_key, id FROM read_parquet('{data}') ORDER BY sort_key, id", "rows": 1000000},
    {"name": "sort_in_memory", "sql": "SELECT sort_key, id FROM read_parquet('{data}') ORDER BY sort_key, id", "rows": 1000000, "memory": 256 << 20, "spill": 128 << 20},
    {"name": "topn", "sql": "SELECT id, value FROM read_parquet('{data}') ORDER BY value DESC, id ASC LIMIT 100", "rows": 100},
    {"name": "offset", "sql": "SELECT id, value FROM read_parquet('{data}') ORDER BY id LIMIT 100 OFFSET 100000", "rows": 100},
    {"name": "global_aggregate", "sql": "SELECT COUNT(*), COUNT(nullable_value), SUM(value), AVG(value), MIN(value), MAX(value) FROM read_parquet('{data}')", "rows": 1},
    {"name": "group_having", "sql": "SELECT grp, COUNT(*), SUM(value), AVG(value) FROM read_parquet('{data}') GROUP BY grp HAVING COUNT(*) >= 100 ORDER BY grp", "rows": 1000},
    {"name": "inner_join", "sql": "SELECT COUNT(*), SUM(d.weight) FROM read_parquet('{data}') f JOIN read_parquet('{dim}') d ON f.grp = d.grp", "rows": 1},
    {"name": "multi_file", "sql": "SELECT COUNT(*), SUM(value) FROM read_parquet('{parts}')", "rows": 1},
]

# FUNCTION_CASES 只做一次功能可用性与行数核对，不进入计时统计。
FUNCTION_CASES = [
    {"name": "distinct", "sql": "SELECT DISTINCT grp FROM read_parquet('{data}') ORDER BY grp", "rows": 1000},
    {"name": "cte", "sql": "WITH a AS (SELECT grp, value FROM read_parquet('{data}') WHERE id < 10000) SELECT grp, SUM(value) FROM a GROUP BY grp ORDER BY grp", "rows": 1000},
    {"name": "scalar_subquery", "sql": "SELECT id FROM read_parquet('{data}') WHERE id < (SELECT MAX(grp) FROM read_parquet('{dim}')) ORDER BY id", "rows": 999},
    {"name": "left_join_nulls", "sql": "SELECT COUNT(*), COUNT(d.grp) FROM read_parquet('{data}') f LEFT JOIN read_parquet('{dim_small}') d ON f.grp = d.grp WHERE f.id < 10000", "rows": 1},
    {"name": "semi_join", "sql": "SELECT COUNT(*) FROM read_parquet('{data}') f SEMI JOIN read_parquet('{dim_small}') d ON f.grp = d.grp", "rows": 1},
    {"name": "anti_join", "sql": "SELECT COUNT(*) FROM read_parquet('{data}') f ANTI JOIN read_parquet('{dim_small}') d ON f.grp = d.grp", "rows": 1},
    {"name": "offset", "sql": "SELECT id, value FROM read_parquet('{data}') ORDER BY id LIMIT 100 OFFSET 100000", "rows": 100},
    {"name": "csv_scan", "fssql": "SELECT column0, column2 FROM read_csv('{csv}') WHERE column0 >= 999000", "duckdb": "SELECT column0, column2 FROM read_csv('{csv}', header=false) WHERE column0 >= 999000", "rows": 1000, "preserve_order": False},
    {"name": "ndjson_scan", "fssql": "SELECT id, value FROM read_ndjson('{ndjson}') WHERE id >= 999000", "duckdb": "SELECT id, value FROM read_json_auto('{ndjson}') WHERE id >= 999000", "rows": 1000, "preserve_order": False},
    {"name": "json_array", "sql": "SELECT id, grp, value FROM read_json('{json}') WHERE grp < 50", "rows": 500},
]


# PROCESS_MEMORY_COUNTERS 对应 Win32 进程内存计数，只读取当前进程。
class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
    _fields_ = [
        ("cb", wintypes.DWORD),
        ("PageFaultCount", wintypes.DWORD),
        ("PeakWorkingSetSize", ctypes.c_size_t),
        ("WorkingSetSize", ctypes.c_size_t),
        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
        ("PagefileUsage", ctypes.c_size_t),
        ("PeakPagefileUsage", ctypes.c_size_t),
    ]


class FILETIME(ctypes.Structure):
    _fields_ = [("dwLowDateTime", wintypes.DWORD), ("dwHighDateTime", wintypes.DWORD)]


def _load_win32():
    """加载进程内存与 CPU 查询函数；非 Windows 返回 None。"""
    if os.name != "nt":
        return None
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    get_memory = getattr(kernel32, "K32GetProcessMemoryInfo", None)
    if get_memory is None:
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        get_memory = psapi.GetProcessMemoryInfo
    get_memory.argtypes = [wintypes.HANDLE, ctypes.POINTER(PROCESS_MEMORY_COUNTERS), wintypes.DWORD]
    get_memory.restype = wintypes.BOOL
    kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    kernel32.GetProcessTimes.argtypes = [
        wintypes.HANDLE, ctypes.POINTER(FILETIME), ctypes.POINTER(FILETIME),
        ctypes.POINTER(FILETIME), ctypes.POINTER(FILETIME),
    ]
    kernel32.GetProcessTimes.restype = wintypes.BOOL
    return kernel32, get_memory


WIN32 = _load_win32()


def process_rss():
    """返回当前进程工作集字节数；不可用时返回 None。"""
    if WIN32 is None:
        return None
    kernel32, get_memory = WIN32
    counters = PROCESS_MEMORY_COUNTERS()
    counters.cb = ctypes.sizeof(counters)
    if not get_memory(kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb):
        return None
    return int(counters.WorkingSetSize)


def _filetime_seconds(value):
    return ((value.dwHighDateTime << 32) | value.dwLowDateTime) / 1e7


def process_cpu_seconds():
    """返回当前进程用户态与内核态 CPU 秒数；不可用时返回 None。"""
    if WIN32 is None:
        return None
    kernel32 = WIN32[0]
    created, exited, kernel, user = FILETIME(), FILETIME(), FILETIME(), FILETIME()
    if not kernel32.GetProcessTimes(kernel32.GetCurrentProcess(), ctypes.byref(created),
                                    ctypes.byref(exited), ctypes.byref(kernel), ctypes.byref(user)):
        return None
    return _filetime_seconds(kernel) + _filetime_seconds(user)


class ResourceSampler:
    """按 20ms 采样当前进程 RSS，避免把进程历史峰值当成单次查询峰值。"""

    def __init__(self):
        self.stop = threading.Event()
        self.peak = 0
        self.base = process_rss() or 0
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self.stop.is_set():
            value = process_rss()
            if value and value > self.peak:
                self.peak = value
            self.stop.wait(0.02)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join()
        return False


def load_settings(repo):
    """读取已忽略目录中的 MinIO 测试配置。"""
    path = repo / ".cache/agent-verification/minio-test-env.json"
    return json.loads(path.read_text(encoding="utf-8"))


def file_uri(path):
    """返回 Windows 绝对路径对应的规范 file URI。"""
    return Path(path).resolve().as_uri()


def source_paths(root, bucket, source):
    """按来源生成 SQL 中使用的位置占位值。"""
    if source == "local":
        base = file_uri(root)
    else:
        base = "s3://%s/large" % bucket
    return {
        "data": base + "/data.parquet",
        "dim": base + "/dim.parquet",
        "dim_small": base + "/dim-small.parquet",
        "parts": base + "/parts/*.parquet",
        "csv": base + "/data.csv",
        "ndjson": base + "/data.ndjson",
        "json": base + "/small.json",
    }


def duckdb_sql(sql):
    """把 fssql 的 SEMI/ANTI 语法改写为 DuckDB 等价 EXISTS 形式。"""
    for keyword, predicate in (("SEMI", "EXISTS"), ("ANTI", "NOT EXISTS")):
        marker = " f %s JOIN " % keyword
        if marker not in sql:
            continue
        left, rest = sql.split(marker, 1)
        right, _ = rest.split(" ON ", 1)
        left = left.split("SELECT COUNT(*) FROM ", 1)[1]
        right = right.strip()
        if right.endswith(" d"):
            right = right[:-2]
        return "SELECT COUNT(*) FROM %s f WHERE %s (SELECT 1 FROM %s d WHERE f.grp = d.grp)" % (
            left, predicate, right)
    return sql


def configure_duckdb(connection, settings, case, source, tempdir):
    """为单次测量建立与 fssql 相同上限的 DuckDB 连接。"""
    connection.execute("SET threads=%d" % case.get("workers", 1))
    connection.execute("SET memory_limit='%dMB'" % max(1, case.get("memory", 64 << 20) // MIB))
    connection.execute("SET temp_directory=?", [str(tempdir)])
    if case.get("preserve_order") is False:
        connection.execute("SET preserve_insertion_order=false")
    if source == "s3":
        endpoint = urlparse(settings["FSSQL_MINIO_ENDPOINT"])
        connection.execute("LOAD httpfs")
        connection.execute("SET s3_endpoint=?", ["%s:%s" % (endpoint.hostname, endpoint.port or 9000)])
        connection.execute("SET s3_use_ssl=%s" % ("true" if endpoint.scheme == "https" else "false"))
        connection.execute("SET s3_url_style='path'")
        connection.execute("SET s3_access_key_id=?", [settings["FSSQL_MINIO_ACCESS_KEY"]])
        connection.execute("SET s3_secret_access_key=?", [settings["FSSQL_MINIO_SECRET_KEY"]])


def measure_duckdb(settings, case, source, sql, tempdir):
    """完整消费 Arrow reader 并返回单次耗时与资源。"""
    connection = duckdb.connect()
    try:
        configure_duckdb(connection, settings, case, source, tempdir)
        cpu_before = process_cpu_seconds()
        with ResourceSampler() as sampler:
            start = time.perf_counter()
            reader = connection.sql(sql).to_arrow_reader(2048)
            rows = 0
            for batch in reader:
                rows += batch.num_rows
            reader.close()
            seconds = time.perf_counter() - start
        cpu_after = process_cpu_seconds()
        return {
            "engine": "duckdb", "source": source, "case": case["name"],
            "seconds": seconds, "rows": rows,
            "cpu_seconds": (cpu_after - cpu_before) if cpu_before is not None and cpu_after is not None else None,
            "rss_base_bytes": sampler.base or None, "rss_peak_bytes": sampler.peak or None,
            "memory_limit_bytes": case.get("memory", 64 << 20),
        }
    finally:
        connection.close()


def run_fssql(repo, binary, settings, case, source, root, case_path, out_path):
    """执行已编译的 Go 测试二进制，读取单个场景结果。"""
    if out_path.exists():
        out_path.unlink()
    env = dict(os.environ)
    env.update(settings)
    env["FSSQL_COMPARE_CASE"] = str(case_path)
    env["FSSQL_COMPARE_OUT"] = str(out_path)
    env["FSSQL_COMPARE_ROOT"] = str(root)
    env["GOCACHE"] = str(repo / ".cache/agent-verification/duckdb-compare-go-build")
    temp = Path(binary).parent / "tmp"
    temp.mkdir(parents=True, exist_ok=True)
    env["TEMP"] = env["TMP"] = str(temp)
    result = subprocess.run([str(binary), "-test.run", "^TestDuckDBCompareCase$", "-test.v"],
                            cwd=repo, env=env, capture_output=True, text=True, timeout=300)
    if not out_path.exists():
        raise RuntimeError("fssql case failed:\n%s\n%s" % (result.stdout[-2000:], result.stderr[-2000:]))
    record = json.loads(out_path.read_text(encoding="utf-8").splitlines()[0])
    record["engine"] = "fssql"
    record["case"] = case["name"]
    return record


def build_binary(repo, evidence):
    """编译一次 gosql 测试二进制，后续样本复用。"""
    binary = evidence / "gosql-compare.test.exe"
    env = dict(os.environ)
    env["GOCACHE"] = str(repo / ".cache/agent-verification/duckdb-compare-go-build")
    result = subprocess.run(["go", "test", "-c", "-o", str(binary), "./pkg/gosql"],
                            cwd=repo, env=env, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        raise RuntimeError("go test -c failed:\n%s\n%s" % (result.stdout[-4000:], result.stderr[-4000:]))
    return binary


def write_case_case(evidence, tag, case, source, sql):
    """保存场景输入，便于复现单次样本。"""
    folder = evidence / "cases"
    folder.mkdir(exist_ok=True)
    path = folder / (tag + ".json")
    payload = dict(case)
    payload.update({"source": source, "sql": sql})
    payload["expected_rows"] = payload.pop("rows")
    payload.setdefault("memory_limit", payload.get("memory", 64 << 20))
    payload.setdefault("spill_budget", payload.get("spill", 4 << 20))
    payload.setdefault("temp_limit", 1 << 30)
    payload.setdefault("workers", 1)
    payload.pop("memory", None)
    payload.pop("spill", None)
    payload.pop("fssql", None)
    payload.pop("duckdb", None)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")
    return path


def sample_engines(engines, rng):
    """按固定种子随机决定本次样本的引擎顺序。"""
    order = list(engines)
    if rng.random() < 0.5:
        order.reverse()
    return order


def summarize(records):
    """按引擎/来源/场景汇总中位数、MAD 与比值。"""
    grouped = {}
    for record in records:
        if "error" in record:
            continue
        grouped.setdefault((record["source"], record["case"], record["engine"]), []).append(record)
    summary = []
    keys = sorted({(source, case) for source, case, _ in grouped})
    for source, case in keys:
        row = {"source": source, "case": case, "engines": {}}
        for engine in ("fssql", "duckdb"):
            samples = grouped.get((source, case, engine), [])
            times = sorted(sample["seconds"] for sample in samples)
            if not times:
                continue
            median = statistics.median(times)
            deviations = [abs(value - median) for value in times]
            mad = statistics.median(deviations)
            row["engines"][engine] = {
                "n": len(times), "median": median, "mad": mad,
                "mad_ratio": (mad / median) if median else None,
                "min": min(times), "max": max(times),
                "rows": samples[0]["rows"],
            }
        if "fssql" in row["engines"] and "duckdb" in row["engines"]:
            row["time_ratio"] = row["engines"]["fssql"]["median"] / row["engines"]["duckdb"]["median"]
            row["valid"] = all(row["engines"][name]["mad_ratio"] is not None and
                               row["engines"][name]["mad_ratio"] <= 0.10 for name in ("fssql", "duckdb"))
        summary.append(row)
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", type=int, default=7)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--sources", default="local,s3")
    parser.add_argument("--cases", default="core", choices=["core", "all", "functional"])
    parser.add_argument("--only", default="", help="comma-separated case names for a smoke run")
    parser.add_argument("--seed", type=int, default=20261104)
    parser.add_argument("--append", action="store_true", help="append to existing samples.jsonl")
    parser.add_argument("--tag", default="", help="output suffix for an independent evidence set")
    args = parser.parse_args()

    repo = Path(__file__).resolve().parents[2]
    evidence = repo / "artifacts/local-validation/2026-11-04/duckdb-compare"
    evidence.mkdir(parents=True, exist_ok=True)
    (evidence / "tmp").mkdir(exist_ok=True)
    (evidence / "raw").mkdir(exist_ok=True)
    settings = load_settings(repo)
    bucket = settings["FSSQL_MINIO_BUCKET"]
    if not bucket.startswith("fssql-agent-test-"):
        raise RuntimeError("only a dedicated test bucket is allowed")
    root = repo / ".cache/agent-verification/large-data"
    sources = [value for value in args.sources.split(",") if value]
    only = {value for value in args.only.split(",") if value}
    core_cases = [case for case in CORE_CASES if not only or case["name"] in only]
    function_cases = [case for case in FUNCTION_CASES if not only or case["name"] in only]
    rng = random.Random(args.seed)
    binary = build_binary(repo, evidence)
    samples_path = evidence / (("samples-%s.jsonl" % args.tag) if args.tag else "samples.jsonl")
    functional_path = evidence / (("functional-%s.jsonl" % args.tag) if args.tag else "functional.jsonl")
    summary_path = evidence / (("summary-%s.json" % args.tag) if args.tag else "summary.json")

    if args.cases in ("core", "all"):
        with samples_path.open("a" if args.append else "w", encoding="utf-8", newline="\n") as output:
            for source in sources:
                paths = source_paths(root, bucket, source)
                for case in core_cases:
                    fssql_sql = case["sql"].format(**paths)
                    duck_sql = duckdb_sql(fssql_sql)
                    for round_index in range(args.warmups + args.samples):
                        measured = round_index >= args.warmups
                        for engine in sample_engines(["fssql", "duckdb"], rng):
                            tag = "%s-%s-%s-%02d-%s" % (source, case["name"], "sample" if measured else "warmup", round_index, engine)
                            try:
                                if engine == "fssql":
                                    case_path = write_case_case(evidence, tag, case, source, fssql_sql)
                                    record = run_fssql(repo, binary, settings, case, source, root, case_path,
                                                       evidence / "raw" / (tag + ".json"))
                                else:
                                    record = measure_duckdb(settings, case, source, duck_sql, evidence / "tmp")
                            except Exception as error:  # 保留失败样本，不静默剔除
                                record = {"engine": engine, "source": source, "case": case["name"], "error": str(error)}
                            if measured:
                                output.write(json.dumps(record, ensure_ascii=False) + "\n")
                                output.flush()
                                print("%s %s %s %.3fs" % (source, case["name"], engine,
                                                          record.get("seconds", -1)), flush=True)

    if args.cases in ("functional", "all"):
        with functional_path.open("w", encoding="utf-8", newline="\n") as output:
            for source in sources:
                paths = source_paths(root, bucket, source)
                for case in function_cases:
                    fssql_sql = (case.get("fssql") or case.get("sql")).format(**paths)
                    duck_sql = (case.get("duckdb") or case.get("sql") or "").format(**paths)
                    for engine, sql in (("fssql", fssql_sql), ("duckdb", duckdb_sql(duck_sql))):
                        tag = "func-%s-%s-%s" % (source, case["name"], engine)
                        try:
                            if engine == "fssql":
                                case_path = write_case_case(evidence, tag, case, source, sql)
                                record = run_fssql(repo, binary, settings, case, source, root, case_path,
                                                   evidence / "raw" / (tag + ".json"))
                            else:
                                record = measure_duckdb(settings, case, source, sql, evidence / "tmp")
                            record["ok"] = record["rows"] == case["rows"]
                        except Exception as error:
                            record = {"engine": engine, "source": source, "case": case["name"],
                                      "rows": 0, "ok": False, "error": str(error)}
                        output.write(json.dumps(record, ensure_ascii=False) + "\n")
                        output.flush()
                        print("func %s %s %s ok=%s" % (source, case["name"], engine, record["ok"]), flush=True)

    records = []
    if samples_path.exists():
        records = [json.loads(line) for line in samples_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    summary = summarize(records)
    (evidence / summary_path.name).write_text(json.dumps({"seed": args.seed, "samples": args.samples,
                                                       "warmups": args.warmups, "summary": summary},
                                                      ensure_ascii=False, indent=2) + "\n",
                                         encoding="utf-8", newline="\n")
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

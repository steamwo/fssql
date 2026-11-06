"""Million-row loopback HTTP Range acceptance; does not certify a remote S3 service.

Requires requirements.txt and Go on PATH. Reuses the original fixture generator,
DuckDB result digests and the production S3 adapter's HTTP byte meter.
"""

import json
import os
from pathlib import Path
import re
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import duckdb
from minio_large_fixture import build_cases, generate_files, reference_digest


def main():
    repo = Path(__file__).resolve().parents[2]
    evidence = repo / ".cache/scan-acceptance"
    root = evidence / "data"
    generate_files(root, 1_000_000)
    bucket = "fssql-agent-test-loopback"

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_HEAD(self):
            self.serve(False)

        def do_GET(self):
            self.serve(True)

        def serve(self, body):
            prefix = "/" + bucket + "/large/"
            name = self.path.removeprefix(prefix)
            if not self.path.startswith(prefix) or name not in {"data.csv", "data.ndjson", "data.parquet"}:
                self.send_error(404)
                return
            path = root / name
            size = path.stat().st_size
            start, end, status = 0, size - 1, 200
            if "Range" in self.headers:
                match = re.fullmatch(r"bytes=(\d+)-(\d+)", self.headers["Range"])
                if not match:
                    self.send_error(416)
                    return
                start, end = map(int, match.groups())
                if not 0 <= start <= end < size:
                    self.send_error(416)
                    return
                status = 206
            self.send_response(status)
            self.send_header("Content-Length", str(end - start + 1))
            self.send_header("ETag", '"fixture-%d"' % size)
            self.send_header("Accept-Ranges", "bytes")
            if status == 206:
                self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            self.end_headers()
            if body:
                try:
                    with path.open("rb") as source:
                        source.seek(start)
                        remaining = end - start + 1
                        while remaining:
                            block = source.read(min(remaining, 256 << 10))
                            self.wfile.write(block)
                            remaining -= len(block)
                except (BrokenPipeError, ConnectionResetError):
                    pass  # Early LIMIT/cancellation may close a response.

    names = {"scan_narrow", "scan_wide", "filter_range", "filter_empty", "projection_limit", "csv_scan", "ndjson_scan"}
    cases = [c for c in build_cases(bucket, 1_000_000) if c["name"] in names]
    # Use compare semantics for LIMIT too, so both row count and digest are checked.
    for case in cases:
        case["kind"] = "compare"
    for case in list(cases):
        if case["name"] in {"csv_scan", "ndjson_scan"}:
            cases.append(dict(case, name=case["name"]+"_256", memory_limit=256 << 20))
    with duckdb.connect() as conn:
        conn.execute("SET memory_limit='512MB'")
        conn.execute("SET threads=4")
        for case in cases:
            sql = case["sql"].replace(f"s3://{bucket}/large/", root.as_posix()+"/").replace("read_ndjson(", "read_json_auto(")
            case["expected_rows"], case["expected_digest"] = reference_digest(conn, sql, case["ordered"])
            print("reference", case["name"], case["expected_rows"], flush=True)
    manifest = {"rows": 1_000_000, "files": {p.name: p.stat().st_size for p in root.iterdir() if p.name in {"data.csv", "data.ndjson", "data.parquet"}}, "cases": cases, "duckdb_version": duckdb.__version__, "transport": "loopback HTTP Range"}
    (evidence / "manifest.json").write_text(json.dumps(manifest, indent=2)+"\n")
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    env = dict(os.environ, FSSQL_MINIO_ENDPOINT=f"http://127.0.0.1:{server.server_port}", FSSQL_MINIO_BUCKET=bucket,
               FSSQL_MINIO_ACCESS_KEY="loopback", FSSQL_MINIO_SECRET_KEY="loopback-only",
               FSSQL_LARGE_MANIFEST=str(evidence / "manifest.json"), FSSQL_LARGE_REPORT=str(evidence / "results.jsonl"),
               FSSQL_LARGE_CASES=",".join(c["name"] for c in cases), FSSQL_LARGE_TIMEOUT="10m")
    try:
        with (evidence / "test.log").open("w") as log:
            result = subprocess.run(["go", "test", "-v", "-count=1", "-timeout=30m", "-run", "^TestMinIOLargeMatrix$", "./pkg/gosql"], cwd=repo, env=env, stdout=log, stderr=subprocess.STDOUT)
    finally:
        server.shutdown()
        server.server_close()
        worker.join()
    if result.returncode:
        raise RuntimeError((evidence / "test.log").read_text()[-8000:])
    results = {r["name"]: r for r in map(json.loads, (evidence / "results.jsonl").read_text().splitlines())}
    if set(results) != {c["name"] for c in cases} or len(cases) != 9:
        raise RuntimeError("incomplete acceptance matrix")
    for row in results.values():
        if row["status"] != "pass" or row["managed_after_bytes"] or row["temp_after_bytes"] or row["managed_peak_bytes"] > row["memory_limit_bytes"] or row["full_get_requests"] or row["non_206_range_responses"]:
            raise RuntimeError(row)
        print(row["name"], "io=", row["io_bytes"], "managed=", row["managed_peak_bytes"], flush=True)
    narrow = results["scan_narrow"]["io_bytes"]
    if narrow >= results["scan_wide"]["io_bytes"] / 2 or results["filter_range"]["io_bytes"] >= narrow / 2 or results["filter_empty"]["io_bytes"] >= 1 << 20 or results["projection_limit"]["io_bytes"] >= narrow / 2:
        raise RuntimeError("physical I/O pushdown gate failed")
    print("PASS: loopback million-row acceptance; remote service acceptance remains separate", flush=True)


if __name__ == "__main__":
    main()

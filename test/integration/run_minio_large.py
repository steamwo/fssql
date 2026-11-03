"""运行真实服务大文件矩阵，保存原始日志与服务资源采样。"""

import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time


def main():
    """对本仓库执行正常、race 或 benchmark 模式，始终保留真实退出状态。"""
    repo = Path(__file__).resolve().parents[2]
    cache = repo / ".cache/agent-verification"
    evidence = repo / ".cache/agent-verification/large"
    evidence.mkdir(parents=True, exist_ok=True)
    mode = "bench" if "--bench" in sys.argv else "race" if "--race" in sys.argv else "normal"
    if "--workers" in sys.argv:
        mode = "workers-race" if "--race" in sys.argv else "workers"
    if "--recheck" in sys.argv:
        mode = "recheck"
    if "--defaults" in sys.argv:
        mode = "defaults"
    if "--joins" in sys.argv:
        mode = "join-extra"
    env = dict(os.environ)
    env.update(json.loads((cache / "minio-test-env.json").read_text(encoding="utf-8")))
    env["GOCACHE"] = str(cache / "go-build")
    env["FSSQL_LARGE_MANIFEST"] = str(cache / "large-manifest.json")
    env["FSSQL_LARGE_REPORT"] = str(evidence / (mode + ".jsonl"))
    temp = cache / "large-tmp"
    temp.mkdir(exist_ok=True)
    env["TEMP"] = env["TMP"] = str(temp)
    command = ["go", "test", "-v", "-timeout=30m", "-count=1", "-run", "^TestMinIOLargeMatrix$", "./pkg/gosql"]
    if "--race" in sys.argv:
        command.insert(2, "-race")
        env["FSSQL_LARGE_CASES"] = "global_aggregate,topn,aggregate_spill,join_spill,cancel,consumer_error,concurrent_queries"
        env["FSSQL_LARGE_TIMEOUT"] = "10m"
    if "--label" in sys.argv:
        label = sys.argv[sys.argv.index("--label") + 1]
        if not label.replace('-', '').isalnum():
            raise RuntimeError('Invalid evidence label')
        mode = label
        env["FSSQL_LARGE_REPORT"] = str(evidence / (mode + ".jsonl"))
    if "--workers" in sys.argv:
        command[command.index("-run") + 1] = "^TestMinIOLargeWorkers4$"
    if "--defaults" in sys.argv:
        command[command.index("-run") + 1] = "^TestMinIOLargeDefaultBudget$"
    if "--cases" in sys.argv:
        env["FSSQL_LARGE_CASES"] = sys.argv[sys.argv.index("--cases") + 1]
    if "--recheck" in sys.argv:
        env["FSSQL_LARGE_CASES"] = "using_sample_unsupported,arithmetic_sample,stratified_sample"
    if "--joins" in sys.argv:
        env["FSSQL_LARGE_CASES"] = "right_join,full_join,cross_join"
    if mode == "bench":
        command = ["go", "test", "-timeout=30m", "-run", "^$", "-bench", "^BenchmarkMinIOLarge$", "-benchtime=1x", "-count=3", "-benchmem", "./pkg/gosql"]
    else:
        Path(env["FSSQL_LARGE_REPORT"]).write_bytes(b"")
    stop = threading.Event()

    def sample_services():
        """每五秒只读采样共享服务，采样失败单独记入证据。"""
        with (evidence / (mode + "-containers.jsonl")).open("w", encoding="utf-8", newline="\n") as output:
            while not stop.is_set():
                try:
                    result = subprocess.run(["wsl", "-d", "Ubuntu-22.04", "--", "/usr/bin/docker", "stats", "--no-stream", "--format", "{{json .}}", "docker-minio-1", "docker-postgres-1"],
                                            capture_output=True, text=True, timeout=12)
                    for line in result.stdout.splitlines():
                        try:
                            row = json.loads(line)
                            row["sample_unix"] = time.time()
                            output.write(json.dumps(row) + "\n")
                        except json.JSONDecodeError:
                            pass
                    output.flush()
                except subprocess.TimeoutExpired:
                    output.write(json.dumps({"error": "docker stats timeout"}) + "\n")
                stop.wait(5)

    thread = threading.Thread(target=sample_services)
    thread.start()
    start = time.perf_counter()
    with (evidence / (mode + ".log")).open("wb") as log:
        process = subprocess.Popen(command, cwd=repo, env=env, stdout=log, stderr=subprocess.STDOUT)
        seen = set()
        while process.poll() is None:
            report = evidence / (mode + ".jsonl")
            if mode != "bench" and report.exists():
                for line in report.read_text(encoding="utf-8").splitlines():
                    try:
                        case = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if case["name"] not in seen:
                        seen.add(case["name"])
                        print("%s: %s %.3fs CPU=%.1f%% RSS=%.1fMiB managed=%.1fMiB temp=%.1fMiB net=%.1fMiB" %
                              (case["name"], case["status"], case["seconds"], case["cpu_percent"],
                               case["rss_peak_bytes"] / 2**20, case["managed_peak_bytes"] / 2**20,
                               case["temp_peak_bytes"] / 2**20, case["io_bytes"] / 2**20), flush=True)
            time.sleep(1)
    stop.set()
    thread.join()
    result = {"mode": mode, "exit_code": process.returncode, "seconds": time.perf_counter() - start}
    (evidence / (mode + "-status.json")).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8", newline="\n")
    print(json.dumps(result), flush=True)
    if process.returncode or mode == "bench":
        print((evidence / (mode + ".log")).read_text(encoding="utf-8", errors="replace")[-7000:], flush=True)
    return process.returncode


if __name__ == "__main__":
    raise SystemExit(main())

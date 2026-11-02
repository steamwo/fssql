"""为百万行窄列查询保留 CPU 与分配 profile，独立于三次 benchmark 基线。"""

import json
import os
from pathlib import Path
import subprocess


def main():
    """执行单次带 profile 的查询，并导出 CPU 与 heap 分配热点。"""
    repo = Path(__file__).resolve().parents[2]
    cache = repo / ".cache/agent-verification"
    evidence = repo / "artifacts/local-validation/2026-11-02/large"
    env = dict(os.environ, GOCACHE=str(cache / "go-build"))
    env.update(json.loads((cache / "minio-test-env.json").read_text(encoding="utf-8")))
    env["FSSQL_LARGE_MANIFEST"] = str(cache / "large-manifest.json")
    binary = cache / "profile-gosql.test.exe"
    command = ["go", "test", "-run", "^$", "-bench", "^BenchmarkMinIOLarge$/scan_narrow$", "-benchtime=1x", "-count=1", "-benchmem",
               "-cpuprofile", str(evidence / "scan.cpu.pprof"), "-memprofile", str(evidence / "scan.mem.pprof"), "-o", str(binary), "./pkg/gosql"]
    with (evidence / "profile.log").open("wb") as output:
        result = subprocess.run(command, cwd=repo, env=env, stdout=output, stderr=subprocess.STDOUT, timeout=180)
    if result.returncode:
        print((evidence / "profile.log").read_text(encoding="utf-8", errors="replace"))
        return result.returncode
    for name, flags, profile in [
        ("cpu-top.txt", [], "scan.cpu.pprof"),
        ("alloc-top.txt", ["-alloc_space"], "scan.mem.pprof"),
        ("heap-live-top.txt", ["-inuse_space"], "scan.mem.pprof"),
    ]:
        with (evidence / name).open("wb") as output:
            result = subprocess.run(["go", "tool", "pprof", "-top", "-nodecount=20"] + flags + [str(binary), str(evidence / profile)],
                                    cwd=repo, env=env, stdout=output, stderr=subprocess.STDOUT, timeout=60)
        print(name + ': exit=' + str(result.returncode), flush=True)
        if result.returncode:
            return result.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

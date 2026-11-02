"""验证 Windows Manifest 修复并保存真实退出码，不隐藏失败的检查。"""

import json
import os
from pathlib import Path
import subprocess


def main():
    """运行完整 Go 检查，并对其他平台实现进行交叉编译验证。"""
    repo = Path(__file__).resolve().parents[2]
    cache = repo / ".cache/agent-verification"
    evidence = repo / "artifacts/local-validation/2026-11-02/windows-copy-fix"
    evidence.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, GOCACHE=str(cache / "go-build"))
    reference = dict(env, FSSQL_REQUIRE_DUCKDB="1")
    reference["PATH"] = str(cache / "runtime/python-env/Scripts") + os.pathsep + env["PATH"]
    linux = dict(env, GOOS="linux", CGO_ENABLED="0")
    checks = [
        ("correctness", ["go", "test", "-count=1", "./..."], env),
        ("race", ["go", "test", "-race", "-count=1", "./..."], env),
        ("vet", ["go", "vet", "./..."], env),
        ("architecture", ["go", "run", "./tools/architecture-check", "."], env),
        ("duckdb", ["go", "test", "-count=1", "./test/compat"], reference),
        ("duckdb-race", ["go", "test", "-race", "-count=1", "./test/compat"], reference),
        ("linux-compile", ["go", "test", "-c", "-o", str(cache / "export-linux.test"), "./internal/export"], linux),
    ]
    statuses = {}
    for name, command, settings in checks:
        print("RUN " + name, flush=True)
        with (evidence / (name + ".log")).open("wb") as log:
            result = subprocess.run(command, cwd=repo, env=settings, stdout=log,
                                    stderr=subprocess.STDOUT, timeout=300)
        statuses[name] = result.returncode
        print(name + ": exit=" + str(result.returncode), flush=True)
        if result.returncode:
            print((evidence / (name + ".log")).read_text(encoding="utf-8", errors="replace")[-6000:], flush=True)
        (evidence / "status.json").write_text(json.dumps(statuses, indent=2) + "\n", encoding="utf-8", newline="\n")
    return int(any(statuses.values()))


if __name__ == "__main__":
    raise SystemExit(main())

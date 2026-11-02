"""从本地忽略目录加载测试凭据，运行真实 MinIO 集成测试并保留日志。"""

import json
import os
from pathlib import Path
import subprocess
import sys


def main():
    """向测试子进程注入凭据，保持退出码与真实测试结果一致。"""
    root = Path(__file__).resolve().parents[2] / ".cache/agent-verification"
    root.mkdir(parents=True, exist_ok=True)
    repo = root.parents[1]
    env = dict(os.environ)
    env.update(json.loads((root / "minio-test-env.json").read_text(encoding="utf-8")))
    env["GOCACHE"] = str(root / "go-build")
    temp = root / "services-tmp"
    temp.mkdir(exist_ok=True)
    env["TEMP"] = env["TMP"] = str(temp)
    race = "--race" in sys.argv
    args = ["go", "test", "-v", "-count=1"]
    if race:
        args.append("-race")
    args.extend(["-run", "^TestMinIO", "./pkg/gosql"])
    result = subprocess.run(args, cwd=repo, env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, timeout=180)
    evidence = repo / "artifacts/local-validation/2026-11-02/services"
    evidence.mkdir(parents=True, exist_ok=True)
    name = "minio-race.log" if race else "minio.log"
    (evidence / name).write_bytes(result.stdout)
    sys.stdout.buffer.write(result.stdout)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())

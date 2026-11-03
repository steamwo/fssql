"""验证大文件测试代码与差异注册表的完整常规回归。"""

import json
import os
from pathlib import Path
import subprocess


def main():
    """把常规回归与已知失败的 opt-in 大文件场景分开保留退出状态。"""
    repo = Path(__file__).resolve().parents[2]
    cache = repo / ".cache/agent-verification"
    evidence = repo / '.cache/agent-verification/large'
    env = dict(os.environ, GOCACHE=str(cache / 'go-build'), PYTHONDONTWRITEBYTECODE='1')
    env.pop('FSSQL_LARGE_MANIFEST', None)
    env.pop('FSSQL_MINIO_ENDPOINT', None)
    reference = dict(env, FSSQL_REQUIRE_DUCKDB='1')
    reference['PATH'] = str(cache / 'runtime/python-env/Scripts') + os.pathsep + env['PATH']
    linux = dict(env, GOOS='linux', CGO_ENABLED='0')
    checks = [
        ('correctness', ['go', 'test', '-count=1', './...'], env),
        ('race', ['go', 'test', '-race', '-count=1', './...'], env),
        ('vet', ['go', 'vet', './...'], env),
        ('architecture', ['go', 'run', './tools/architecture-check', '.'], env),
        ('duckdb', ['go', 'test', '-count=1', './test/compat'], reference),
        ('duckdb-race', ['go', 'test', '-race', '-count=1', './test/compat'], reference),
        ('linux-compile', ['go', 'test', '-c', '-o', str(cache / 'gosql-linux.test'), './pkg/gosql'], linux),
        ('fixture-python', ['python', '-m', 'unittest', 'discover', '-s', 'test/integration', '-p', 'minio_large_fixture_test.py'], env),
    ]
    status = {}
    for name, command, settings in checks:
        print('RUN regression-' + name, flush=True)
        path = evidence / ('regression-' + name + '.log')
        with path.open('wb') as output:
            result = subprocess.run(command, cwd=repo, env=settings, stdout=output, stderr=subprocess.STDOUT, timeout=300)
        status[name] = result.returncode
        print('regression-' + name + ': exit=' + str(result.returncode), flush=True)
        if result.returncode:
            print(path.read_text(encoding='utf-8', errors='replace')[-4000:], flush=True)
        (evidence / 'regression-status.json').write_text(json.dumps(status, indent=2) + '\n', encoding='utf-8', newline='\n')
    return int(any(status.values()))


if __name__ == '__main__':
    raise SystemExit(main())

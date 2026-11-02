"""执行仓库已有的算子与向量 microbenchmark，避免与远端 baseline 并发。"""

import json
import os
from pathlib import Path
import re
import subprocess


def main():
    """每组运行三次，未匹配到 benchmark 也视为失败。"""
    repo = Path(__file__).resolve().parents[2]
    cache = repo / ".cache/agent-verification"
    evidence = repo / 'artifacts/local-validation/2026-11-02/large'
    env = dict(os.environ, GOCACHE=str(cache / 'go-build'))
    groups = [
        ('native', './pkg/gosql', '^BenchmarkNativeQueryChunks$'),
        ('expression', './internal/execution/expression', '^BenchmarkNumericAdd2048$'),
        ('hash', './internal/execution/hash', '^Benchmark(VectorHashInt64|VectorHashVarchar|HashTableLookup)$'),
        ('join', './internal/execution/join', '^Benchmark(InnerHashJoin2048|ExternalHashJoinForcedSpill)$'),
        ('aggregate', './internal/execution/aggregate', '^Benchmark(HashAggregateBigInt|ExternalAggregateForcedSpill)$'),
        ('sort', './internal/execution/sort', '^Benchmark(ExternalSortForcedSpill|TopN)$'),
        ('distinct', './internal/execution/distinct', '^BenchmarkExternalDistinct$'),
    ]
    statuses = {}
    for name, package, pattern in groups:
        print('RUN core-' + name, flush=True)
        path = evidence / ('core-' + name + '.log')
        with path.open('wb') as log:
            result = subprocess.run(['go', 'test', '-run', '^$', '-bench', pattern, '-benchmem', '-benchtime=100ms', '-count=3', package],
                                    cwd=repo, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=300)
        text = path.read_text(encoding='utf-8', errors='replace')
        status = result.returncode
        if not re.search(r'^Benchmark.*\bns/op\b', text, re.MULTILINE):
            status = status or 2
        statuses[name] = status
        print('core-' + name + ': exit=' + str(status), flush=True)
        if status:
            print(text[-3000:], flush=True)
    (evidence / 'core-status.json').write_text(json.dumps(statuses, indent=2) + '\n', encoding='utf-8', newline='\n')
    return int(any(statuses.values()))


if __name__ == '__main__':
    raise SystemExit(main())

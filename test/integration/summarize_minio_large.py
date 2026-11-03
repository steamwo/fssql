"""将真实测试、重复 benchmark 和容器采样整理为可审阅报告。"""

from collections import Counter, defaultdict
import csv
import json
from pathlib import Path
import re
import statistics


def read_lines(path):
    """只读取完整的结构化行，缺失文件明确留空以便报告未完成项。"""
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines() if line.strip()]


def benchmark_rows(path):
    """解析 Go benchmark 原始单位，保留每个独立样本。"""
    rows = defaultdict(list)
    if not path.exists():
        return rows
    for line in path.read_text(encoding='utf-8').splitlines():
        tokens = line.split()
        if len(tokens) < 4 or not tokens[0].startswith('Benchmark') or 'ns/op' not in tokens:
            continue
        name = re.sub(r'-\d+$', '', tokens[0])
        rows[name].append({tokens[i + 1]: float(tokens[i]) for i in range(2, len(tokens) - 1, 2)})
    return rows


def memory_bytes(text):
    """解析 Docker stats 的二进制与十进制容量单位。"""
    match = re.match(r'([0-9.]+)\s*([A-Za-z]+)', text)
    if not match:
        return 0
    multipliers = {'B': 1, 'kB': 1000, 'MB': 10**6, 'GB': 10**9, 'KiB': 1024, 'MiB': 2**20, 'GiB': 2**30}
    return float(match[1]) * multipliers.get(match[2], 1)


def main():
    """用最新复核覆盖同名场景，并链接保留的原始运行证据。"""
    repo = Path(__file__).resolve().parents[2]
    root = repo / '.cache/agent-verification/large'
    manifest = json.loads((root / 'manifest.json').read_text(encoding='utf-8'))
    cases = {}
    for source in ['normal', 'recheck', 'workers', 'defaults', 'join-extra', 'copy-full']:
        for row in read_lines(root / (source + '.jsonl')):
            row['evidence_run'] = source
            cases[row['name']] = row
    counts = Counter(row['status'] for row in cases.values())
    race = {row['name']: row for row in read_lines(root / 'race.jsonl')}
    for row in read_lines(root / 'race-topn-retry.jsonl') + read_lines(root / 'workers-race.jsonl'):
        race[row['name']] = row
    benches = benchmark_rows(root / 'bench.log')
    benchmark_summary = []
    for name, rows in benches.items():
        seconds = [r['ns/op'] / 1e9 for r in rows]
        benchmark_summary.append({'name': name.split('/', 1)[-1], 'samples': len(rows),
                                  'median_seconds': statistics.median(seconds), 'min_seconds': min(seconds), 'max_seconds': max(seconds),
                                  'cpu_percent': statistics.median(r['cpu-%'] for r in rows),
                                  'managed_peak_bytes': max(r['managed-peak-B'] for r in rows),
                                  'rss_peak_bytes': max(r['rss-peak-B'] for r in rows),
                                  'net_bytes': statistics.median(r['net-B/op'] for r in rows),
                                  'allocated_bytes': statistics.median(r['B/op'] for r in rows),
                                  'allocations': statistics.median(r['allocs/op'] for r in rows)})
    samples = defaultdict(list)
    for path in root.glob('*-containers.jsonl'):
        for row in read_lines(path):
            if 'Name' in row:
                samples[row['Name']].append(row)
    containers = []
    for name, rows in samples.items():
        containers.append({'name': name, 'samples': len(rows),
                           'cpu_peak_percent': max(float(r['CPUPerc'].rstrip('%')) for r in rows),
                           'memory_peak_bytes': max(memory_bytes(r['MemUsage'].split('/')[0]) for r in rows)})
    summary = {'logical_rows': manifest['rows'], 'dataset_bytes': sum(manifest['files'].values()),
               'case_counts': dict(counts), 'cases': list(cases.values()), 'race_cases': list(race.values()),
               'benchmarks': benchmark_summary, 'containers': containers}
    (root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n', encoding='utf-8', newline='\n')
    with (root / 'metrics.csv').open('w', encoding='utf-8', newline='') as output:
        fields = list(next(iter(cases.values())).keys())
        fields.extend(sorted({key for row in cases.values() for key in row} - set(fields)))
        writer = csv.DictWriter(output, fieldnames=fields, lineterminator='\n', extrasaction='ignore')
        writer.writeheader()
        writer.writerows(cases.values())
    text = [
        '# 百万行真实 MinIO 测试与 Benchmark 报告', '',
        '**结论：大 Parquet 的主要 SQL 结果、spill 与资源清理通过；大 CSV/NDJSON 的默认内存路径失败，查询下推和 Top-N 性能存在明显缺口，不能判定大文件能力完整通过。**', '',
        '## 环境与数据', '',
        '- Windows / amd64，Go 1.24.2，Intel i5-12400F，12 个逻辑核。',
        '- Windows 本机运行引擎，访问既有 WSL Docker MinIO；PG 仅只读连接验证和资源旁观，未宣称引擎支持 PG 数据源。',
        '- DuckDB 2.0.0 是结果参考，PyArrow 24.0.0 生成无压缩、无 dictionary 的 Parquet。每个 row group 最多 32768 行。',
        '- 同一逻辑事实表 1,000,000 行，6 列，包含 NULL、负数、1000 个分组、乱序排序键和 256 B payload。',
        '- 原始对象合计 %s B（%.2f MiB）；Parquet 299,960,006 B，CSV 283,038,719 B，NDJSON 344,274,015 B。' % (format(sum(manifest['files'].values()), ','), sum(manifest['files'].values()) / 2**20),
        '- 本机 warm baseline，未清空共享服务缓存；未并行执行其他测试来干扰 benchmark。共享主机的外部负载未隔离。', '',
        '## 测试覆盖与结果', '',
        '共 %d 个独立场景：正常功能通过 %d，预期错误/清理验证 %d，显式未支持 %d，真实失败 %d。' %
        (len(cases), counts['pass'], counts['expected_error'], counts['unsupported'], counts['fail']), '',
        '预期错误包括取消、消费端停止、内存/临时空间配额，以及高内存预算下读取首块后主动停止的两个诊断场景。首块诊断不代表完整文本扫描通过。', '',
        '汇总按同名场景的最后复核结果统计；原始日志均保留。CAST 等边界探测保留相同 SQL 和实际错误，不计为功能支持。初轮 USING SAMPLE 的极短运行触发统计器除零，修复统计器后单独复核；没有修改引擎来改变该结果。', '',
        '| 场景 | 结果 | 返回行数 | 耗时 s | CPU % | RSS MiB | 受管峰值 MiB | 临时文件峰值 MiB | 实读 MiB | GC |',
        '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |',
    ]
    for row in cases.values():
        cpu = '—' if row['seconds'] < 0.1 else '%.1f' % row['cpu_percent']
        text.append('| %s | %s | %d | %.3f | %s | %.1f | %.1f | %.1f | %.1f | %d |' %
                    (row['name'], row['status'], row['rows'], row['seconds'], cpu,
                     row['rss_peak_bytes'] / 2**20, row['managed_peak_bytes'] / 2**20,
                     row['temp_peak_bytes'] / 2**20, row['io_bytes'] / 2**20, row['gc_cycles']))
    text += ['', '## 关键发现', '',
             '1. **大文本读取存在功能阻塞（MEM-001）**：CSV/NDJSON 默认串行扫描申请整文件输入缓冲，64 MiB 和默认 256 MiB 预算均无法扫描本轮对象。提高到 1 GiB 后，返回首块前已占用约 271/329 MiB 受管内存；此诊断主动停止，没有把大内存单块成功当作流式能力通过。',
             '2. **Parquet 下推未生效（PERF-001）**：窄列、LIMIT 100、选择性过滤及空结果仍读取约 300,375,678 B、约 3487 次 Range。生产 adapter 源码明确将 Projection/Filter/Limit 留给 residual operators，因此输入 payload 列仍被解码。',
             '3. **Top-N 未见收益**：Top-100 与全排序落盘的中位耗时、实读量及临时文件接近；本轮不能将其当作有效的生产 Top-N 优化。',
             '4. **峰值有界但分配量大**：窄列扫描的 benchmark 累计分配约 1.34 GiB/次；pprof 约 84% 分配来自 MemoryManager 下面的 DefaultAllocator，StringHeap.PutBytes/Buffer.Resize 与未裁剪字符串列相关。它们仍受内存记账管理，但频繁分配触发 GC。',
             '5. **并行收益有限**：1/4 worker 的窄列吞吐接近；四查询并发结果正确，但每个查询仍完整扫描源文件，网络量相应放大。',
             '6. **能力边界明确**：TABLESAMPLE、USING SAMPLE、random()、显式 CAST 和 BIGINT `%` 目前不支持；聚合外层标量子查询和 FROM 派生表也超出当前有限子查询实现。受支持的普通标量子查询、CTE Join、分层谓词抽样和消费端 reservoir 分别验证。', '',
             '已将资源/性能缺陷和未支持边界写入 [compatibility/deviations.yaml](../../../../compatibility/deviations.yaml)。本轮未新增 Stable feature 或隐藏错误。', '',
             '## 结果正确性与清理', '',
             '- DuckDB 独立计算完整结果。ORDER BY 比较有序 SHA256；无序查询比较保留重复计数的行摘要和。',
             '- 未排序 LIMIT 只验证合法子集、行数与 payload；不会把另一合法子集误判为错误。',
             '- 排序、聚合、Join 分别对照内存执行与低预算 spill，确实观测到临时文件，结果均匹配参考。',
             '- Reservoir 通过完整有序百万行流抽取 1000 项，与独立 Python 固定种子参考一致；另有 128 种子 property test。',
             '- 所有已记录场景结束时，受管内存和查询临时文件均为零；包括内存不足、临时配额、取消和消费端错误。',
             '- 连续 20 次查询结束均归零；累计 Go 分配约 26.6 GiB、GC 约一万次。内存释放正确，分配效率仍需改进。',
             '- 100000 行含宽字符串的 COPY 写入真实 MinIO，随后回读全部 id/value/payload，完整行摘要与 DuckDB 参考一致。', '',
             '## End-to-end Benchmark（三次样本）', '',
             '回调只计数，不做逐行 SHA 校验。表中 CPU 100% 代表一个逻辑核，并非整台 12 核机器的 100%。RSS 包含 Go runtime/连接池，不能等同于受管内存；顺序 benchmark 的 RSS 也受前序场景保留页影响。', '',
             '| 场景 | n | 中位 s | 最小/最大 s | CPU % | 受管峰值 MiB | 累计分配 MiB/op | 实读 MiB/op |',
             '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |']
    for row in benchmark_summary:
        text.append('| %s | %d | %.3f | %.3f / %.3f | %.1f | %.1f | %.1f | %.1f |' %
                    (row['name'], row['samples'], row['median_seconds'], row['min_seconds'], row['max_seconds'], row['cpu_percent'],
                     row['managed_peak_bytes'] / 2**20, row['allocated_bytes'] / 2**20, row['net_bytes'] / 2**20))
    text += ['', '## Race、microbenchmark 与回归', '',
             '| Race 场景 | 最终结果 | 耗时 s |', '| --- | --- | ---: |']
    for name, row in race.items():
        text.append('| %s | %s | %.3f |' % (name, row['status'], row['seconds']))
    text += ['', 'Race 初轮 Top-N 因测试器 2 分钟上限被取消；资源清理通过，未出现数据竞争报告。提高到有界的 10 分钟后只复测同一场景，不放宽结果断言。保留初轮和重测日志，race 耗时不用于生产 benchmark 对比。', '',
             'microbenchmark 使用既有向量、Hash、Join、Aggregate、Sort、Distinct 与 Native API，100ms × 3 次。', '',
             '| 包/benchmark | 样本数 | 中位 ns/op | 中位 B/op | 中位 allocs/op |', '| --- | ---: | ---: | ---: | ---: |']
    for path in sorted(root.glob('core-*.log')):
        for name, rows in benchmark_rows(path).items():
            text.append('| %s / %s | %d | %.1f | %.0f | %.0f |' %
                        (path.stem, name, len(rows), statistics.median(r['ns/op'] for r in rows),
                         statistics.median(r.get('B/op', 0) for r in rows), statistics.median(r.get('allocs/op', 0) for r in rows)))
    for filename in ['core-status.json', 'regression-status.json']:
        path = root / filename
        if path.exists():
            text += ['', filename + '：`' + json.dumps(json.loads(path.read_text()), ensure_ascii=False) + '`。']
    text += ['', '## 共享服务资源', '',
             '以下为 Docker stats 约 5 秒采样的整个既有容器占用，包含其缓存及其他潜在客户，不能归因于单条查询，也不能与 Windows 引擎 RSS 直接相加。', '',
             '| 容器 | 样本 | CPU 峰值 % | 内存峰值 MiB |', '| --- | ---: | ---: | ---: |']
    for row in containers:
        text.append('| %s | %d | %.1f | %.1f |' % (row['name'], row['samples'], row['cpu_peak_percent'], row['memory_peak_bytes'] / 2**20))
    text += ['', '## 证据与验证边界', '',
             '- [完整原始矩阵](normal.log)、[抽样边界复核](recheck.log)、[四 worker](workers.log)、[默认内存预算](defaults.log)、[RIGHT/FULL/CROSS Join 补充](join-extra.log)、[COPY 全行内容校验](copy-full.log)。',
             '- [三次 end-to-end benchmark](bench.log)、[CPU 热点](cpu-top.txt)、[累计分配热点](alloc-top.txt)、[存活 heap 热点](heap-live-top.txt)。',
             '- [初轮 race](race.log)、[Top-N race 重测](race-topn-retry.log)、[四 worker race](workers-race.log)。',
             '- [结构化汇总](summary.json)、[指标 CSV](metrics.csv)、[数据与 SQL 描述](manifest.json)。',
             '- CPU/heap/临时文件采样存在开销；RSS 与临时文件瞬时峰值可能低估，受管内存峰值来自精确记账。小于 100ms 的 CPU 百分比不作为可比较数据。',
             '- 不使用 race 的 RSS/耗时作为正常运行基线；pprof 和普通基线独立运行。',
             '- 本轮大数据覆盖整型、浮点聚合、NULL 和宽字符串；没有执行大规模 DECIMAL/LIST/STRUCT、压缩编码全矩阵、任意规模极限、长时间 soak 或真实断电注入。仓库已有 unit/fault/interop 测试不等于这些维度的大文件规模验证；不宣称完整 Release Gate 通过。',
             '- 当前专项结论为 FAIL：大文本读取与性能缺口尚未修复。项目阶段元数据未修改。', '']
    (root / 'report.md').write_text('\n'.join(text), encoding='utf-8', newline='\n')
    print(json.dumps({'cases': len(cases), 'counts': dict(counts), 'race': len(race), 'benchmarks': len(benches)}, indent=2))


if __name__ == '__main__':
    main()

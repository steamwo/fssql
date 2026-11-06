"""交替采样 JOIN 优化前后与 DuckDB，保留所有结果及资源数据。"""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import random
import statistics
import subprocess

# 同时支持从证据目录或临时缓存目录运行。
root = next(p for p in Path(__file__).resolve().parents if (p/'go.mod').is_file() and (p/'test/integration/duckdb_compare.py').is_file())
evidence = root / '.cache/join-optimization'
spec = importlib.util.spec_from_file_location('comparison', root/'test/integration/duckdb_compare.py')
comparison = importlib.util.module_from_spec(spec)
spec.loader.exec_module(comparison)
settings = comparison.load_settings(root)
parser = argparse.ArgumentParser()
parser.add_argument('--profile', action='store_true')
parser.add_argument('--quick', action='store_true')
args = parser.parse_args()
after = evidence/'after.test.exe'
before = evidence/'before.test.exe'
subprocess.run(['go','test','-c','-o',str(after),'./pkg/gosql'], cwd=root, check=True)
for folder in ['raw','tmp','cases']:
    (evidence/folder).mkdir(exist_ok=True)
case = next(c for c in comparison.CORE_CASES if c['name']=='inner_join')
if args.profile:
    paths = comparison.source_paths(root/'.cache/agent-verification/large-data', settings['FSSQL_MINIO_BUCKET'], 'local')
    case_path = comparison.write_case_case(evidence, 'profile', case, 'local', case['sql'].format(**paths))
    env = dict(os.environ, **settings)
    env.update(FSSQL_COMPARE_CASE=str(case_path), FSSQL_COMPARE_OUT=str(evidence/'profile.json'), FSSQL_COMPARE_ROOT=str(root/'.cache/agent-verification/large-data'))
    subprocess.run([str(after),'-test.run','^TestDuckDBCompareCase$','-test.count=40','-test.cpuprofile',str(evidence/'after.cpu'),'-test.memprofile',str(evidence/'after.mem')],env=env,cwd=root,check=True)
    raise SystemExit(0)
rng = random.Random(20261107)
records, summary = [], []
variants = [case]
if not args.quick:
    variants += [dict(case, name='inner_join_holes', sql="SELECT COUNT(*), SUM(d.weight) FROM read_parquet('{data}') f JOIN (SELECT * FROM read_parquet('{dim}') WHERE grp % 2 = 0) d ON f.grp = d.grp"),
                 dict(case, name='inner_join_duplicates', sql="SELECT COUNT(*), SUM(d.weight) FROM read_parquet('{data}') f JOIN (SELECT * FROM read_parquet('{dim}') UNION ALL SELECT * FROM read_parquet('{dim}')) d ON f.grp = d.grp")]
with (evidence/'samples.jsonl').open('w',encoding='utf-8',newline='\n') as output:
    for source in ['local','s3']:
        paths = comparison.source_paths(root/'.cache/agent-verification/large-data', settings['FSSQL_MINIO_BUCKET'], source)
        for item in variants:
            sql = item['sql'].format(**paths)
            for iteration in range(3 if args.quick else 7):
                engines = ['before','after','duckdb']
                rng.shuffle(engines)
                for engine in engines:
                    tag = '%s-%s-%d-%s' % (source,item['name'],iteration,engine)
                    if engine == 'duckdb':
                        record = comparison.measure_duckdb(settings,item,source,sql,evidence/'tmp')
                    else:
                        path = comparison.write_case_case(evidence,tag,item,source,sql)
                        record = comparison.run_fssql(root,before if engine=='before' else after,settings,item,source,root/'.cache/agent-verification/large-data',path,evidence/'raw'/(tag+'.json'))
                    record.update(engine=engine, iteration=iteration, warmup=iteration<2)
                    output.write(json.dumps(record,ensure_ascii=False)+'\n')
                    output.flush()
                    records.append(record)
            row = dict(source=source,case=item['name'],engines={})
            for engine in ['before','after','duckdb']:
                times = [r['seconds'] for r in records if r['source']==source and r['case']==item['name'] and r['engine']==engine and not r['warmup']]
                median = statistics.median(times)
                mad = statistics.median(abs(t-median) for t in times)
                row['engines'][engine] = dict(median=median,mad=mad,mad_ratio=mad/median,n=len(times))
            row['speedup'] = row['engines']['before']['median']/row['engines']['after']['median']
            row['duckdb_ratio'] = row['engines']['after']['median']/row['engines']['duckdb']['median']
            summary.append(row)
            print(source,item['name'],json.dumps(row),flush=True)
payload = dict(warmups=2,samples=1 if args.quick else 5,seed=20261107,before_sha256=hashlib.sha256(before.read_bytes()).hexdigest(),after_sha256=hashlib.sha256(after.read_bytes()).hexdigest(),summary=summary)
(evidence/'summary.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8',newline='\n')

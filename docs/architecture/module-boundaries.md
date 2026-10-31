# Module Boundaries

## 推荐目录

```text
cmd/
pkg/gosql/

internal/
  core/
    types/
    vector/
    memory/
    errors/
    stats/
  semantics/
  sql/
    lexer/
    parser/
    ast/
    binder/
  function/
  catalog/
  planner/
  optimizer/
  execution/
    expression/
    pipeline/
    scheduler/
    hash/
    join/
    aggregate/
    sort/
    result/
  storage/
    vfs/
    remote/
    fileset/
    cache/
    asyncio/
    temp/
  format/
    scan/
    compression/
    parquet/
    csv/
    json/
  introspection/
  export/
  session/
  query/
  resource/
  settings/
  security/
  profile/
  server/

test/
benchmark/
tools/
docs/
```

## 禁止依赖

- `core/types` -> binder/optimizer/parquet/execution
- `core/vector` -> format/sql/optimizer
- `core/memory` -> concrete join/sort/aggregate
- `optimizer` -> parquet/s3/sftp
- `storage/vfs` -> parquet/csv/json
- `execution/scheduler` -> parquet rowgroup / csv split
- `catalog` -> runtime operator/vector/file handle/remote client
- format reader -> optimizer implementation

## 正确依赖方式

- Optimizer 只认识 generic ScanPushdown/Statistics contracts。
- Parquet/CSV/JSON 将 generic pushdown 翻译为格式能力。
- MemoryManager 只认识 MemoryConsumer/Reclaimer/Reservation/Tag。
- Scheduler 只认识 generic Task。
- Third-party SDK type 只允许存在于 adapter boundary。

## CI

实现 `tools/architecture-check`，在 CI 中验证 forbidden import。

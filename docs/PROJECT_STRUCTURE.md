# Project Structure

```text
gosql/
├── cmd/
│   ├── gosql/
│   └── gosqld/
├── pkg/
│   └── gosql/
├── internal/
│   ├── core/
│   │   ├── types/
│   │   ├── vector/
│   │   ├── memory/
│   │   ├── stats/
│   │   └── errors/
│   ├── semantics/
│   ├── sql/
│   │   ├── lexer/
│   │   ├── parser/
│   │   ├── ast/
│   │   └── binder/
│   ├── function/
│   ├── catalog/
│   ├── planner/
│   ├── optimizer/
│   ├── execution/
│   │   ├── expression/
│   │   ├── pipeline/
│   │   ├── scheduler/
│   │   ├── hash/
│   │   ├── join/
│   │   ├── aggregate/
│   │   ├── sort/
│   │   └── result/
│   ├── storage/
│   │   ├── vfs/
│   │   ├── remote/
│   │   ├── fileset/
│   │   ├── cache/
│   │   ├── asyncio/
│   │   └── temp/
│   ├── format/
│   │   ├── scan/
│   │   ├── compression/
│   │   ├── parquet/
│   │   ├── csv/
│   │   └── json/
│   ├── introspection/
│   ├── export/
│   ├── session/
│   ├── query/
│   ├── resource/
│   ├── settings/
│   ├── security/
│   ├── profile/
│   └── server/
├── api/
├── configs/
├── compatibility/
├── migrations/
├── test/
│   ├── sqllogic/
│   ├── differential/
│   ├── property/
│   ├── fuzz/
│   ├── interop/
│   ├── fault/
│   ├── leak/
│   └── integration/
├── benchmark/
│   ├── micro/
│   ├── operator/
│   └── e2e/
├── tools/
├── scripts/
├── examples/
└── docs/
```

阶段不要映射为 `internal/phase0` 之类代码目录。代码按长期模块边界组织，Phase 只用于计划和验收。

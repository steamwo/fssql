# fssql 本机验证记录

日期：2026-11-02。仓库：`E:/study_file/go_object/fssql`。运行环境：Windows，Go 1.24.2，DuckDB 2.0.0，PyArrow 24.0.0。

更新：下文保留修复前的验证记录。Windows Manifest 问题现已修复，最新结果见 [Windows COPY 修复验证](windows-copy-fix/report.md)及[现有服务集成测试](services/report.md)。

后续新增了[百万行大文件与 Benchmark 验证](large/report.md)。该专项发现大 CSV/NDJSON 默认内存路径及查询下推性能缺口，结论为未完整通过；先前的小样本成功不能替代此项证据。

## 已完成

| 检查 | 结果 |
| --- | --- |
| `go test -count=1 ./...` | 失败；仅 internal/export 包的 4 个 Manifest 持久化测试失败，其余测试包通过。 |
| `go test -race -count=1 ./...` | 失败；同样 4 个测试失败，输出未发现数据竞争报告。 |
| `go vet ./...` | 通过。 |
| `go run ./tools/architecture-check .` | 通过。 |
| `FSSQL_REQUIRE_DUCKDB=1 go test -count=1 ./test/compat` | 通过；使用 DuckDB 2.0.0。 |
| `FSSQL_REQUIRE_DUCKDB=1 go test -race -count=1 ./test/compat` | 通过。 |
| `python -m unittest discover -s ./test/interop/phase1 -p run_output_test.py` | 通过。 |
| `python ./test/interop/phase4/run.py` | 通过；fssql 产出的 Parquet 经 DuckDB、PyArrow 读取，3 行结果一致，包括 NULL。 |
| `phase5-cancellation-leak.sh` | 通过。 |
| `phase5-security.sh` | 通过。 |
| `phase5-config-compatibility.sh` | 通过。 |
| `phase5-stability.sh` | 通过。 |

Phase 5 脚本通过 Git Bash 执行，输入为原脚本文本经 CRLF 转 LF 的内存副本；仓库脚本未修改。日志见当前目录各 `phase5-*.sh.log`。

## 失败定位

以下测试在普通与 race 模式下均失败：

- `TestLoadAndPrepareResumePersistsRedoState`
- `TestLocalManifestStoreRoundTripAndCrashResume`
- `TestLocalManifestStoreDeleteIsIdempotent`
- `TestKillRestartResumePreservesCompleteAndRedoesIncomplete`

错误均发生在 `internal/export/manifest_store.go` 的目录同步路径：`sync <directory>: Access is denied.`。将 TEMP/TMP 改到允许写入的目录后，4 个测试仍以相同错误失败，日志见 [manifest-writable-temp.log](manifest-writable-temp.log)。这表明问题涉及当前 Windows 环境下目录同步的支持或权限；本次没有修改持久化实现。日志保留实际运行时的原始路径；测试环境现已迁入本仓库 `.cache/agent-verification/runtime`。

## 环境限制与未完成检查

- `wsl --list --verbose` 返回 `Wsl/EnumerateDistros/Service/E_ACCESSDENIED`，当前会话无法访问 WSL。
- Windows PATH 中没有 Docker CLI，检查的默认 Docker Desktop CLI 路径与命名管道也不可用；未执行容器测试。
- Windows 的 python3 应用别名不可执行。测试时创建隔离 venv，复用现有 DuckDB/PyArrow，并提供可运行的 python3.exe 名称，供 Go 差分测试调用；未修改系统 Python 或持久 PATH。隔离环境与构建缓存现已迁入本仓库 `.cache/agent-verification/`；原先误用的仓库外测试目录已迁走。
- 当前 Python 缺少 Polars、PySpark，Phase 1 四引擎互操作矩阵未执行。
- Phase 2/3 RSS 指标程序依赖 Linux `/proc/self/statm`；Linux 资源指标与完整 Phase 6 Gate 未完成。
- 未执行本轮性能基线与真实远端服务容器测试；已通过的协议测试使用仓库测试环境。

完整 Gate 本轮没有通过。项目阶段状态和架构契约未修改。

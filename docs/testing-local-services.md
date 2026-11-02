# 使用现有 MinIO / PostgreSQL 做本机测试

## 测试范围

`pkg/gosql/minio_integration_test.go` 通过公共 Go API 访问真实 MinIO，覆盖 CSV/Parquet 读取、排序、聚合、缺失对象错误处理和单文件 COPY 回读。设置环境变量后才启用；未设置 endpoint 时明确跳过。

PostgreSQL 当前用于独立验证本机连接与只读 SQL。引擎尚未提供 PostgreSQL 数据源或 PG Wire 接口，PG 服务可连接不代表引擎支持这些能力。

本次复用 `docker-minio-1` 与 `docker-postgres-1`，映射地址分别为 `127.0.0.1:9000` 与 `127.0.0.1:5432`。测试环境的实际端口以本机容器配置为准。

## MinIO 样本

准备一个名称以 `fssql-agent-test-` 开头的专用 bucket。测试只访问该 bucket；COPY 成功时会写入带时间戳的 `output-*.parquet`。

上传两个对象：

- `input.csv`：下列无表头 CSV。当前公共 CSV 默认不启用 Header，列名为 `column0`、`column1`、`column2`。
- `input.parquet`：相同的三行数据，字段为 `id: INT64`、`label: STRING`、`amount: INT64`。建议使用无压缩、无 dictionary 的 Parquet V1 样本。

```csv
1,alpha,10
2,beta,20
3,gamma,30
```

## 执行测试

在当前 PowerShell 进程设置测试连接配置。凭据使用现有服务的测试授权信息；不要将实际值提交到仓库。

```powershell
$env:FSSQL_MINIO_ENDPOINT = 'http://127.0.0.1:9000'
$env:FSSQL_MINIO_BUCKET = 'fssql-agent-test-你的测试标识'
$env:FSSQL_MINIO_ACCESS_KEY = '你的访问密钥'
$env:FSSQL_MINIO_SECRET_KEY = '你的私密密钥'

go test -count=1 -v -run '^TestMinIO' ./pkg/gosql
go test -race -count=1 -v -run '^TestMinIO' ./pkg/gosql
```

## 本次验证结果

本机真实服务验证中，CSV/Parquet 读取、聚合、404 错误清理和单文件 COPY 回读均通过普通与 race 测试；PostgreSQL 只读连接查询通过。

此前 Windows COPY 在本地 Manifest 目录同步阶段返回 `Access is denied`。现已拆分平台适配：Unix 保留文件同步、rename 与目录同步；Windows 在同步临时文件后，用 `MoveFileExW(MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)` 发布。删除先同步移走恢复入口，再清理临时 tombstone；首次创建目录也使用同步移动，并序列化同进程的目录发布。原生失败继续向调用方返回。

平台回归测试覆盖重复替换、幂等删除、取消、文件占用时旧状态保护、并发目录创建以及 Unicode/长路径。Windows 实现使用 [MoveFileExW 的官方同步移动契约](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw)；本次没有进行真实断电注入。

结果与日志见[服务测试报告](../artifacts/local-validation/2026-11-02/services/report.md)和[修复验证报告](../artifacts/local-validation/2026-11-02/windows-copy-fix/report.md)。连接凭据与本次辅助脚本保存在已忽略的 `.cache/agent-verification/` 中。

百万行数据、资源指标和重复 benchmark 的说明见[大文件测试](testing-large-minio.md)。

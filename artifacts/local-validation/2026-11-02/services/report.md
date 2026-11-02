# 现有服务的 Windows 本机集成测试

日期：2026-11-02。测试程序运行于 Windows，访问 WSL Docker 中既有的服务。

## 服务与结果

| 检查 | 普通模式 | Race 模式 |
| --- | --- | --- |
| MinIO CSV 读取、排序、结果与内存释放 | PASS | PASS |
| MinIO Parquet 读取、排序、结果与内存释放 | PASS | PASS |
| MinIO COUNT/SUM 经 database/sql 返回精确结果 | PASS | PASS |
| MinIO 缺失对象真实 404 与内存清理 | PASS | PASS |
| MinIO 单文件 COPY → Parquet 回读 | PASS | PASS |
| PostgreSQL 本机连接与只读 `SELECT version(), current_database(), 1 + 1` | PASS | 不适用 |

MinIO：`docker-minio-1`，本机 endpoint `http://127.0.0.1:9000`。

PostgreSQL：`docker-postgres-1`，本机地址 `127.0.0.1:5432`，实际版本 15.4。使用 psycopg 连接，并将事务设为只读。未创建数据库或修改业务数据。

MinIO 专用样本 bucket：`fssql-agent-test-20261102-32e9d8c7`。其中保留 `input.csv`、`input.parquet` 两个三行样本供复测。

## COPY 修复

修复前 `TestMinIOCopyRoundTrip` 在 `session: persist COPY WRITING manifest` 阶段失败，底层为 `sync <本地 Manifest 目录>: Access is denied.`。历史失败见 [修复前 Race 日志](minio-race-before-fix.log)。

修复后，Windows 使用同步文件与原生 write-through 移动发布 Manifest，删除使用同步移走恢复入口的方式。真实 MinIO COPY 已通过普通与 race 测试，导出对象回读值为 42，查询完成后受管内存为零。完整修复证据见[验证报告](../windows-copy-fix/report.md)。

## 日志和复测

- [普通集成测试](minio.log)
- [Race 集成测试](minio-race.log)
- [服务版本与样本信息](services.json)
- [复测说明](../../../../docs/testing-local-services.md)

未设置 MinIO 环境变量时，`go test ./pkg/gosql ./examples/quickstart` 与对应 race 检查通过；公共包 vet 通过。默认测试的通过不包含被明确跳过的真实 MinIO 测试。

当前授权后的只读 WSL Docker 检查已成功，Docker 客户端与服务端均为 29.5.0。本次复用现有服务，没有启动新的 MinIO/PostgreSQL 服务；没有通过容器运行引擎测试，也没有宣称完整 Linux Gate 通过。

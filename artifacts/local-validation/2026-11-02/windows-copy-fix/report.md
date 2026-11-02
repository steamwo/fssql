# Windows COPY Manifest 修复验证

## 修复内容

`LocalManifestStore.Save/Delete` 使用平台持久化适配。Unix 保留文件 Sync、rename 和目录 Sync；Windows 将已 Sync 的完整临时文件通过 `MoveFileExW` 的 `REPLACE_EXISTING | WRITE_THROUGH` 标志发布，使用同卷移动且不允许跨卷复制降级。首次目录创建逐级同步发布，同进程共享目录创建由互斥量串行化。

Windows 删除先将 Manifest 同步移到唯一 tombstone，再移除 tombstone。恢复入口在同步移动后即不再存在；崩溃遗留的 tombstone 不会被 Load 识别为 Manifest。原生移动、文件同步与清理失败均传播错误。

实现依据 [Microsoft MoveFileExW 接口契约](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw)。未修改冻结架构、Manifest 编码或 COPY 状态机。

## 回归测试

- 嵌套 Unicode 目录首次发布、重复保存及最新状态回读。
- 幂等删除、恢复入口消失与临时文件清理。
- 已取消保存保留旧 snapshot。
- Windows 真实文件共享冲突：Save/Delete 失败并保留旧 snapshot。
- Windows Unicode/长路径保存、加载与删除。
- 8 个并发调用首次创建共享目录。
- 既有 Manifest resume 与持久化回归测试。
- 真实 MinIO 单文件 COPY 发布及 Parquet 回读，结果值 42，受管内存归零。

## 检查结果

最终退出码保存在 [status.json](status.json)，原始输出见同目录日志：

- [全量普通测试](correctness.log)
- [全量 Race 测试](race.log)
- [Vet](vet.log)
- [架构检查](architecture.log)
- [DuckDB 强制兼容性](duckdb.log)
- [DuckDB Race 兼容性](duckdb-race.log)
- [Linux export 测试交叉编译](linux-compile.log)
- [真实 MinIO 普通测试](../services/minio.log)
- [真实 MinIO Race 测试](../services/minio-race.log)

## 验证边界

本轮在 Windows 本机执行运行测试。Linux 仅交叉编译，未执行 Linux Gate；未做真实断电注入，未声称验证任意文件系统或存储设备的掉电恢复能力。阶段状态未变更。

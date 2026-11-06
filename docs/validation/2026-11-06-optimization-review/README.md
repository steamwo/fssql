# 2026-11-06 引擎优化分析证据

本目录归档 [引擎优化计划](../../optimization-review-2026-11-06.md) 的历史诊断证据，整理文档时没有重新执行测试或测量。

| 文件 | 内容 |
| --- | --- |
| clean-samples.jsonl | 两条 CSV 复现记录，以及五种查询各七条预热后样本 |
| summary.json | 五种查询的中位数、耗时 MAD 和受管资源收尾汇总 |
| hmac-isolated.jsonl / window-isolated.jsonl | 各自独立进程的 CSV 复现记录与七条预热后样本 |
| hmac-alloc-top.txt / window-alloc-top.txt | 独立进程的累计 alloc_objects 采样，包含两次预热及七次测量 |
| csv-duckdb-reference.json | 本机固定 DuckDB 2.0.0 对引号空串的独立类型与结果 |
| quoted-empty.csv | CSV 自动推断失败的原始输入 |
| audit.go.txt | 诊断程序原文；使用文本后缀，不增加仓库 Go package 或测试入口 |

分析时 fssql 基线为 da17401 加工作区变更；编译环境 Go 1.27.3 windows/amd64、CGO_ENABLED=0，CPU i7-10510U（4 核/8 线程）。原诊断二进制 SHA-256：279B2A54F294D78E7D26BD5501319FDEC8BA74FB73F060AFDF7D8B157E8A2AE7。该二进制和原始 pprof 未纳入版本库，仍保留于本机 edge-agent-go 的 .cache/fssql-optimization-audit/。

数据完全合成，HMAC 使用公开测试材料。程序仅通过 gosql 公共入口读取合成数据，不连接业务数据库、对象存储或回调服务。CSV 失败记录不是成功断言；诊断程序输出错误供核对，不因预期的 CSV 失败返回非零退出码。

## 重跑诊断

在 fssql 仓库根目录执行，要求 go 在 PATH：

```powershell
$work = Join-Path $PWD '.cache\optimization-review-replay'
New-Item -ItemType Directory -Path $work -Force | Out-Null
Copy-Item 'docs\validation\2026-11-06-optimization-review\audit.go.txt' (Join-Path $work 'main.go')
$env:CGO_ENABLED = '0'
go build -o (Join-Path $work 'audit.exe') (Join-Path $work 'main.go')
& (Join-Path $work 'audit.exe') $work

# 隔离进程的分配 profile；输出位于同一工作目录
& (Join-Path $work 'audit.exe') $work hmac
& (Join-Path $work 'audit.exe') $work window
```

该命令使用当前工作区源码，不会重建历史基线。重跑结果应另行保存版本、环境和原始输出；历史 summary.json 不自动更新。DuckDB 参考结果由独立工具生成，程序本身不会替调用方安装或运行 DuckDB。

查询场景工作不同，不能直接把其耗时相除当作优化倍数；累计分配不等于峰值内存。回调只核对完整消费行数，不校验每个结果值。正式发布必须另做语义差分、资源边界和稳定性能测量。

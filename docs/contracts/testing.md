# Testing Contract

详细执行标准见 [项目测试规范与 DuckDB 对标方案](../testing-standard.md)。该标准规定功能、函数和公共方法覆盖、差分比较、性能测量、资源与失败矩阵、执行层级及发布证据；以下为必须保持的测试不变量。

## 必备体系

- Unit
- SQLLogic
- DuckDB Differential
- Property
- Fuzz
- Interop
- Fault Injection
- Deterministic Scheduler
- Leak Assertions
- Race Detector
- Performance Regression

## Verification Framework

```text
OptimizerVerifier
optimized == reference

VectorVerifier
FLAT == CONSTANT/DICTIONARY equivalent

ParallelVerifier
parallel == single-worker

SpillVerifier
in-memory == forced-spill

IOFaultVerifier
fault/retry semantic result == normal semantic result

DuckDBCompatibilityVerifier
our result == DuckDB
```

## Property Examples

```text
Equals(a,b) => Hash(a)==Hash(b)
Serialize→Deserialize == identity
Selection execution == materialize-then-execute
```

## Leak Assertions

Cancel / failure 后：

```text
active tasks = 0
active async requests = 0
query reservations = 0
query temp ownership = 0
client leases = 0
incomplete multipart = 0
```

# Testing Contract

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

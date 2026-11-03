# Scan and sort optimizations

This change preserves the Push Pipeline, internal Vector ABI, managed memory,
immutable ScanSet/SourceSnapshot and temporary-storage ownership contracts.

## Covered query shapes

- A global, single `COUNT(*)` directly over `read_parquet` uses exact footer row
  counts. Filters, grouping, DISTINCT and other aggregates retain the ordinary
  execution path. Every participating file still passes snapshot/schema checks;
  inconsistent or overflowing footer counts fail explicitly.
- A global, single `COUNT(*)` directly over `read_csv` uses a source-local
  lexical count. It still reads the CSV bytes and validates quoting and record
  width, but does not convert fields or allocate payload vectors. Binding and
  source identity checks are shared with the normal CSV source.
- A global, single `COUNT(*)` directly over `read_ndjson` shares frozen binding,
  bounded reads and framing with the normal source. It validates records in one
  pass without typed value conversion or payload vectors. JSON structure,
  trailing tokens and duplicate bound object fields are still checked. Filters,
  COUNT(column), grouping and multiple aggregates keep normal execution.
- Computed projections and aggregate inputs propagate column dependencies
  through Filter, Sort and Limit to the scan. Residual filter and sort columns
  remain available at their original bound indexes. Projection/Join/Distinct
  boundaries are not crossed without an index-mapping contract.
- CSV accepts safe scan LIMIT and column projection candidates. LIMIT uses
  bounded sequential reads to avoid the full-file parallel boundary prescan.
  Unread fields still undergo CSV lexical and row-width validation, but their
  type conversions and payload materialization are skipped, as with column
  projection in the Parquet reader.
- Streaming LIMIT reports normal completion before workers claim more source
  work. In-flight batches drain before source cleanup. A LIMIT after a blocking
  operator does not avoid building that operator's input. Join-aware sink
  transforms do not yet propagate this early-completion signal.
- CSV serial scans reuse a managed input window across batches. Partial records
  are retried from their record boundary; oversized records can still grow only
  through the managed allocator. Direct Source callers must Close when they
  stop consuming early. Exhaustion, failure and cancellation release the window.
- Unquoted CSV fields decode directly from the input span; only quoted fields
  need unescaping scratch. The record-count pass remains quote-aware and is
  retained to size output vectors exactly.
- Sort uses managed-index introsort with a bounded-depth heapsort fallback,
  borrowed immutable row views, prepared key descriptors and direct ownership
  transfer of full output batches. String comparisons retain full-key semantics.
  Sorted runs and K-way merge remain the external algorithm. Sorting remains
  serial; VARCHAR TopN selection and radix sorting are unchanged. Leading
  VARCHAR keys may use a nine-byte-per-row prefix cache within the existing
  sort budget. Collisions always compare complete keys, including NULL order,
  embedded NUL bytes and subsequent sort keys. Insufficient scratch budget
  retains the ordinary full comparator.
- Parquet fixed-width PLAIN values and all-defined RLE definition runs support
  batched scalar output. Numeric dictionary batches gather into owned vectors
  with one buffer acquisition, while decimals and timestamp unit conversions
  retain checked decode paths. Hybrid packed indexes use word extraction;
  PeekEntry uses inline state rather than a heap object per value.
- Parquet header discovery starts with a bounded 128-byte probe on larger
  chunks, retaining narrow probes for small tails. This reduces header round
  trips without introducing whole-file fallback or unbounded readahead.
- Parquet page-body reads may include up to 128 bytes of the next header within
  the same column chunk. A managed tail buffer keeps that probe alive across page
  release, so a complete cached header avoids another request. Long headers use
  normal checked discovery. Memory-limit failures fall back to exact-body reads;
  actual I/O and identity errors fail without retries. This reduces request
  round trips, rather than column data volume or the number of decoded values.
- Snappy matches copy an initialized prefix and double it for overlapping runs,
  replacing per-byte back references. Decoding still targets managed memory and
  retains length and offset checks. No compression dependency was added.
- Global aggregates resolve their single group once per batch and reuse the
  state-buffer view. COUNT(*) updates cardinality once per batch. Grouped
  aggregation and spill/combine algorithms are unchanged.
- Projection transfers owned expression results directly and copies column
  references in batches. Filtering preserves constant NULL placeholders rather
  than expanding unread columns into flat payloads. All returned replacements
  still own storage independently from their input.
- TopN compares only keys against the heap threshold before serializing a row;
  rejected rows do not allocate or copy payload scratch.
- Fixed-width global COUNT/SUM/AVG/MIN/MAX borrow one immutable input-buffer
  view per batch and write the accumulator state once. Integer/decimal SUM
  retains checked Int128 addition; floating-point input order, NULL behavior,
  dictionary/constant vectors and AVG overflow checks are preserved.
- TopN borrows a fixed-width leading key once per batch and caches the current
  heap threshold until replacement. Equal leading keys still compare all
  remaining keys, with the existing floating-point and NULL ordering rules.
- CSV advances ordinary data spans using word checks (and quote search inside
  quoted fields). Structural bytes still use the shared lexical state machine;
  quoted scratch copies spans instead of appending individual bytes. The two
  passes still size vectors exactly and validate row width and quoted syntax.
- Parquet dictionary indexes fill batches by RLE/packed run. Scalar decoding
  also batches all-defined packed prefixes and NULL RLE runs, retaining scalar
  checks for malformed definition levels and unsupported value conversions.
- NDJSON translates optimizer column indexes to literal top-level field paths
  after the frozen binding check. Unread top-level columns remain constant NULL
  placeholders at their bound indexes. Syntax validation and residual operators
  remain active, including duplicate known fields and malformed unread values.
  Projections without a selected LIST decode in one pass; selected LISTs retain
  exact child counting before managed allocation. Root duplicate-field scratch
  is reused per batch, and newline framing uses byte search. This reduces CPU
  work and materialization; it does not reduce the JSON bytes read. Literal
  field names containing dots are not interpreted as nested paths.

## Manual validation

Tests and benchmarks were not executed for this change, at the user's request.
Regression tests were added for metadata counts, column dependencies, CSV
LIMIT/window reuse/quoted fields, streaming early completion and string sorting
with both in-memory and forced-spill execution.

Suggested commands for manual execution:

```sh
go test ./internal/core/vector ./internal/optimizer ./internal/session ./internal/format/parquet ./internal/format/csv ./internal/format/json ./internal/execution/aggregate ./internal/execution/lowering ./internal/execution/sqltransform ./internal/execution/sort ./internal/execution/hash ./internal/execution/pipeline
go test ./...
go test -race ./...
go test ./internal/execution/aggregate -run '^$' -bench BenchmarkGlobalFixedReduction -benchmem
go test ./internal/format/csv -run '^$' -bench BenchmarkCSVDataSpans -benchmem
go test ./internal/format/json -run '^$' -bench BenchmarkNDJSONScalarProjection -benchmem
go test ./internal/format/json -run '^$' -bench BenchmarkNDJSONCountValidation -benchmem
go test ./internal/format/parquet -run '^$' -bench BenchmarkSnappyCopyMatch -benchmem
```

Repeat the original performance matrix with the same SQL, data, thread limits,
memory budgets, result-consumption path and cold/warm cache conditions. Record
actual range bytes/requests, CPU and elapsed time, allocations/GC, managed peak,
RSS and spill volume. No speedup or memory measurements are claimed here.

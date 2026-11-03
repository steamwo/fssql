# Registered table functions

`gosql.Config.TableFunctions` freezes fixed-schema, zero-argument table functions when `gosql.Open` succeeds. Registration validates the function name, schema, supported scalar logical types, duplicate names, and the `Open` callback. Definitions cannot be mutated while the DB is running.

Binding and schema inspection use only the frozen definition. They do not invoke `Open` or consume rows. Execution opens a `BatchReader` lazily when the source is first consumed and opens the registered source at most once per query.

`BatchReader.Next` receives an engine-owned `BatchWriter` and returns the number of rows filled. `BatchWriter.Capacity()` never exceeds `vector.DefaultVectorSize`; fixed row storage uses a 1 MiB default batch target. Variable-width payloads remain managed and a single large value may dominate a batch only after its managed reservation succeeds. Cells begin as NULL, explicit `SetNull` is supported, and non-nullable cells are checked before publication.

Typed setters cover BOOLEAN, signed and unsigned integers, FLOAT/DOUBLE, exact DECIMAL, VARCHAR, BLOB, DATE, TIMESTAMP, TIMESTAMP_NS and TIMESTAMPTZ. VARCHAR/BLOB bytes are copied into managed vector storage during the setter call. DECIMAL text is converted exactly without `float64`; values requiring undeclared rounding are rejected.

A source referenced once streams directly. Multiple references in the same query share one query-scoped materialization. Native `DataChunk` serialization preserves type modifiers and NULL validity; retained batches stay in managed memory up to `SpillMemoryBudgetBytes`, then spill through the existing checksummed temporary page store. CSV is not an intermediate format.

The first open/read/materialization/spill error is terminal for that query source. Execution never reopens the upstream reader or silently falls back to another ingestion path. Query cleanup closes the reader, releases retained managed buffers and spill pages, then closes temporary and memory scopes. Empty sources still expose their registered schema and produce zero data chunks.

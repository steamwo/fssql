# Phase 6 Failure Matrix

Date: 2026-11-02
Candidate commit: `fd737c3c6f45f47fe1ba1735d09658e6c1120bc1`
Workflow run: `41430739607`

## Repository-wide verification

The release gate passed:

- `go test ./...`
- `go test -race ./...`
- `go vet ./...`
- `go run ./tools/architecture-check .`

## Forced-spill correctness

The release gate revalidated equality / bounded terminal behavior for:

- external hash join forced spill
- skew fallback bounded terminal join
- external aggregate forced spill
- external DISTINCT forced spill
- external sort forced spill
- Top-N vs full sort + limit

All targeted tests passed.

## Fault / leak matrix

`go test -count=1 ./test/fault/... ./test/leak/...` passed.

The Phase 5 production failure suites were intentionally re-run as current release gates rather than treated as historical evidence:

- `scripts/phase5-cancellation-leak.sh` — PASS
- `scripts/phase5-security.sh` — PASS
- `scripts/phase5-config-compatibility.sh` — PASS
- `scripts/phase5-stability.sh` — PASS

The cancellation/leak gate covered query/server lifecycle, async I/O cancellation, temporary-storage ownership, bounded SFTP leases, S3 multipart cleanup, and race-sensitive lifecycle packages. All completed with zero leaked owned resources according to their assertions.

The stability gate repeated server/query lifecycle and remote FTP/WebDAV/SFTP/S3 ownership paths and re-ran the architecture boundary check. It passed.

## Release blocker regression

The first Phase 6 run failed because the Phase 1 interop runner stdout mixed a generator diagnostic path with machine-readable JSON. The runner now redirects generator stdout to stderr, and `test/interop/phase1/run_output_test.py` verifies this output contract. The regression test passed before the real interop matrix in the accepted release run.

## Result

Failure, cancellation, lifecycle, recovery, race and stability evidence required by Phase 6 is current and passing. No known resource-leak or failure-path blocker remains for v1.0.

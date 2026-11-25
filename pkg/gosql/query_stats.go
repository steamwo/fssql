package gosql

import (
	"context"
	"time"
)

// QueryStats describes one execution, including any rows delivered before an error.
type QueryStats struct {
	PlanningTime     time.Duration // Includes argument binding and source discovery.
	ExecutionTime    time.Duration // Includes time spent in the callback.
	Rows             int64
	Chunks           int64
	PeakManagedBytes int64 // Peak bytes reserved by the query's managed memory scope.
	PeakTempBytes    int64 // Peak bytes reserved by the query's temporary storage scope.
	FilesMatched     int   // Distinct inputs found during query binding and execution.
	FilesRead        int   // Distinct inputs with at least one completed range read.
	ReadRequests     int64 // Completed format-reader requests, including metadata and sampling.
	BytesRead        int64 // Bytes returned to format readers by those requests.
}

// QueryChunksWithStats streams chunks to fn and returns per-query statistics.
// Optional value parameters follow the same rules as QueryChunksWithArgs.
func (db *DB) QueryChunksWithStats(ctx context.Context, sql string, fn func(*Chunk) error, args ...any) ([]Column, QueryStats, error) {
	var stats QueryStats
	columns, err := db.queryChunks(ctx, sql, fn, args, &stats)
	return columns, stats, err
}

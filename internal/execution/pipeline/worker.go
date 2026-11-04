package pipeline

import "context"

type workerIDKey struct{}

// WithWorkerID marks a pipeline worker context so operators can keep
// goroutine-local accumulator state without a shared lookup table.
func WithWorkerID(ctx context.Context, id int) context.Context {
	return context.WithValue(ctx, workerIDKey{}, id)
}

// WorkerID returns the stable worker index assigned by Pipeline.Run.
func WorkerID(ctx context.Context) (int, bool) {
	id, ok := ctx.Value(workerIDKey{}).(int)
	return id, ok
}

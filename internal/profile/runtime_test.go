package profile

import "testing"

func TestRuntimeMetricsAggregatesQueryWorkerIOAndMemory(t *testing.T) {
	metrics := &RuntimeMetrics{}
	metrics.QuerySubmitted()
	metrics.QuerySubmitted()
	metrics.QueryFinished(QueryOutcomeSucceeded)
	metrics.QueryFinished(QueryOutcomeCanceled)

	worker := &WorkerCounters{}
	worker.AddPipelineTask()
	worker.AddOperatorCall()
	worker.AddChunk(2048)
	metrics.MergeWorker(worker)
	if *worker != (WorkerCounters{}) {
		t.Fatalf("worker counters not reset: %+v", *worker)
	}
	metrics.AddIO(4096)
	metrics.AddSpill(8192)
	metrics.SetManagedMemory(16384)

	s := metrics.Snapshot()
	if s.QueriesSubmitted != 2 || s.QueriesSucceeded != 1 || s.QueriesCanceled != 1 || s.QueriesFailed != 0 || s.ActiveQueries != 0 {
		t.Fatalf("query metrics=%+v", s)
	}
	if s.PipelineTasks != 1 || s.OperatorCalls != 1 || s.ChunksProduced != 1 || s.RowsProduced != 2048 {
		t.Fatalf("execution metrics=%+v", s)
	}
	if s.IORequests != 1 || s.IOBytes != 4096 || s.SpillBytes != 8192 || s.ManagedMemoryBytes != 16384 {
		t.Fatalf("resource metrics=%+v", s)
	}
}

func TestRuntimeMetricsDoesNotFinishUnknownOutcome(t *testing.T) {
	metrics := &RuntimeMetrics{}
	metrics.QuerySubmitted()
	metrics.QueryFinished(QueryOutcome(99))
	s := metrics.Snapshot()
	if s.ActiveQueries != 1 || s.QueriesSucceeded != 0 || s.QueriesFailed != 0 || s.QueriesCanceled != 0 {
		t.Fatalf("invalid outcome mutated terminal metrics: %+v", s)
	}
}

func BenchmarkWorkerCountersAddChunk(b *testing.B) {
	var worker WorkerCounters
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		worker.AddChunk(2048)
	}
	if worker.ChunksProduced == 0 {
		b.Fatal("counter did not advance")
	}
}

func BenchmarkRuntimeMetricsMergeWorker(b *testing.B) {
	metrics := &RuntimeMetrics{}
	worker := &WorkerCounters{PipelineTasks: 1, OperatorCalls: 4, ChunksProduced: 2, RowsProduced: 4096}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		metrics.MergeWorker(worker)
		worker.PipelineTasks = 1
		worker.OperatorCalls = 4
		worker.ChunksProduced = 2
		worker.RowsProduced = 4096
	}
}

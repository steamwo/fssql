package profile

import "sync/atomic"

// QueryOutcome 表示 Query lifecycle 终态，用于统一 query metrics 记账。
type QueryOutcome uint8

const (
	// QueryOutcomeSucceeded 表示 Query 正常成功。
	QueryOutcomeSucceeded QueryOutcome = iota + 1
	// QueryOutcomeFailed 表示 Query 失败终止。
	QueryOutcomeFailed
	// QueryOutcomeCanceled 表示 Query 取消终止。
	QueryOutcomeCanceled
)

// RuntimeSnapshot 是 Query/Pipeline/Operator/I/O/Memory-Spill 的只读指标快照。
// Snapshot 只包含数值，不暴露 exporter SDK 或内部原子对象。
type RuntimeSnapshot struct {
	// QueriesSubmitted 是累计进入 server QueryRegistry 的 Query 数。
	QueriesSubmitted int64
	// QueriesSucceeded 是累计成功 Query 数。
	QueriesSucceeded int64
	// QueriesFailed 是累计失败 Query 数。
	QueriesFailed int64
	// QueriesCanceled 是累计取消 Query 数。
	QueriesCanceled int64
	// ActiveQueries 是当前尚未成功完成 lifecycle cleanup 的 Query 数。
	ActiveQueries int64
	// PipelineTasks 是累计完成的 pipeline task 数。
	PipelineTasks int64
	// OperatorCalls 是累计 operator chunk-call 数，不按 row 做 interface dispatch。
	OperatorCalls int64
	// ChunksProduced 是累计输出 DataChunk 数。
	ChunksProduced int64
	// RowsProduced 是累计输出 row 数。
	RowsProduced int64
	// IORequests 是累计远程/本地 I/O request 数。
	IORequests int64
	// IOBytes 是累计 I/O 字节数。
	IOBytes int64
	// ManagedMemoryBytes 是最近一次采样的 engine managed memory 使用量。
	ManagedMemoryBytes int64
	// SpillBytes 是累计 spill 写出字节数。
	SpillBytes int64
}

// RuntimeMetrics 使用原子计数保存跨 worker/query 聚合结果；worker 热路径先写 WorkerCounters，再批量 MergeWorker。
type RuntimeMetrics struct {
	// querySubmitted/querySucceeded/queryFailed/queryCanceled 记录 query lifecycle counter。
	querySubmitted atomic.Int64
	querySucceeded atomic.Int64
	queryFailed    atomic.Int64
	queryCanceled  atomic.Int64
	// activeQueries 记录尚未完成 lifecycle cleanup 的 Query gauge。
	activeQueries atomic.Int64
	// pipelineTasks/operatorCalls/chunksProduced/rowsProduced 聚合执行指标。
	pipelineTasks  atomic.Int64
	operatorCalls  atomic.Int64
	chunksProduced atomic.Int64
	rowsProduced   atomic.Int64
	// ioRequests/ioBytes 聚合 I/O 指标。
	ioRequests atomic.Int64
	ioBytes    atomic.Int64
	// managedMemoryBytes 是外部 MemoryManager 周期采样写入的 gauge。
	managedMemoryBytes atomic.Int64
	// spillBytes 聚合 spill I/O 字节数。
	spillBytes atomic.Int64
}

// WorkerCounters 是 worker-local 无锁计数器；同一实例只能由一个 worker 写入。
type WorkerCounters struct {
	// PipelineTasks 是 worker 已完成的 task 数。
	PipelineTasks int64
	// OperatorCalls 是 worker 执行的 operator chunk-call 数。
	OperatorCalls int64
	// ChunksProduced 是 worker 输出的 chunk 数。
	ChunksProduced int64
	// RowsProduced 是 worker 输出的 row 数。
	RowsProduced int64
}

// AddPipelineTask 在 worker-local 热路径累加一个 task。
func (w *WorkerCounters) AddPipelineTask() { w.PipelineTasks++ }

// AddOperatorCall 在 worker-local 热路径累加一次 operator chunk-call。
func (w *WorkerCounters) AddOperatorCall() { w.OperatorCalls++ }

// AddChunk 在 worker-local 热路径一次性累加 chunk 与 row；禁止逐 row 调用 exporter。
func (w *WorkerCounters) AddChunk(rows int) {
	w.ChunksProduced++
	w.RowsProduced += int64(rows)
}

// Reset 清零 worker-local counters，通常在 MergeWorker 后调用。
func (w *WorkerCounters) Reset() { *w = WorkerCounters{} }

// QuerySubmitted 记录一个 Query 已进入 QueryRegistry，并增加 active gauge。
func (m *RuntimeMetrics) QuerySubmitted() {
	if m == nil {
		return
	}
	m.querySubmitted.Add(1)
	m.activeQueries.Add(1)
}

// QueryFinished 在 QueryContext lifecycle cleanup 成功后记录唯一终态并减少 active gauge。
func (m *RuntimeMetrics) QueryFinished(outcome QueryOutcome) {
	if m == nil {
		return
	}
	switch outcome {
	case QueryOutcomeSucceeded:
		m.querySucceeded.Add(1)
	case QueryOutcomeFailed:
		m.queryFailed.Add(1)
	case QueryOutcomeCanceled:
		m.queryCanceled.Add(1)
	default:
		return
	}
	m.activeQueries.Add(-1)
}

// MergeWorker 在 task/chunk 边界把 worker-local counters 批量合并到全局原子指标。
func (m *RuntimeMetrics) MergeWorker(w *WorkerCounters) {
	if m == nil || w == nil {
		return
	}
	m.pipelineTasks.Add(w.PipelineTasks)
	m.operatorCalls.Add(w.OperatorCalls)
	m.chunksProduced.Add(w.ChunksProduced)
	m.rowsProduced.Add(w.RowsProduced)
	w.Reset()
}

// AddIO 在 I/O request 完成边界累加 request/bytes，不保存 endpoint/credential 等高基数字符串。
func (m *RuntimeMetrics) AddIO(bytes int64) {
	if m == nil || bytes < 0 {
		return
	}
	m.ioRequests.Add(1)
	m.ioBytes.Add(bytes)
}

// AddSpill 在 spill write 完成边界累加写出字节。
func (m *RuntimeMetrics) AddSpill(bytes int64) {
	if m == nil || bytes < 0 {
		return
	}
	m.spillBytes.Add(bytes)
}

// SetManagedMemory 写入 MemoryManager 的最新 managed-used gauge。
func (m *RuntimeMetrics) SetManagedMemory(bytes int64) {
	if m == nil || bytes < 0 {
		return
	}
	m.managedMemoryBytes.Store(bytes)
}

// Snapshot 原子读取统一 runtime metrics；读取过程不阻塞 worker。
func (m *RuntimeMetrics) Snapshot() RuntimeSnapshot {
	if m == nil {
		return RuntimeSnapshot{}
	}
	return RuntimeSnapshot{
		QueriesSubmitted:   m.querySubmitted.Load(),
		QueriesSucceeded:   m.querySucceeded.Load(),
		QueriesFailed:      m.queryFailed.Load(),
		QueriesCanceled:    m.queryCanceled.Load(),
		ActiveQueries:      m.activeQueries.Load(),
		PipelineTasks:      m.pipelineTasks.Load(),
		OperatorCalls:      m.operatorCalls.Load(),
		ChunksProduced:     m.chunksProduced.Load(),
		RowsProduced:       m.rowsProduced.Load(),
		IORequests:         m.ioRequests.Load(),
		IOBytes:            m.ioBytes.Load(),
		ManagedMemoryBytes: m.managedMemoryBytes.Load(),
		SpillBytes:         m.spillBytes.Load(),
	}
}

package profile

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// PrometheusAdapter 把 RuntimeMetrics 快照导出为 Prometheus text exposition；不在 worker 热路径做格式化。
type PrometheusAdapter struct {
	// Metrics 是统一 runtime metrics 源。
	Metrics *RuntimeMetrics
}

// WriteText 在 scrape/read 边界生成 Prometheus 文本，不持有执行期资源。
func (a PrometheusAdapter) WriteText(w io.Writer) error {
	if w == nil {
		return errors.New("profile: nil Prometheus writer")
	}
	s := a.Metrics.Snapshot()
	metrics := []struct {
		name  string
		help  string
		kind  string
		value int64
	}{
		{"fssql_queries_submitted_total", "Queries admitted to the server registry.", "counter", s.QueriesSubmitted},
		{"fssql_queries_succeeded_total", "Queries completed successfully.", "counter", s.QueriesSucceeded},
		{"fssql_queries_failed_total", "Queries completed with failure.", "counter", s.QueriesFailed},
		{"fssql_queries_canceled_total", "Queries completed by cancellation.", "counter", s.QueriesCanceled},
		{"fssql_active_queries", "Queries whose lifecycle cleanup is not complete.", "gauge", s.ActiveQueries},
		{"fssql_pipeline_tasks_total", "Completed pipeline tasks.", "counter", s.PipelineTasks},
		{"fssql_operator_calls_total", "Operator chunk calls.", "counter", s.OperatorCalls},
		{"fssql_chunks_produced_total", "Produced DataChunks.", "counter", s.ChunksProduced},
		{"fssql_rows_produced_total", "Produced rows.", "counter", s.RowsProduced},
		{"fssql_io_requests_total", "Completed I/O requests.", "counter", s.IORequests},
		{"fssql_io_bytes_total", "Transferred I/O bytes.", "counter", s.IOBytes},
		{"fssql_managed_memory_bytes", "Current managed-memory bytes.", "gauge", s.ManagedMemoryBytes},
		{"fssql_spill_bytes_total", "Spill bytes written.", "counter", s.SpillBytes},
	}
	for _, metric := range metrics {
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %d\n", metric.name, metric.help, metric.name, metric.kind, metric.name, metric.value); err != nil {
			return err
		}
	}
	return nil
}

// OpenTelemetrySink 是对 OTel SDK 的窄适配边界；具体 SDK adapter 可在应用 wiring 层实现此接口。
// 这样第三方 SDK 类型不会进入 core/profile ABI。
type OpenTelemetrySink interface {
	// Counter 记录单调累计指标。
	Counter(context.Context, string, int64) error
	// Gauge 记录当前值指标。
	Gauge(context.Context, string, int64) error
}

// OpenTelemetryAdapter 在 export/scrape 边界把 RuntimeSnapshot 推送给 OTel sink。
type OpenTelemetryAdapter struct {
	// Metrics 是统一 runtime metrics 源。
	Metrics *RuntimeMetrics
}

// Export 把当前快照写入 sink；任何 exporter 失败都向上返回，不影响 engine 内部计数语义。
func (a OpenTelemetryAdapter) Export(ctx context.Context, sink OpenTelemetrySink) error {
	if sink == nil {
		return errors.New("profile: nil OpenTelemetry sink")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s := a.Metrics.Snapshot()
	counters := []struct {
		name  string
		value int64
	}{
		{"fssql.queries.submitted", s.QueriesSubmitted},
		{"fssql.queries.succeeded", s.QueriesSucceeded},
		{"fssql.queries.failed", s.QueriesFailed},
		{"fssql.queries.canceled", s.QueriesCanceled},
		{"fssql.pipeline.tasks", s.PipelineTasks},
		{"fssql.operator.calls", s.OperatorCalls},
		{"fssql.chunks.produced", s.ChunksProduced},
		{"fssql.rows.produced", s.RowsProduced},
		{"fssql.io.requests", s.IORequests},
		{"fssql.io.bytes", s.IOBytes},
		{"fssql.spill.bytes", s.SpillBytes},
	}
	for _, metric := range counters {
		if err := sink.Counter(ctx, metric.name, metric.value); err != nil {
			return err
		}
	}
	if err := sink.Gauge(ctx, "fssql.queries.active", s.ActiveQueries); err != nil {
		return err
	}
	return sink.Gauge(ctx, "fssql.memory.managed_bytes", s.ManagedMemoryBytes)
}

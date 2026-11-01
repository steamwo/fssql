package profile

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

// recordingOTelSink 记录 adapter 输出，验证 profile 不依赖具体 OTel SDK 类型。
type recordingOTelSink struct {
	// counters 保存 counter 名称和值。
	counters map[string]int64
	// gauges 保存 gauge 名称和值。
	gauges map[string]int64
	// failName 用于注入 exporter failure。
	failName string
}

// Counter 记录一个 counter。
func (s *recordingOTelSink) Counter(_ context.Context, name string, value int64) error {
	if name == s.failName {
		return errors.New("injected counter failure")
	}
	if s.counters == nil {
		s.counters = make(map[string]int64)
	}
	s.counters[name] = value
	return nil
}

// Gauge 记录一个 gauge。
func (s *recordingOTelSink) Gauge(_ context.Context, name string, value int64) error {
	if name == s.failName {
		return errors.New("injected gauge failure")
	}
	if s.gauges == nil {
		s.gauges = make(map[string]int64)
	}
	s.gauges[name] = value
	return nil
}

func testRuntimeMetrics() *RuntimeMetrics {
	metrics := &RuntimeMetrics{}
	metrics.QuerySubmitted()
	metrics.QueryFinished(QueryOutcomeSucceeded)
	worker := &WorkerCounters{PipelineTasks: 2, OperatorCalls: 3, ChunksProduced: 4, RowsProduced: 5}
	metrics.MergeWorker(worker)
	metrics.AddIO(6)
	metrics.SetManagedMemory(7)
	metrics.AddSpill(8)
	return metrics
}

func TestPrometheusAdapterExportsStableSnapshot(t *testing.T) {
	var out bytes.Buffer
	if err := (PrometheusAdapter{Metrics: testRuntimeMetrics()}).WriteText(&out); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{
		"# TYPE fssql_queries_submitted_total counter",
		"fssql_queries_submitted_total 1",
		"fssql_queries_succeeded_total 1",
		"fssql_active_queries 0",
		"fssql_rows_produced_total 5",
		"fssql_io_bytes_total 6",
		"fssql_managed_memory_bytes 7",
		"fssql_spill_bytes_total 8",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("Prometheus output missing %q:\n%s", want, text)
		}
	}
}

func TestOpenTelemetryAdapterExportsCountersAndGauges(t *testing.T) {
	sink := &recordingOTelSink{}
	if err := (OpenTelemetryAdapter{Metrics: testRuntimeMetrics()}).Export(context.Background(), sink); err != nil {
		t.Fatal(err)
	}
	if sink.counters["fssql.queries.submitted"] != 1 || sink.counters["fssql.rows.produced"] != 5 || sink.counters["fssql.io.bytes"] != 6 || sink.counters["fssql.spill.bytes"] != 8 {
		t.Fatalf("counters=%+v", sink.counters)
	}
	if sink.gauges["fssql.queries.active"] != 0 || sink.gauges["fssql.memory.managed_bytes"] != 7 {
		t.Fatalf("gauges=%+v", sink.gauges)
	}
}

func TestOpenTelemetryAdapterPropagatesExporterFailureAndCancellation(t *testing.T) {
	sink := &recordingOTelSink{failName: "fssql.io.bytes"}
	if err := (OpenTelemetryAdapter{Metrics: testRuntimeMetrics()}).Export(context.Background(), sink); err == nil {
		t.Fatal("expected exporter failure")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := (OpenTelemetryAdapter{Metrics: testRuntimeMetrics()}).Export(ctx, &recordingOTelSink{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v, want canceled", err)
	}
}

package json

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func TestNDJSONCountUsesFrozenSourceWithoutPayloadVectors(t *testing.T) {
	data := append([]byte("\xef\xbb\xbf\n \r\n"), fixture(5000)...)
	data = append(data, []byte("\n{\"id\":5000,\"name\":\"last\",\"tags\":[]}")...)
	driver := &trackingNDJSONDriver{memDriver: newMem(t, data, vfs.Capabilities(vfs.RandomRead, vfs.ParallelRandomRead))}
	registry, _ := vfs.NewRegistry(driver)
	m := memory.NewManager(4 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	binder, err := NewNDJSONBinder(registry, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := binder.Bind(context.Background(), driver.uri.String(), NDJSONBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := NDJSONRuntimeSourceConfig{Binder: binder, Pattern: driver.uri.String(), ExpectedFingerprint: binding.Fingerprint(), Scan: NDJSONScanOptions{EnableParallel: true, TargetRangeBytes: 1024}}
	source, err := NewNDJSONCountSource(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !source.source.countOnly {
		t.Fatal("COUNT source did not enable validation-only batches")
	}
	for _, job := range source.source.jobs {
		if !job.serial {
			t.Fatal("COUNT performed parallel boundary discovery")
		}
	}
	driver.reset(0)
	out, ok, err := source.Next(context.Background())
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if len(out.Columns) != 1 || out.Cardinality != 1 {
		t.Fatal("COUNT output shape changed")
	}
	if got, valid := out.Columns[0].Int64(0); !valid || got != 5001 {
		t.Fatalf("count=%d valid=%v", got, valid)
	}
	out.Release()
	if out, ok, err := source.Next(context.Background()); err != nil || ok || out != nil {
		t.Fatalf("COUNT did not finish: ok=%v err=%v", ok, err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if _, maxRange := driver.stats(); maxRange > 1024 {
		t.Fatalf("COUNT range grew beyond configured window: %d", maxRange)
	}
	cfg.ExpectedFingerprint = "changed"
	if source, err := NewNDJSONCountSource(context.Background(), cfg); err == nil || source != nil {
		t.Fatal("COUNT accepted snapshot drift")
	}
	if m.Used() != 0 {
		t.Fatalf("managed leak=%d", m.Used())
	}
}

func TestNDJSONCountValidationErrorsAndAllocationFailure(t *testing.T) {
	m := memory.NewManager(1 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	for _, raw := range []string{`[]`, `{"id":1,}`, `{"id":1,"id":2}`, `{"profile":{"name":"x","name":"y"}}`, `{"tags":[1,]}`, `{"id":1} true`, `{"tags":["bad\q"]}`} {
		if err := validateCountRecords(context.Background(), [][]byte{[]byte(raw)}, nestedSchema(), scope, bm); err == nil {
			t.Fatalf("invalid count record accepted: %s", raw)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := validateCountRecords(ctx, [][]byte{[]byte(`{"id":1}`)}, nestedSchema(), scope, bm); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
	if m.Used() != 0 {
		t.Fatalf("managed leak=%d", m.Used())
	}
	limited := memory.NewManager(128)
	if err := validateCountRecords(context.Background(), [][]byte{[]byte(`{"id":1}`)}, nestedSchema(), limited.NewScope(), bm); !errors.Is(err, memory.ErrLimitExceeded) || limited.Used() != 0 {
		t.Fatalf("allocation failure=%v used=%d", err, limited.Used())
	}
}

func TestNDJSONCountRangeFailureCancellationAndClose(t *testing.T) {
	driver := &trackingNDJSONDriver{memDriver: newMem(t, fixture(20), vfs.Capabilities(vfs.RandomRead))}
	registry, _ := vfs.NewRegistry(driver)
	m := memory.NewManager(4 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	binder, _ := NewNDJSONBinder(registry, scope, bm)
	binding, err := binder.Bind(context.Background(), driver.uri.String(), NDJSONBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := NDJSONRuntimeSourceConfig{Binder: binder, Pattern: driver.uri.String(), ExpectedFingerprint: binding.Fingerprint()}
	for _, mode := range []string{"range-failure", "cancel", "close"} {
		driver.reset(0)
		source, err := NewNDJSONCountSource(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		if mode == "range-failure" {
			driver.reset(1)
		} else if mode == "cancel" {
			var cancel context.CancelFunc
			ctx, cancel = context.WithCancel(ctx)
			cancel()
		} else {
			_ = source.Close()
		}
		out, ok, err := source.Next(ctx)
		if out != nil || ok || (mode == "close") != (err == nil) {
			t.Fatalf("mode=%s ok=%v err=%v", mode, ok, err)
		}
		_ = source.Close()
		if len(source.source.jobs) != 0 || m.Used() != 0 {
			t.Fatalf("mode=%s jobs=%d managed=%d", mode, len(source.source.jobs), m.Used())
		}
	}
}

func BenchmarkNDJSONCountValidation(b *testing.B) {
	records := bytes.Split(bytes.TrimSpace(fixture(1024)), []byte{'\n'})
	m := memory.NewManager(4 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	schema := nestedSchema()
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := validateCountRecords(context.Background(), records, schema, scope, bm); err != nil {
			b.Fatal(err)
		}
	}
}

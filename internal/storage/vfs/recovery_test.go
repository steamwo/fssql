package vfs

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
)

// testRecoveryDriver 为 VFS recovery contract 提供最小故障注入实现。
type testRecoveryDriver struct {
	testWriteDriver
	// calls 记录真正进入协议 recovery 的次数。
	calls int
	// report/err 是下一次 recovery 返回值。
	report RecoveryReport
	err    error
}

// RecoverIncompleteWrites 模拟协议级 cleanup，不暴露任何私有 token。
func (d *testRecoveryDriver) RecoverIncompleteWrites(context.Context, URI, *memory.Scope) (RecoveryReport, error) {
	d.calls++
	return d.report, d.err
}

func TestRecoverIncompleteWritesRequiresCapability(t *testing.T) {
	d := &testRecoveryDriver{
		testWriteDriver: testWriteDriver{testReadOnlyDriver: testReadOnlyDriver{caps: Capabilities(StreamingWrite)}},
		report:          RecoveryReport{Discovered: 1, Recovered: 1},
	}
	r, err := NewRegistry(d)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := ParseURI("test://host/staging/job-1/")
	scope := memory.NewManager(1024).NewScope()
	if _, err := r.RecoverIncompleteWrites(context.Background(), u, scope); !errors.Is(err, ErrIncompleteWriteRecoveryUnsupported) {
		t.Fatalf("expected unsupported recovery, got %v", err)
	}
	if d.calls != 0 {
		t.Fatalf("recovery driver called without capability: %d", d.calls)
	}

	d.caps = Capabilities(StreamingWrite, IncompleteWriteRecovery)
	report, err := r.RecoverIncompleteWrites(context.Background(), u, scope)
	if err != nil {
		t.Fatal(err)
	}
	if report.Discovered != 1 || report.Recovered != 1 || d.calls != 1 {
		t.Fatalf("report=%+v calls=%d", report, d.calls)
	}
}

func TestRecoverIncompleteWritesRejectsRootScopeAndInvalidReport(t *testing.T) {
	d := &testRecoveryDriver{
		testWriteDriver: testWriteDriver{testReadOnlyDriver: testReadOnlyDriver{caps: Capabilities(StreamingWrite, IncompleteWriteRecovery)}},
		report:          RecoveryReport{Discovered: 1, Recovered: 2},
	}
	r, _ := NewRegistry(d)
	scope := memory.NewManager(1024).NewScope()
	root, _ := ParseURI("test://host/")
	if _, err := r.RecoverIncompleteWrites(context.Background(), root, scope); err == nil {
		t.Fatal("expected root recovery scope rejection")
	}
	if d.calls != 0 {
		t.Fatalf("root recovery called driver: %d", d.calls)
	}

	u, _ := ParseURI("test://host/staging/job-1/")
	if _, err := r.RecoverIncompleteWrites(context.Background(), u, scope); err == nil {
		t.Fatal("expected invalid recovery report rejection")
	}
}

func TestRecoverIncompleteWritesHonorsCancellationBeforeDriver(t *testing.T) {
	d := &testRecoveryDriver{
		testWriteDriver: testWriteDriver{testReadOnlyDriver: testReadOnlyDriver{caps: Capabilities(StreamingWrite, IncompleteWriteRecovery)}},
	}
	r, _ := NewRegistry(d)
	u, _ := ParseURI("test://host/staging/job-1/")
	scope := memory.NewManager(1024).NewScope()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.RecoverIncompleteWrites(ctx, u, scope); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if d.calls != 0 {
		t.Fatalf("recovery driver called after cancellation: %d", d.calls)
	}
}

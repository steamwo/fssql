package asyncio

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type blockingDriver struct {
	current atomic.Int64
	peak    atomic.Int64
	start   chan struct{}
	release chan struct{}
}

func (d *blockingDriver) Scheme() string { return "fake" }
func (d *blockingDriver) StaticCapabilities() vfs.CapabilitySet {
	return vfs.Capabilities(vfs.RandomRead)
}
func (d *blockingDriver) Probe(context.Context) (vfs.ProbeResult, error) {
	return vfs.ProbeResult{Supported: d.StaticCapabilities(), Endpoint: "fake://endpoint", CredentialScope: "x", SecurityProfile: "test"}, nil
}
func (d *blockingDriver) Stat(context.Context, vfs.URI) (vfs.ObjectInfo, error) {
	return vfs.ObjectInfo{}, errors.New("unused")
}
func (d *blockingDriver) List(context.Context, vfs.URI, vfs.ListOptions) (vfs.ListPage, error) {
	return vfs.ListPage{}, errors.New("unused")
}
func (d *blockingDriver) ReadRange(ctx context.Context, r vfs.RangeRequest, scope *memory.Scope, bm *memory.BufferManager) (vfs.RangeResult, error) {
	cur := d.current.Add(1)
	for p := d.peak.Load(); cur > p && !d.peak.CompareAndSwap(p, cur); p = d.peak.Load() {
	}
	defer d.current.Add(-1)
	if d.start != nil {
		select {
		case d.start <- struct{}{}:
		default:
		}
	}
	select {
	case <-ctx.Done():
		return vfs.RangeResult{}, ctx.Err()
	case <-d.release:
	}
	buf, err := bm.Allocate(scope, r.Tag, int(r.Length))
	if err != nil {
		return vfs.RangeResult{}, err
	}
	return vfs.RangeResult{Buffer: buf}, nil
}

func requestFor(d vfs.Driver, scope *memory.Scope, bm *memory.BufferManager) Request {
	u, _ := vfs.ParseURI("fake://host/x")
	return Request{Driver: d, Range: vfs.RangeRequest{URI: u, Offset: 0, Length: 1, Tag: memory.TagNetworkBuffer}, Memory: scope, BufferManager: bm}
}

func TestManagerBoundsConcurrentIO(t *testing.T) {
	d := &blockingDriver{start: make(chan struct{}, 8), release: make(chan struct{}, 8)}
	m := NewManager(2, 8)
	defer m.Close()
	mm := memory.NewManager(100)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	fs := make([]Future, 4)
	for i := range fs {
		fs[i] = m.Submit(context.Background(), requestFor(d, scope, bm))
	}
	for i := 0; i < 2; i++ {
		select {
		case <-d.start:
		case <-time.After(time.Second):
			t.Fatal("request did not start")
		}
	}
	if d.peak.Load() > 2 {
		t.Fatalf("peak=%d", d.peak.Load())
	}
	for i := 0; i < 4; i++ {
		d.release <- struct{}{}
	}
	for _, f := range fs {
		res, err := f.Wait(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		res.Buffer.Release()
	}
	if m.Active() != 0 || scope.Used() != 0 {
		t.Fatalf("active=%d used=%d", m.Active(), scope.Used())
	}
}

func TestCancellationDrainsActiveRequest(t *testing.T) {
	d := &blockingDriver{start: make(chan struct{}, 1), release: make(chan struct{})}
	m := NewManager(1, 1)
	defer m.Close()
	mm := memory.NewManager(100)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	ctx, cancel := context.WithCancel(context.Background())
	f := m.Submit(ctx, requestFor(d, scope, bm))
	<-d.start
	cancel()
	_, err := f.Wait(context.Background())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if m.Active() != 0 || scope.Used() != 0 {
		t.Fatalf("active=%d used=%d", m.Active(), scope.Used())
	}
}

func TestCloseCancelsOwnedIO(t *testing.T) {
	d := &blockingDriver{start: make(chan struct{}, 1), release: make(chan struct{})}
	m := NewManager(1, 1)
	mm := memory.NewManager(100)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	f := m.Submit(context.Background(), requestFor(d, scope, bm))
	<-d.start
	m.Close()
	_, err := f.Wait(context.Background())
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v", err)
	}
	if m.Active() != 0 {
		t.Fatalf("active=%d", m.Active())
	}
}

func TestSubmitReadAheadTagsManagedBytes(t *testing.T) {
	d := &blockingDriver{release: make(chan struct{}, 1)}
	d.release <- struct{}{}
	m := NewManager(1, 1)
	defer m.Close()
	mm := memory.NewManager(100)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	f := m.SubmitReadAhead(context.Background(), requestFor(d, scope, bm))
	res, err := f.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if scope.UsedByTag(memory.TagReadAhead) != 1 {
		t.Fatalf("read-ahead bytes=%d", scope.UsedByTag(memory.TagReadAhead))
	}
	res.Buffer.Release()
}

func TestRangePlannerCoalescesAndPreservesParts(t *testing.T) {
	p := RangePlanner{MaxGap: 4, MaxMergedLength: 32}
	planned, err := p.Plan([]ByteRange{{Offset: 20, Length: 5}, {Offset: 0, Length: 4}, {Offset: 7, Length: 3}, {Offset: 22, Length: 8}})
	if err != nil {
		t.Fatal(err)
	}
	if len(planned) != 2 {
		t.Fatalf("planned=%+v", planned)
	}
	if planned[0].Offset != 0 || planned[0].Length != 10 || len(planned[0].Parts) != 2 {
		t.Fatalf("first=%+v", planned[0])
	}
	if planned[1].Offset != 20 || planned[1].Length != 10 || len(planned[1].Parts) != 2 {
		t.Fatalf("second=%+v", planned[1])
	}
	if planned[1].Parts[1].InputIndex != 3 || planned[1].Parts[1].Offset != 2 {
		t.Fatalf("parts=%+v", planned[1].Parts)
	}
}

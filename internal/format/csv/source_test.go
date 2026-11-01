package csv

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type phase3MemoryDriver struct {
	data []byte
	caps vfs.CapabilitySet
	uri  vfs.URI
}

func newPhase3MemoryDriver(t *testing.T, data []byte, caps vfs.CapabilitySet) *phase3MemoryDriver {
	t.Helper()
	u, err := vfs.ParseURI("mem://dataset/data.csv")
	if err != nil { t.Fatal(err) }
	return &phase3MemoryDriver{data: append([]byte(nil), data...), caps: caps, uri: u}
}
func (d *phase3MemoryDriver) Scheme() string { return "mem" }
func (d *phase3MemoryDriver) StaticCapabilities() vfs.CapabilitySet { return d.caps }
func (d *phase3MemoryDriver) Probe(context.Context) (vfs.ProbeResult, error) {
	return vfs.ProbeResult{Supported: d.caps, Endpoint: "mem://dataset", CredentialScope: "test", SecurityProfile: "test"}, nil
}
func (d *phase3MemoryDriver) Stat(context.Context, vfs.URI) (vfs.ObjectInfo, error) {
	id := vfs.ObjectIdentity{ETag: "fixture", Size: int64(len(d.data))}
	return vfs.ObjectInfo{URI: d.uri, Size: int64(len(d.data)), Identity: id}, nil
}
func (d *phase3MemoryDriver) List(context.Context, vfs.URI, vfs.ListOptions) (vfs.ListPage, error) {
	id := vfs.ObjectIdentity{ETag: "fixture", Size: int64(len(d.data))}
	return vfs.ListPage{Entries: []vfs.ListEntry{{URI: d.uri, Size: int64(len(d.data)), Identity: id}}}, nil
}
func (d *phase3MemoryDriver) ReadRange(ctx context.Context, r vfs.RangeRequest, scope *memory.Scope, bm *memory.BufferManager) (vfs.RangeResult, error) {
	if err := ctx.Err(); err != nil { return vfs.RangeResult{}, err }
	if err := r.Validate(); err != nil { return vfs.RangeResult{}, err }
	if r.Offset+r.Length > int64(len(d.data)) { return vfs.RangeResult{}, fmt.Errorf("test range out of bounds") }
	buf, err := bm.Allocate(scope, r.Tag, int(r.Length))
	if err != nil { return vfs.RangeResult{}, err }
	copy(buf.Bytes(), d.data[r.Offset:r.Offset+r.Length])
	return vfs.RangeResult{Buffer: buf, Identity: vfs.ObjectIdentity{ETag: "fixture", Size: int64(len(d.data))}}, nil
}

func TestSourceBindingAndInspectionShareInference(t *testing.T) {
	data := []byte("id,active,value,name\n1,true,1.5,alpha\n2,false,2,beta\n")
	driver := newPhase3MemoryDriver(t, data, vfs.Capabilities(vfs.RandomRead, vfs.ParallelRandomRead))
	registry, err := vfs.NewRegistry(driver)
	if err != nil { t.Fatal(err) }
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	binder, err := NewSourceBinder(registry, scope, bm)
	if err != nil { t.Fatal(err) }
	opts := SourceBindOptions{Inference: InferenceOptions{Header: true, NullLiteral: []byte("NULL"), MaxRows: 100}}
	binding, err := binder.Bind(context.Background(), driver.uri.String(), opts)
	if err != nil { t.Fatal(err) }
	report, err := binder.InspectSource(context.Background(), driver.uri.String(), opts)
	if err != nil { t.Fatal(err) }
	fields := binding.Schema().Fields
	if len(fields) != 4 || len(report.Fields()) != len(fields) { t.Fatalf("fields=%v report=%v", fields, report.Fields()) }
	for i, field := range fields {
		if report.Fields()[i].Name != field.Name || report.Fields()[i].Type.ID != field.Type.ID {
			t.Fatalf("binding/report mismatch at %d", i)
		}
	}
	if got := scope.Used(); got != 0 { t.Fatalf("managed bytes after bind/inspect=%d", got) }
}

func makeParallelFixture(rows int) []byte {
	var b strings.Builder
	b.WriteString("id,note\n")
	for i := 0; i < rows; i++ {
		if i%777 == 0 {
			fmt.Fprintf(&b, "%d,\"line %d\ncontinued\"\n", i, i)
		} else {
			fmt.Fprintf(&b, "%d,row-%d\n", i, i)
		}
	}
	return []byte(b.String())
}

func collectIDs(t *testing.T, src *Source, workers int) []int64 {
	t.Helper()
	ctx := context.Background()
	var mu sync.Mutex
	var ids []int64
	var firstErr error
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				chunk, ok, err := src.Next(ctx)
				if err != nil {
					mu.Lock(); if firstErr == nil { firstErr = err }; mu.Unlock(); return
				}
				if !ok { return }
				local := make([]int64, 0, chunk.Cardinality)
				for i := 0; i < chunk.Cardinality; i++ {
					v, valid := chunk.Columns[0].Int64(i)
					if !valid { mu.Lock(); if firstErr == nil { firstErr = fmt.Errorf("null id") }; mu.Unlock(); chunk.Release(); return }
					local = append(local, v)
				}
				chunk.Release()
				mu.Lock(); ids = append(ids, local...); mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if firstErr != nil { t.Fatal(firstErr) }
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func TestParallelCSVEqualsSequentialCSV(t *testing.T) {
	const rows = 5000
	data := makeParallelFixture(rows)
	driver := newPhase3MemoryDriver(t, data, vfs.Capabilities(vfs.RandomRead, vfs.ParallelRandomRead))
	registry, err := vfs.NewRegistry(driver)
	if err != nil { t.Fatal(err) }
	manager := memory.NewManager(32 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	binder, err := NewSourceBinder(registry, scope, bm)
	if err != nil { t.Fatal(err) }
	binding, err := binder.Bind(context.Background(), driver.uri.String(), SourceBindOptions{Inference: InferenceOptions{Header: true, MaxRows: 2048}})
	if err != nil { t.Fatal(err) }

	sequential, err := NewSource(context.Background(), registry, binding, ScanOptions{EnableParallel: false, TargetRangeBytes: 512}, scope, bm)
	if err != nil { t.Fatal(err) }
	seqIDs := collectIDs(t, sequential, 1)
	parallel, err := NewSource(context.Background(), registry, binding, ScanOptions{EnableParallel: true, TargetRangeBytes: 512}, scope, bm)
	if err != nil { t.Fatal(err) }
	parIDs := collectIDs(t, parallel, 4)
	if len(seqIDs) != rows || len(parIDs) != rows { t.Fatalf("rows sequential=%d parallel=%d", len(seqIDs), len(parIDs)) }
	for i := 0; i < rows; i++ {
		if seqIDs[i] != int64(i) || parIDs[i] != int64(i) { t.Fatalf("row %d seq=%d par=%d", i, seqIDs[i], parIDs[i]) }
	}
	if got := scope.Used(); got != 0 { t.Fatalf("managed bytes after scans=%d", got) }
}

func TestParallelRequestFallsBackToSequentialWithoutParallelRandomRead(t *testing.T) {
	data := makeParallelFixture(300)
	driver := newPhase3MemoryDriver(t, data, vfs.Capabilities(vfs.RandomRead))
	registry, _ := vfs.NewRegistry(driver)
	manager := memory.NewManager(4 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	binder, _ := NewSourceBinder(registry, scope, bm)
	binding, err := binder.Bind(context.Background(), driver.uri.String(), SourceBindOptions{Inference: InferenceOptions{Header: true}})
	if err != nil { t.Fatal(err) }
	src, err := NewSource(context.Background(), registry, binding, ScanOptions{EnableParallel: true, TargetRangeBytes: 64}, scope, bm)
	if err != nil { t.Fatal(err) }
	ids := collectIDs(t, src, 4)
	if len(ids) != 300 { t.Fatalf("rows=%d", len(ids)) }
	if got := scope.Used(); got != 0 { t.Fatalf("managed bytes=%d", got) }
}

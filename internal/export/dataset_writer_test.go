package export

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type datasetFakeOpen struct {
	mu        sync.Mutex
	opened    []string
	active    int
	maxActive int
	writes    map[string][]int32
	failWrite bool
	aborts    int
}

func (o *datasetFakeOpen) open(_ context.Context, uri vfs.URI, _ vfs.WriteOptions) (FormatWriter, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.opened = append(o.opened, uri.Path)
	o.active++
	if o.active > o.maxActive {
		o.maxActive = o.active
	}
	if o.writes == nil {
		o.writes = make(map[string][]int32)
	}
	return &datasetFakeWriter{owner: o, path: uri.Path, failWrite: o.failWrite}, nil
}

type datasetFakeWriter struct {
	owner     *datasetFakeOpen
	path      string
	failWrite bool
	terminal  bool
}

func (w *datasetFakeWriter) WriteChunk(_ context.Context, chunk *vector.DataChunk) error {
	if w.failWrite {
		return errors.New("write failed")
	}
	if len(chunk.Columns) > 1 {
		for row := 0; row < chunk.Cardinality; row++ {
			id, _ := chunk.Columns[1].Int32(row)
			w.owner.writes[w.path] = append(w.owner.writes[w.path], id)
		}
	}
	return nil
}
func (*datasetFakeWriter) Flush(context.Context) error { return nil }
func (w *datasetFakeWriter) Close(context.Context) (WriteResult, error) {
	if !w.terminal {
		w.owner.active--
		w.terminal = true
	}
	return WriteResult{Bytes: 11, Checksum: "sum"}, nil
}
func (w *datasetFakeWriter) Abort(context.Context) error {
	if !w.terminal {
		w.owner.active--
		w.owner.aborts++
		w.terminal = true
	}
	return nil
}

func newWritingManifest(t *testing.T) *Manifest {
	t.Helper()
	m, err := NewManifest("job", "query", "source", "s3://bucket/staging/job/", "parquet", "identity", "snapshot", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.TransitionExport(ExportStateWriting); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestDatasetWriterPartitionedClosesEachRouteAndUpdatesManifest(t *testing.T) {
	schema := []Column{{Name: "region", Type: types.LogicalType{ID: types.Varchar}}, {Name: "id", Type: types.LogicalType{ID: types.Integer}}}
	chunk, scope := newRoutingTestChunk(t, schema, []any{"b", "a", "b"}, []int32{1, 2, 3})
	defer func() { chunk.Release(); _ = scope.Close() }()
	router, err := NewFileRouter(RoutingConfig{Mode: LayoutPartitioned, Extension: "parquet", PartitionColumns: []int{0}}, schema)
	if err != nil {
		t.Fatal(err)
	}
	fake := &datasetFakeOpen{}
	writer, err := newDatasetWriter(newWritingManifest(t), router, fake.open)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(context.Background(), chunk, "seg1"); err != nil {
		t.Fatal(err)
	}
	if fake.maxActive != 1 || fake.active != 0 {
		t.Fatalf("active=%d max=%d", fake.active, fake.maxActive)
	}
	if got := fake.writes["/staging/job/region=b/part-seg1.parquet"]; len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("b rows=%v", got)
	}
	if len(writer.manifest.FileStates) != 2 {
		t.Fatalf("files=%+v", writer.manifest.FileStates)
	}
	for _, entry := range writer.manifest.FileStates {
		if entry.State != FileStateComplete || entry.Size != 11 || entry.Checksum != "sum" {
			t.Fatalf("entry=%+v", entry)
		}
	}
}

func TestDatasetWriterSkipsCompletedStableRoute(t *testing.T) {
	schema := []Column{{Name: "id", Type: types.LogicalType{ID: types.Integer}}}
	chunk, scope := newRoutingTestChunk(t, schema, nil, []int32{1})
	defer func() { chunk.Release(); _ = scope.Close() }()
	router, _ := NewFileRouter(RoutingConfig{Mode: LayoutMultiFile, Extension: "csv"}, schema)
	m := newWritingManifest(t)
	if err := m.PlanFile("part-seg1.csv"); err != nil {
		t.Fatal(err)
	}
	if err := m.TransitionFile("part-seg1.csv", FileStateWriting, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := m.TransitionFile("part-seg1.csv", FileStateComplete, 4, "done"); err != nil {
		t.Fatal(err)
	}
	fake := &datasetFakeOpen{}
	writer, err := newDatasetWriter(m, router, fake.open)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(context.Background(), chunk, "seg1"); err != nil {
		t.Fatal(err)
	}
	if len(fake.opened) != 0 {
		t.Fatalf("completed route reopened: %v", fake.opened)
	}
}

func TestDatasetWriterFailureMarksIncompleteFileFailed(t *testing.T) {
	schema := []Column{{Name: "region", Type: types.LogicalType{ID: types.Varchar}}, {Name: "id", Type: types.LogicalType{ID: types.Integer}}}
	chunk, scope := newRoutingTestChunk(t, schema, []any{"a"}, []int32{1})
	defer func() { chunk.Release(); _ = scope.Close() }()
	router, _ := NewFileRouter(RoutingConfig{Mode: LayoutPartitioned, Extension: "json", PartitionColumns: []int{0}}, schema)
	fake := &datasetFakeOpen{failWrite: true}
	writer, err := newDatasetWriter(newWritingManifest(t), router, fake.open)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(context.Background(), chunk, "seg1"); err == nil {
		t.Fatal("expected write failure")
	}
	if fake.aborts != 1 || fake.active != 0 {
		t.Fatalf("aborts=%d active=%d", fake.aborts, fake.active)
	}
	entry, ok := writer.manifest.LookupFile("region=a/part-seg1.json")
	if !ok || entry.State != FileStateFailed {
		t.Fatalf("entry=%+v ok=%v", entry, ok)
	}
}

func TestDatasetWriterSingleFileReusesOneWriterUntilClose(t *testing.T) {
	schema := []Column{{Name: "id", Type: types.LogicalType{ID: types.Integer}}}
	first, firstScope := newRoutingTestChunk(t, schema, nil, []int32{1})
	second, secondScope := newRoutingTestChunk(t, schema, nil, []int32{2})
	defer func() {
		first.Release()
		second.Release()
		_ = firstScope.Close()
		_ = secondScope.Close()
	}()
	router, _ := NewFileRouter(RoutingConfig{Mode: LayoutSingleFile, SingleFilePath: "result.csv"}, schema)
	fake := &datasetFakeOpen{}
	writer, err := newDatasetWriter(newWritingManifest(t), router, fake.open)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(context.Background(), first, "ignored"); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(context.Background(), second, "ignored"); err != nil {
		t.Fatal(err)
	}
	if len(fake.opened) != 1 || fake.active != 1 {
		t.Fatalf("opened=%v active=%d", fake.opened, fake.active)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	entry, ok := writer.manifest.LookupFile("result.csv")
	if !ok || entry.State != FileStateComplete || fake.active != 0 {
		t.Fatalf("entry=%+v ok=%v active=%d", entry, ok, fake.active)
	}
}

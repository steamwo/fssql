package parquet

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/storage/fileset"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// runtimeFakeScanner 模拟一个 row-group scanner：最多输出一个 managed chunk，然后 EOF。
type runtimeFakeScanner struct {
	scope   *memory.Scope
	buffers *memory.BufferManager
	value   int64
	err     error
	emitted bool
	closed  *int
}

func (s *runtimeFakeScanner) Next(context.Context) (*vector.DataChunk, error) {
	if s.err != nil {
		err := s.err
		s.err = nil
		return nil, err
	}
	if s.emitted {
		return nil, io.EOF
	}
	s.emitted = true
	column, err := vector.NewFlat(s.scope, s.buffers, types.LogicalType{ID: types.BigInt}, 1)
	if err != nil {
		return nil, err
	}
	column.SetInt64(0, s.value)
	chunk, err := vector.NewDataChunk(column)
	if err != nil {
		column.Release()
		return nil, err
	}
	return chunk, nil
}

func (s *runtimeFakeScanner) Close() {
	if s.closed != nil {
		(*s.closed)++
	}
}

func runtimeTestFile(t *testing.T, name string) fileset.FileSnapshot {
	t.Helper()
	u, err := vfs.ParseURI("mem://bucket/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return fileset.FileSnapshot{URI: u, Size: 1, Identity: vfs.ObjectIdentity{ETag: name, Size: 1}}
}

// TestRuntimeSourceAdvancesFilesAndRowGroups 验证多文件/多 row-group 顺序、EOF close 与 managed chunk ownership。
func TestRuntimeSourceAdvancesFilesAndRowGroups(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	closed := 0
	files := []fileset.FileSnapshot{runtimeTestFile(t, "a.parquet"), runtimeTestFile(t, "b.parquet")}
	prepareCalls := 0
	prepare := func(_ context.Context, file fileset.FileSnapshot) (runtimePreparedFile, error) {
		prepareCalls++
		groups := []RowGroup{{NumRows: 1}, {NumRows: 2}}
		if file.URI.ObjectKey() == "b.parquet" {
			groups = []RowGroup{{NumRows: 3}}
		}
		return runtimePreparedFile{
			rowGroups: groups,
			newScanner: func(rg RowGroup) (runtimeRowGroupScanner, error) {
				return &runtimeFakeScanner{scope: scope, buffers: buffers, value: rg.NumRows, closed: &closed}, nil
			},
		}, nil
	}
	source, err := newRuntimeSource(files, prepare)
	if err != nil {
		t.Fatal(err)
	}
	var got []int64
	for {
		chunk, ok, err := source.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		value, valid := chunk.Columns[0].Int64(0)
		chunk.Release()
		if !valid {
			t.Fatal("unexpected NULL")
		}
		got = append(got, value)
	}
	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("values=%v", got)
	}
	if prepareCalls != 2 || closed != 3 {
		t.Fatalf("prepare=%d closed=%d", prepareCalls, closed)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestRuntimeSourceCloseClosesCurrentScanner 验证 downstream 提前终止时 CloseableSource 可以关闭当前 row-group reader。
func TestRuntimeSourceCloseClosesCurrentScanner(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	closed := 0
	prepare := func(context.Context, fileset.FileSnapshot) (runtimePreparedFile, error) {
		return runtimePreparedFile{
			rowGroups: []RowGroup{{NumRows: 9}},
			newScanner: func(RowGroup) (runtimeRowGroupScanner, error) {
				return &runtimeFakeScanner{scope: scope, buffers: buffers, value: 9, closed: &closed}, nil
			},
		}, nil
	}
	source, err := newRuntimeSource([]fileset.FileSnapshot{runtimeTestFile(t, "one.parquet")}, prepare)
	if err != nil {
		t.Fatal(err)
	}
	chunk, ok, err := source.Next(context.Background())
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	chunk.Release()
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if closed != 1 {
		t.Fatalf("scanner closed=%d", closed)
	}
	if _, _, err := source.Next(context.Background()); err == nil {
		t.Fatal("expected read-after-close failure")
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestRuntimeSourceScannerErrorIsTerminal 验证 scanner 错误会关闭 reader 并固化，后续 Next 不会重试同一 row-group。
func TestRuntimeSourceScannerErrorIsTerminal(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	closed := 0
	created := 0
	want := errors.New("scan failed")
	prepare := func(context.Context, fileset.FileSnapshot) (runtimePreparedFile, error) {
		return runtimePreparedFile{
			rowGroups: []RowGroup{{NumRows: 1}},
			newScanner: func(RowGroup) (runtimeRowGroupScanner, error) {
				created++
				return &runtimeFakeScanner{scope: scope, buffers: memory.NewBufferManager(nil), err: want, closed: &closed}, nil
			},
		}, nil
	}
	source, err := newRuntimeSource([]fileset.FileSnapshot{runtimeTestFile(t, "bad.parquet")}, prepare)
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if _, _, err := source.Next(context.Background()); !errors.Is(err, want) {
			t.Fatalf("attempt=%d err=%v", attempt, err)
		}
	}
	if created != 1 || closed != 1 {
		t.Fatalf("created=%d closed=%d", created, closed)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestRuntimeSourcePrepareErrorIsTerminal 验证 footer/schema gate 失败后不会重复远端 metadata I/O。
func TestRuntimeSourcePrepareErrorIsTerminal(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	calls := 0
	want := errors.New("schema drift")
	prepare := func(context.Context, fileset.FileSnapshot) (runtimePreparedFile, error) {
		calls++
		return runtimePreparedFile{}, want
	}
	source, err := newRuntimeSource([]fileset.FileSnapshot{runtimeTestFile(t, "drift.parquet")}, prepare)
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if _, _, err := source.Next(context.Background()); !errors.Is(err, want) {
			t.Fatalf("attempt=%d err=%v", attempt, err)
		}
	}
	if calls != 1 {
		t.Fatalf("prepare calls=%d", calls)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

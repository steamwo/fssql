package export

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

// captureSink 是测试专用内存 sink；生产 writer 不依赖普通 Go heap 累积整个文件。
type captureSink struct {
	buf     bytes.Buffer
	short   bool
	aborted bool
	closed  bool
}

func (s *captureSink) Write(_ context.Context, p []byte) (int, error) {
	if s.short && len(p) > 0 {
		n := len(p) - 1
		_, _ = s.buf.Write(p[:n])
		return n, nil
	}
	return s.buf.Write(p)
}
func (*captureSink) Flush(context.Context) error { return nil }
func (s *captureSink) Close(context.Context) (WriteResult, error) {
	s.closed = true
	return WriteResult{Bytes: int64(s.buf.Len())}, nil
}
func (s *captureSink) Abort(context.Context) error { s.aborted = true; return nil }

func newVector(t testing.TB, scope *memory.Scope, bm *memory.BufferManager, typ types.LogicalType, n int) *vector.Vector {
	t.Helper()
	v, err := vector.NewFlat(scope, bm, typ, n)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestCSVWriterEscapingNullAndHeader(t *testing.T) {
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	sink := &captureSink{}
	id := newVector(t, scope, bm, types.LogicalType{ID: types.Integer}, 2)
	id.SetInt32(0, 1)
	id.SetNull(1)
	name := newVector(t, scope, bm, types.LogicalType{ID: types.Varchar}, 2)
	_ = name.SetString(0, "a,b")
	_ = name.SetString(1, "")
	chunk, err := vector.NewDataChunk(id, name)
	if err != nil {
		t.Fatal(err)
	}
	schema := []Column{{Name: "id", Type: id.Type}, {Name: "name", Type: name.Type}}
	w, err := NewCSVWriter(schema, CSVWriteOptions{Header: true, BufferSize: 8}, scope, bm, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteChunk(context.Background(), chunk); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := "id,name\n1,\"a,b\"\n,\"\"\n"
	if got := sink.buf.String(); got != want {
		t.Fatalf("CSV mismatch\ngot:  %q\nwant: %q", got, want)
	}
	chunk.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed memory leak: %d", scope.Used())
	}
}

func TestJSONWriterTypedNestedValues(t *testing.T) {
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	sink := &captureSink{}
	intType := types.LogicalType{ID: types.Integer}
	listType := types.NewList(intType)
	items := newVector(t, scope, bm, listType, 1)
	child := newVector(t, scope, bm, intType, 2)
	child.SetInt32(0, 7)
	child.SetNull(1)
	items.SetChildren(child)
	items.SetListEntry(0, vector.ListEntry{Offset: 0, Length: 2})
	structType := types.NewStruct(types.StructField{Name: "flag", Type: types.LogicalType{ID: types.Boolean}}, types.StructField{Name: "text", Type: types.LogicalType{ID: types.Varchar}})
	meta := newVector(t, scope, bm, structType, 1)
	flag := newVector(t, scope, bm, structType.Fields[0].Type, 1)
	flag.SetBool(0, true)
	text := newVector(t, scope, bm, structType.Fields[1].Type, 1)
	_ = text.SetString(0, "x\ny")
	meta.SetChildren(flag, text)
	chunk, err := vector.NewDataChunk(items, meta)
	if err != nil {
		t.Fatal(err)
	}
	schema := []Column{{Name: "items", Type: listType}, {Name: "meta", Type: structType}}
	w, err := NewJSONWriter(schema, JSONWriteOptions{Mode: JSONModeArray, BufferSize: 11}, scope, bm, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteChunk(context.Background(), chunk); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := "[{\"items\":[7,null],\"meta\":{\"flag\":true,\"text\":\"x\\ny\"}}]"
	if got := sink.buf.String(); got != want {
		t.Fatalf("JSON mismatch\ngot:  %s\nwant: %s", got, want)
	}
	chunk.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed memory leak: %d", scope.Used())
	}
}

func TestNDJSONWriterFramingAndNull(t *testing.T) {
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	sink := &captureSink{}
	v := newVector(t, scope, bm, types.LogicalType{ID: types.BigInt}, 2)
	v.SetInt64(0, 42)
	v.SetNull(1)
	chunk, _ := vector.NewDataChunk(v)
	schema := []Column{{Name: "n", Type: v.Type}}
	w, err := NewJSONWriter(schema, JSONWriteOptions{Mode: JSONModeNDJSON, BufferSize: 7}, scope, bm, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteChunk(context.Background(), chunk); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, want := sink.buf.String(), "{\"n\":42}\n{\"n\":null}\n"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	chunk.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed memory leak: %d", scope.Used())
	}
}

func TestFormatWriterAllocationAndShortWriteFailureReleaseMemory(t *testing.T) {
	bm := memory.NewBufferManager(nil)
	mmSmall := memory.NewManager(4)
	smallScope := mmSmall.NewScope()
	_, err := NewCSVWriter([]Column{{Name: "x", Type: types.LogicalType{ID: types.Varchar}}}, CSVWriteOptions{BufferSize: 8}, smallScope, bm, &captureSink{})
	if !errors.Is(err, memory.ErrLimitExceeded) {
		t.Fatalf("expected memory limit, got %v", err)
	}
	if smallScope.Used() != 0 {
		t.Fatalf("allocation failure leaked %d bytes", smallScope.Used())
	}

	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	sink := &captureSink{short: true}
	v := newVector(t, scope, bm, types.LogicalType{ID: types.Varchar}, 1)
	_ = v.SetString(0, "this-is-long-enough-to-flush")
	chunk, _ := vector.NewDataChunk(v)
	w, err := NewCSVWriter([]Column{{Name: "x", Type: v.Type}}, CSVWriteOptions{BufferSize: 8}, scope, bm, sink)
	if err != nil {
		t.Fatal(err)
	}
	err = w.WriteChunk(context.Background(), chunk)
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("expected short write, got %v", err)
	}
	if err := w.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !sink.aborted {
		t.Fatal("sink was not aborted")
	}
	chunk.Release()
	if scope.Used() != 0 {
		t.Fatalf("failure leaked managed memory: %d", scope.Used())
	}
}

func TestFormatWriterRejectsSchemaDrift(t *testing.T) {
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	sink := &captureSink{}
	v := newVector(t, scope, bm, types.LogicalType{ID: types.Integer}, 1)
	v.SetInt32(0, 1)
	chunk, _ := vector.NewDataChunk(v)
	w, err := NewCSVWriter([]Column{{Name: "x", Type: types.LogicalType{ID: types.BigInt}}}, CSVWriteOptions{BufferSize: 8}, scope, bm, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteChunk(context.Background(), chunk); err == nil {
		t.Fatal("expected schema mismatch")
	}
	_ = w.Abort(context.Background())
	chunk.Release()
	if scope.Used() != 0 {
		t.Fatalf("leak=%d", scope.Used())
	}
}

func BenchmarkCSVWriter1024(b *testing.B) {
	for i := 0; i < b.N; i++ {
		mm := memory.NewManager(8 << 20)
		scope := mm.NewScope()
		bm := memory.NewBufferManager(nil)
		sink := &captureSink{}
		id := newVector(b, scope, bm, types.LogicalType{ID: types.Integer}, 1024)
		name := newVector(b, scope, bm, types.LogicalType{ID: types.Varchar}, 1024)
		for row := 0; row < 1024; row++ {
			id.SetInt32(row, int32(row))
			_ = name.SetString(row, "value")
		}
		chunk, _ := vector.NewDataChunk(id, name)
		w, _ := NewCSVWriter([]Column{{Name: "id", Type: id.Type}, {Name: "name", Type: name.Type}}, CSVWriteOptions{BufferSize: 64 << 10}, scope, bm, sink)
		_ = w.WriteChunk(context.Background(), chunk)
		_, _ = w.Close(context.Background())
		chunk.Release()
	}
}

func TestInt128AndDecimalTextFormatting(t *testing.T) {
	cases := []struct {
		name  string
		value types.Int128
		want  string
	}{
		{name: "minus one", value: types.Int128{Lo: ^uint64(0), Hi: -1}, want: "-1"},
		{name: "max", value: types.Int128{Lo: ^uint64(0), Hi: int64(^uint64(0) >> 1)}, want: "170141183460469231731687303715884105727"},
		{name: "min", value: types.Int128{Lo: 0, Hi: -1 << 63}, want: "-170141183460469231731687303715884105728"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var dst [64]byte
			got := string(appendInt128(dst[:0], tc.value))
			if got != tc.want {
				t.Fatalf("got %s want %s", got, tc.want)
			}
		})
	}
	var dst [64]byte
	if got := string(appendScaledInt64(dst[:0], -123, 2)); got != "-1.23" {
		t.Fatalf("decimal got %s", got)
	}
	if got := string(appendScaledInt64(dst[:0], 5, 2)); got != "0.05" {
		t.Fatalf("decimal got %s", got)
	}
}

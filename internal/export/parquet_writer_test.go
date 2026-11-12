package export

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/format/parquet"
)

// TestParquetWriterRowGroupAndPageBoundariesAreIndependent 验证 row-group/page 边界不跟随输入 DataChunk 边界。
func TestParquetWriterRowGroupAndPageBoundariesAreIndependent(t *testing.T) {
	mm := memory.NewManager(8 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	sink := &captureSink{}
	schema := []Column{{Name: "id", Type: types.LogicalType{ID: types.Integer}}, {Name: "name", Type: types.LogicalType{ID: types.Varchar}}}
	w, err := NewParquetWriter(schema, ParquetWriteOptions{RowGroupRows: 3, PageRows: 2, PageBufferSize: 32}, scope, bm, sink)
	if err != nil {
		t.Fatal(err)
	}

	write := func(ids []int32, names []string) {
		id := newVector(t, scope, bm, schema[0].Type, len(ids))
		name := newVector(t, scope, bm, schema[1].Type, len(ids))
		for i := range ids {
			id.SetInt32(i, ids[i])
			if names[i] == "<NULL>" {
				name.SetNull(i)
			} else if err := name.SetString(i, names[i]); err != nil {
				t.Fatal(err)
			}
		}
		chunk, err := vector.NewDataChunk(id, name)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.WriteChunk(context.Background(), chunk); err != nil {
			t.Fatal(err)
		}
		chunk.Release()
	}
	write([]int32{1, 2}, []string{"a", "b"})
	write([]int32{3, 4}, []string{"<NULL>", "d"})
	if len(w.rowGroups) != 1 || w.rowGroups[0].NumRows != 3 || w.rowCount != 1 {
		t.Fatalf("row-group boundary followed input chunks: groups=%d firstRows=%d buffered=%d", len(w.rowGroups), w.rowGroups[0].NumRows, w.rowCount)
	}
	if _, err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	data := sink.buf.Bytes()
	if len(data) < 12 || !bytes.Equal(data[:4], parquetMagic) || !bytes.Equal(data[len(data)-4:], parquetMagic) {
		t.Fatalf("invalid parquet magic/layout: %x", data)
	}
	footerLen := int(binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4]))
	if footerLen <= 0 || footerLen > len(data)-12 {
		t.Fatalf("invalid footer length %d file=%d", footerLen, len(data))
	}
	metadata, err := parquet.DecodeFileMetaData(data[len(data)-8-footerLen : len(data)-8])
	if err != nil {
		t.Fatal(err)
	}
	if len(metadata.RowGroups) != 2 {
		t.Fatalf("row groups=%d, want 2", len(metadata.RowGroups))
	}
	for i, group := range metadata.RowGroups {
		for _, column := range group.Columns {
			if column.MetaData == nil || column.MetaData.Codec != parquet.CodecSnappy {
				t.Fatalf("row group %d: default compression is not Snappy", i)
			}
		}
		if group.Ordinal != nil {
			t.Fatalf("unencrypted row group %d ordinal=%d, want omitted", i, *group.Ordinal)
		}
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leak: %d", scope.Used())
	}
}

// TestParquetRowGroupOrdinalBoundary 验证未加密路径不受 int16 限制且加密 AAD 保留明确边界。
func TestParquetRowGroupOrdinalBoundary(t *testing.T) {
	for _, count := range []int{0, math.MaxInt16, math.MaxInt16 + 1} {
		ordinal, err := parquetRowGroupOrdinal(false, count)
		if err != nil || ordinal != nil {
			t.Fatalf("unencrypted count=%d ordinal=%v err=%v, want omitted", count, ordinal, err)
		}
	}
	for _, tc := range []struct {
		count   int
		want    int16
		wantErr bool
	}{
		{count: 0, want: 0},
		{count: math.MaxInt16, want: math.MaxInt16},
		{count: math.MaxInt16 + 1, wantErr: true},
	} {
		ordinal, err := parquetRowGroupOrdinal(true, tc.count)
		if (err != nil) != tc.wantErr {
			t.Fatalf("encrypted count=%d err=%v, wantErr=%v", tc.count, err, tc.wantErr)
		}
		if tc.wantErr {
			continue
		}
		if ordinal == nil || *ordinal != tc.want {
			t.Fatalf("encrypted count=%d ordinal=%v, want %d", tc.count, ordinal, tc.want)
		}
	}
}

// TestParquetWriterRejectsUndefinedLogicalIdentity 确保未冻结的 HUGEINT Parquet 映射不会被静默伪装为 DECIMAL。
func TestParquetWriterRejectsUndefinedLogicalIdentity(t *testing.T) {
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	_, err := NewParquetWriter([]Column{{Name: "h", Type: types.LogicalType{ID: types.HugeInt}}}, ParquetWriteOptions{}, scope, bm, &captureSink{})
	if err == nil {
		t.Fatal("expected HUGEINT mapping rejection")
	}
	if scope.Used() != 0 {
		t.Fatalf("constructor leak=%d", scope.Used())
	}
}

// TestParquetWriterAllocationAndShortWriteCleanup 验证 reserve 失败和目标 short write 都不会遗留 managed ownership。
func TestParquetWriterAllocationAndShortWriteCleanup(t *testing.T) {
	bm := memory.NewBufferManager(nil)
	mmSmall := memory.NewManager(32)
	small := mmSmall.NewScope()
	limited, err := NewParquetWriter([]Column{{Name: "s", Type: types.LogicalType{ID: types.Varchar}}}, ParquetWriteOptions{RowGroupRows: 1, PageRows: 1, PageBufferSize: 64}, small, bm, &captureSink{})
	if err == nil {
		// 实现可以在 row-group allocation 或 page allocation 任一 reserve 点拒绝；两者都必须正确回滚。
		srcMM := memory.NewManager(1 << 20)
		srcScope := srcMM.NewScope()
		src := newVector(t, srcScope, bm, types.LogicalType{ID: types.Varchar}, 1)
		_ = src.SetString(0, "value")
		chunk, _ := vector.NewDataChunk(src)
		err = limited.WriteChunk(context.Background(), chunk)
		chunk.Release()
		if closeErr := srcScope.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		_ = limited.Abort(context.Background())
	}
	if !errors.Is(err, memory.ErrLimitExceeded) {
		t.Fatalf("expected memory limit, got %v", err)
	}
	if small.Used() != 0 {
		t.Fatalf("allocation failure leak=%d", small.Used())
	}

	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	sink := &captureSink{short: true}
	w, err := NewParquetWriter([]Column{{Name: "id", Type: types.LogicalType{ID: types.Integer}}}, ParquetWriteOptions{RowGroupRows: 2, PageRows: 1}, scope, bm, sink)
	if err != nil {
		t.Fatal(err)
	}
	v := newVector(t, scope, bm, types.LogicalType{ID: types.Integer}, 1)
	v.SetInt32(0, 1)
	chunk, _ := vector.NewDataChunk(v)
	if err := w.WriteChunk(context.Background(), chunk); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("expected short write, got %v", err)
	}
	if err := w.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	chunk.Release()
	if !sink.aborted {
		t.Fatal("short write did not abort target")
	}
	if scope.Used() != 0 {
		t.Fatalf("short write leak=%d", scope.Used())
	}
}

// BenchmarkParquetWriterInteger1024 记录基础 INTEGER PLAIN writer 的性能基线。
func BenchmarkParquetWriterInteger1024(b *testing.B) {
	for i := 0; i < b.N; i++ {
		mm := memory.NewManager(16 << 20)
		scope := mm.NewScope()
		bm := memory.NewBufferManager(nil)
		sink := &captureSink{}
		v := newVector(b, scope, bm, types.LogicalType{ID: types.Integer}, 1024)
		for row := 0; row < 1024; row++ {
			v.SetInt32(row, int32(row))
		}
		chunk, _ := vector.NewDataChunk(v)
		w, _ := NewParquetWriter([]Column{{Name: "id", Type: v.Type}}, ParquetWriteOptions{RowGroupRows: 1024, PageRows: 256}, scope, bm, sink)
		_ = w.WriteChunk(context.Background(), chunk)
		_, _ = w.Close(context.Background())
		chunk.Release()
	}
}

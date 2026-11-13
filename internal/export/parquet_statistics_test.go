package export

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/format/parquet"
)

func TestParquetStatisticsTypedBounds(t *testing.T) {
	decimal, _ := types.NewDecimal(38, 4)
	decimal32, _ := types.NewDecimal(9, 2)
	decimal64, _ := types.NewDecimal(18, 2)
	for _, tc := range []struct {
		name     string
		typ      types.LogicalType
		fill     func(*vector.Vector)
		min, max string
	}{
		{"boolean", types.LogicalType{ID: types.Boolean}, func(v *vector.Vector) { v.SetBool(0, true); v.SetBool(1, false); v.SetBool(3, true) }, "00", "01"},
		{"small signed", types.LogicalType{ID: types.SmallInt}, func(v *vector.Vector) { v.SetInt16(0, 32767); v.SetInt16(1, -32768); v.SetInt16(3, 0) }, "0080ffff", "ff7f0000"},
		{"integer unsigned", types.LogicalType{ID: types.Integer, Unsigned: true}, func(v *vector.Vector) { v.SetUint32(0, math.MaxUint32); v.SetUint32(1, 0); v.SetUint32(3, 1<<31) }, "00000000", "ffffffff"},
		{"decimal32", decimal32, func(v *vector.Vector) { v.SetInt64(0, 99); v.SetInt64(1, -2); v.SetInt64(3, 0) }, "feffffff", "63000000"},
		{"decimal64", decimal64, func(v *vector.Vector) { v.SetInt64(0, 99); v.SetInt64(1, -2); v.SetInt64(3, 0) }, "feffffffffffffff", "6300000000000000"},
		{"tiny signed", types.LogicalType{ID: types.TinyInt}, func(v *vector.Vector) { v.SetInt8(0, 127); v.SetInt8(1, -128); v.SetInt8(3, 0) }, "80ffffff", "7f000000"},
		{"unsigned", types.LogicalType{ID: types.BigInt, Unsigned: true}, func(v *vector.Vector) { v.SetUint64(0, math.MaxUint64); v.SetUint64(1, 1); v.SetUint64(3, 1<<63) }, "0100000000000000", "ffffffffffffffff"},
		{"decimal128", decimal, func(v *vector.Vector) {
			v.SetInt128(0, types.Int128{Hi: 1})
			v.SetInt128(1, types.Int128FromInt64(-2))
			v.SetInt128(3, types.Int128{})
		}, "fffffffffffffffffffffffffffffffe", "00000000000000010000000000000000"},
		{"empty and binary", types.LogicalType{ID: types.Varchar, Binary: true}, func(v *vector.Vector) { _ = v.SetString(0, ""); _ = v.SetString(1, "\x80"); _ = v.SetString(3, "\x7f") }, "", "80"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scope := memory.NewManager(1 << 20).NewScope()
			v := newVector(t, scope, memory.NewBufferManager(nil), tc.typ, 4)
			tc.fill(v)
			v.SetNull(2)
			stats := collectParquetStatistics(v, 0, 4)
			min, _ := hex.DecodeString(tc.min)
			max, _ := hex.DecodeString(tc.max)
			if stats.MinValue == nil || stats.MaxValue == nil || !bytes.Equal(stats.MinValue, min) || !bytes.Equal(stats.MaxValue, max) || *stats.NullCount != 1 {
				t.Fatalf("statistics=%+v bounds=%x/%x", stats, stats.MinValue, stats.MaxValue)
			}
			// One row per page exercises merging PLAIN-encoded DECIMAL32/128
			// bounds, narrow integers and an intervening all-NULL page.
			sink := &captureSink{}
			writer, err := NewParquetWriter([]Column{{Name: "v", Type: tc.typ}}, ParquetWriteOptions{RowGroupRows: 4, PageRows: 1, PageBufferSize: 32}, scope, memory.NewBufferManager(nil), sink)
			if err != nil {
				t.Fatal(err)
			}
			chunk, _ := vector.NewDataChunk(v)
			if err := writer.WriteChunk(context.Background(), chunk); err != nil {
				t.Fatal(err)
			}
			if _, err := writer.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			data := sink.buf.Bytes()
			length := int(binary.LittleEndian.Uint32(data[len(data)-8:]))
			meta, err := parquet.DecodeFileMetaData(data[len(data)-8-length : len(data)-8])
			if err != nil {
				t.Fatal(err)
			}
			merged := meta.RowGroups[0].Columns[0].MetaData.Statistics
			if merged.MinValue == nil || !bytes.Equal(merged.MinValue, min) || !bytes.Equal(merged.MaxValue, max) || *merged.NullCount != 1 {
				t.Fatalf("merged bounds=%x/%x nulls=%d", merged.MinValue, merged.MaxValue, *merged.NullCount)
			}
			chunk.Release()
			if scope.Used() != 0 {
				t.Fatalf("managed leak=%d", scope.Used())
			}
		})
	}
}

func TestParquetStatisticsFloatingAndOmittedBounds(t *testing.T) {
	for _, id := range []types.LogicalTypeID{types.Float, types.Double} {
		for _, values := range [][]float64{{0, math.Copysign(0, -1)}, {math.Inf(-1), math.Inf(1)}, {math.NaN(), 1}, {math.NaN(), math.NaN()}} {
			scope := memory.NewManager(1 << 20).NewScope()
			v := newVector(t, scope, memory.NewBufferManager(nil), types.LogicalType{ID: id}, 3)
			for i, value := range values {
				if id == types.Float {
					v.SetFloat32(i, float32(value))
				} else {
					v.SetFloat64(i, value)
				}
			}
			v.SetNull(2)
			stats := collectParquetStatistics(v, 0, 3)
			if *stats.NullCount != 1 || stats.NaNCount == nil {
				t.Fatalf("statistics=%+v", stats)
			}
			if math.IsNaN(values[0]) {
				want := int64(1)
				if math.IsNaN(values[1]) {
					want++
				}
				if *stats.NaNCount != want || stats.MinValue != nil || stats.MaxValue != nil {
					t.Fatalf("NaN statistics=%+v", stats)
				}
			} else {
				min, max := parquetStatisticsFloat(stats.MinValue, id), parquetStatisticsFloat(stats.MaxValue, id)
				if *stats.NaNCount != 0 || min != values[0] || max != values[1] || !math.Signbit(min) || math.Signbit(max) {
					t.Fatalf("bounds=%v/%v NaNs=%d", min, max, *stats.NaNCount)
				}
			}
			v.Release()
		}
	}
	scope := memory.NewManager(1 << 20).NewScope()
	v := newVector(t, scope, memory.NewBufferManager(nil), types.LogicalType{ID: types.Varchar}, 2)
	v.SetNull(0)
	v.SetNull(1)
	stats := collectParquetStatistics(v, 0, 2)
	if *stats.NullCount != 2 || stats.MinValue != nil || stats.MaxValue != nil {
		t.Fatalf("NULL statistics=%+v", stats)
	}
	if err := v.SetString(1, strings.Repeat("x", maxParquetStatisticsBound+1)); err != nil {
		t.Fatal(err)
	}
	stats = collectParquetStatistics(v, 0, 2)
	if *stats.NullCount != 1 || stats.MinValue != nil || stats.MaxValue != nil {
		t.Fatalf("long statistics=%+v", stats)
	}
	v.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed leak=%d", scope.Used())
	}
}

func TestParquetStatisticsSurviveRowGroupsAndPages(t *testing.T) {
	scope := memory.NewManager(2 << 20).NewScope()
	bm := memory.NewBufferManager(nil)
	sink := &captureSink{}
	typ := types.LogicalType{ID: types.Varchar}
	writer, err := NewParquetWriter([]Column{{Name: "s", Type: typ}}, ParquetWriteOptions{RowGroupRows: 3, PageRows: 2, PageBufferSize: 32}, scope, bm, sink)
	if err != nil {
		t.Fatal(err)
	}
	v := newVector(t, scope, bm, typ, 6)
	for i, s := range []string{"", "b", "a", "z", "m", "n"} {
		if err := v.SetString(i, s); err != nil {
			t.Fatal(err)
		}
	}
	chunk, _ := vector.NewDataChunk(v)
	if err := writer.WriteChunk(context.Background(), chunk); err != nil {
		t.Fatal(err)
	}
	chunk.Release()
	if _, err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	data := sink.buf.Bytes()
	footerLength := int(binary.LittleEndian.Uint32(data[len(data)-8:]))
	meta, err := parquet.DecodeFileMetaData(data[len(data)-8-footerLength : len(data)-8])
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.ColumnOrders) != 1 || meta.ColumnOrders[0] != parquet.TypeDefinedOrder {
		t.Fatalf("orders=%v", meta.ColumnOrders)
	}
	for i, want := range [][2]string{{"", "b"}, {"m", "z"}} {
		column := meta.RowGroups[i].Columns[0].MetaData
		stats := column.Statistics
		if stats.MinValue == nil || string(stats.MinValue) != want[0] || string(stats.MaxValue) != want[1] || *stats.NullCount != 0 {
			t.Fatalf("group=%d stats=%+v", i, stats)
		}
		header, _, err := parquet.DecodePageHeader(data[column.DataPageOffset:])
		if err != nil {
			t.Fatal(err)
		}
		if header.Data.Statistics == nil || *header.Data.Statistics.NullCount != 0 {
			t.Fatalf("page missing statistics: %+v", header)
		}
	}
	if scope.Used() != 0 {
		t.Fatalf("managed leak=%d", scope.Used())
	}
}

func TestParquetStatisticsBudgetFailureCleanup(t *testing.T) {
	// The first budget rejects the statistics reservation; the second permits
	// encoding and rejects persistent footer metadata growth after the column.
	for _, limit := range []int64{200, 700} {
		scope := memory.NewManager(limit).NewScope()
		sourceScope := memory.NewManager(1 << 20).NewScope()
		bm := memory.NewBufferManager(nil)
		typ := types.LogicalType{ID: types.BigInt}
		sink := &captureSink{}
		writer, err := NewParquetWriter([]Column{{Name: "id", Type: typ}}, ParquetWriteOptions{RowGroupRows: 2, PageRows: 1, PageBufferSize: 32, Compression: "identity"}, scope, bm, sink)
		if err != nil {
			t.Fatal(err)
		}
		v := newVector(t, sourceScope, bm, typ, 2)
		v.SetInt64(0, 1)
		v.SetInt64(1, 2)
		chunk, _ := vector.NewDataChunk(v)
		err = writer.WriteChunk(context.Background(), chunk)
		if !errors.Is(err, memory.ErrLimitExceeded) {
			t.Fatalf("limit=%d error=%v", limit, err)
		}
		if err := writer.Abort(context.Background()); err != nil {
			t.Fatal(err)
		}
		chunk.Release()
		if !sink.aborted || scope.Used() != 0 || sourceScope.Used() != 0 {
			t.Fatalf("limit=%d aborted=%v memory=%d/%d", limit, sink.aborted, scope.Used(), sourceScope.Used())
		}
	}
}

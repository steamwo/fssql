package export

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/format/parquet"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type dictionaryReadDriver struct {
	vfs.Driver
	data []byte
}

func (d *dictionaryReadDriver) ReadRange(ctx context.Context, r vfs.RangeRequest, scope *memory.Scope, bm *memory.BufferManager) (vfs.RangeResult, error) {
	if err := ctx.Err(); err != nil {
		return vfs.RangeResult{}, err
	}
	if r.Offset < 0 || r.Length <= 0 || r.Offset > int64(len(d.data)) || r.Length > int64(len(d.data))-r.Offset {
		return vfs.RangeResult{}, io.ErrUnexpectedEOF
	}
	buffer, err := bm.Allocate(scope, r.Tag, int(r.Length))
	if err != nil {
		return vfs.RangeResult{}, err
	}
	copy(buffer.Bytes(), d.data[r.Offset:r.Offset+r.Length])
	return vfs.RangeResult{Buffer: buffer}, nil
}

func verifyDictionaryArtifact(t *testing.T, data []byte, want [][]byte, valid []bool, encoded bool) {
	t.Helper()
	scope := memory.NewManager(16 << 20).NewScope()
	bm := memory.NewBufferManager(nil)
	driver := &dictionaryReadDriver{data: data}
	uri, _ := vfs.ParseURI("mem://dictionary/output.parquet")
	footer, err := parquet.DiscoverFooter(context.Background(), driver, parquet.FooterSource{URI: uri, Size: int64(len(data))}, scope, bm, 0)
	if err != nil {
		t.Fatal(err)
	}
	position := 0
	for _, group := range footer.Metadata.RowGroups {
		meta := group.Columns[0].MetaData
		if (meta.DictionaryPageOffset != nil) != encoded {
			t.Fatalf("dictionary offset=%v expected=%v", meta.DictionaryPageOffset, encoded)
		}
		if encoded && (*meta.DictionaryPageOffset >= meta.DataPageOffset || *meta.DictionaryPageOffset != group.Columns[0].FileOffset) {
			t.Fatalf("dictionary/data offsets: %+v", meta)
		}
		reader, err := parquet.NewRowGroupScanner(driver, uri, vfs.ObjectIdentity{}, footer.Schema, group, []string{"s"}, nil, scope, bm, 113)
		if err != nil {
			t.Fatal(err)
		}
		for {
			chunk, err := reader.Next(context.Background())
			if err == io.EOF {
				break
			}
			if err != nil {
				reader.Close()
				t.Fatal(err)
			}
			for row := 0; row < chunk.Cardinality; row++ {
				value, ok := chunk.Columns[0].Bytes(row)
				if position >= len(want) || ok != valid[position] || ok && !bytes.Equal(value, want[position]) {
					t.Fatalf("row=%d value=%x valid=%v", position, value, ok)
				}
				position++
			}
			chunk.Release()
		}
		reader.Close()
	}
	if position != len(want) || scope.Used() != 0 {
		t.Fatalf("rows=%d expected=%d managed=%d", position, len(want), scope.Used())
	}
}

func TestParquetDictionaryRoundTrip(t *testing.T) {
	for _, entries := range []int{1, 2, 256, 257} {
		for _, codec := range []string{"identity", "snappy"} {
			t.Run(fmt.Sprintf("%d/%s", entries, codec), func(t *testing.T) {
				scope := memory.NewManager(32 << 20).NewScope()
				bm := memory.NewBufferManager(nil)
				typ := types.LogicalType{ID: types.Varchar, Binary: entries == 2}
				sink := &captureSink{}
				writer, err := NewParquetWriter([]Column{{Name: "s", Type: typ}}, ParquetWriteOptions{RowGroupRows: 2048, PageRows: 257, Compression: codec}, scope, bm, sink)
				if err != nil {
					t.Fatal(err)
				}
				want := make([][]byte, 4096)
				valid := make([]bool, len(want))
				for group := 0; group < 2; group++ {
					v := newVector(t, scope, bm, typ, 2048)
					for row := 0; row < 2048; row++ {
						position := group*2048 + row
						if row%17 == 0 || row >= 257 && row < 514 {
							v.SetNull(row)
							continue
						}
						var value []byte
						if entries == 1 {
							value = []byte{}
						} else if entries == 2 {
							value = []byte{byte(group), 0xff, byte(row % 2)}
						} else {
							value = []byte(fmt.Sprintf("组%d/value-%03d", group, row%entries))
						}
						if err := v.SetStringBytes(row, value); err != nil {
							t.Fatal(err)
						}
						want[position], valid[position] = value, true
					}
					chunk, _ := vector.NewDataChunk(v)
					if err := writer.WriteChunk(context.Background(), chunk); err != nil {
						t.Fatal(err)
					}
					chunk.Release()
				}
				if _, err := writer.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				if scope.Used() != 0 {
					t.Fatalf("writer managed leak=%d", scope.Used())
				}
				verifyDictionaryArtifact(t, sink.buf.Bytes(), want, valid, entries <= 256)
			})
		}
	}
}

func TestParquetDictionaryPlanningLimits(t *testing.T) {
	source := memory.NewManager(8 << 20).NewScope()
	bm := memory.NewBufferManager(nil)
	v := newVector(t, source, bm, types.LogicalType{ID: types.Varchar}, 64)
	for row := 0; row < 64; row++ {
		if err := v.SetString(row, fmt.Sprintf("region-%d", row%2)); err != nil {
			t.Fatal(err)
		}
	}
	for _, limit := range []int64{5000, parquetDictionaryMetadataBytes + 64} {
		scope := memory.NewManager(limit).NewScope()
		d, err := planParquetDictionary(context.Background(), v, 64, 16, scope, bm)
		if err != nil || d != nil || scope.Used() != 0 {
			t.Fatalf("limit=%d dictionary=%v error=%v memory=%d", limit, d, err, scope.Used())
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	scope := memory.NewManager(1 << 20).NewScope()
	if d, err := planParquetDictionary(ctx, v, 64, 16, scope, bm); !errors.Is(err, context.Canceled) || d != nil || scope.Used() != 0 {
		t.Fatalf("cancellation dictionary=%v error=%v memory=%d", d, err, scope.Used())
	}
	for row := 0; row < 64; row++ {
		if err := v.SetString(row, strings.Repeat(string(rune('a'+row%2)), 20000)); err != nil {
			t.Fatal(err)
		}
	}
	if d, err := planParquetDictionary(context.Background(), v, 64, 16, scope, bm); err != nil || d != nil || scope.Used() != 0 {
		t.Fatalf("large dictionary=%v error=%v memory=%d", d, err, scope.Used())
	}
	v.Release()
	if source.Used() != 0 {
		t.Fatalf("source leak=%d", source.Used())
	}
}

func TestParquetDictionaryWorkspaceFallback(t *testing.T) {
	scope := memory.NewManager(12000).NewScope()
	source := memory.NewManager(1 << 20).NewScope()
	bm := memory.NewBufferManager(nil)
	typ := types.LogicalType{ID: types.Varchar}
	sink := &captureSink{}
	writer, err := NewParquetWriter([]Column{{Name: "s", Type: typ}}, ParquetWriteOptions{RowGroupRows: 64, PageRows: 16, PageBufferSize: 32, Compression: "identity"}, scope, bm, sink)
	if err != nil {
		t.Fatal(err)
	}
	v := newVector(t, source, bm, typ, 64)
	want := make([][]byte, 64)
	valid := make([]bool, 64)
	for row := 0; row < 64; row++ {
		want[row] = []byte{byte('a' + row%2)}
		valid[row] = true
		if err := v.SetStringBytes(row, want[row]); err != nil {
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
	if scope.Used() != 0 || source.Used() != 0 {
		t.Fatalf("managed leak=%d/%d", scope.Used(), source.Used())
	}
	header, _, err := parquet.DecodePageHeader(sink.buf.Bytes()[4:])
	if err != nil || header.Type != parquet.PageData {
		t.Fatalf("fallback emitted dictionary bytes: header=%+v error=%v", header, err)
	}
	verifyDictionaryArtifact(t, sink.buf.Bytes(), want, valid, false)
}

type dictionaryFailSink struct {
	captureSink
	writes, failAt int
}

func (s *dictionaryFailSink) Write(ctx context.Context, p []byte) (int, error) {
	s.writes++
	if s.writes == s.failAt {
		return 0, io.ErrClosedPipe
	}
	return s.captureSink.Write(ctx, p)
}

func TestParquetDictionarySinkFailureCleanup(t *testing.T) {
	for _, failAt := range []int{2, 3} {
		scope := memory.NewManager(2 << 20).NewScope()
		bm := memory.NewBufferManager(nil)
		typ := types.LogicalType{ID: types.Varchar}
		sink := &dictionaryFailSink{failAt: failAt}
		writer, err := NewParquetWriter([]Column{{Name: "s", Type: typ}}, ParquetWriteOptions{RowGroupRows: 64, PageRows: 16}, scope, bm, sink)
		if err != nil {
			t.Fatal(err)
		}
		v := newVector(t, scope, bm, typ, 64)
		for row := 0; row < 64; row++ {
			if err := v.SetString(row, "constant"); err != nil {
				t.Fatal(err)
			}
		}
		chunk, _ := vector.NewDataChunk(v)
		if err := writer.WriteChunk(context.Background(), chunk); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("sink error=%v", err)
		}
		if sink.writes != failAt {
			t.Fatalf("replayed after sink error: writes=%d", sink.writes)
		}
		if err := writer.Abort(context.Background()); err != nil {
			t.Fatal(err)
		}
		chunk.Release()
		if scope.Used() != 0 {
			t.Fatalf("managed leak=%d", scope.Used())
		}
	}
}

// Packed groups pad to eight values without adding logical rows.
func TestParquetDictionaryIndexPadding(t *testing.T) {
	scope := memory.NewManager(1 << 20).NewScope()
	bm := memory.NewBufferManager(nil)
	v := newVector(t, scope, bm, types.LogicalType{ID: types.Varchar}, 64)
	for row := 0; row < 64; row++ {
		_ = v.SetString(row, fmt.Sprintf("value-%d", row%5))
	}
	d, err := planParquetDictionary(context.Background(), v, 64, 13, scope, bm)
	if err != nil || d == nil {
		t.Fatalf("dictionary=%v error=%v", d, err)
	}
	for _, count := range []int{1, 7, 8, 9, 13, 64} {
		out, err := newParquetPageBuffer(scope, bm, 32)
		if err != nil {
			t.Fatal(err)
		}
		if err := d.encodeIndices(out, 0, count); err != nil {
			t.Fatal(err)
		}
		indices, err := parquet.DecodeDictionaryIndexes(out.Bytes(), count)
		if err != nil {
			t.Fatal(err)
		}
		for row, index := range indices {
			if index != uint32(row%5) {
				t.Fatalf("row=%d index=%d", row, index)
			}
		}
		out.Release()
	}
	d.Release()
	v.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed leak=%d", scope.Used())
	}
}

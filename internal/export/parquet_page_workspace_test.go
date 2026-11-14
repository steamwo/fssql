package export

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/golang/snappy"
	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/format/parquet"
)

func TestParquetPageWorkspaceReuse(t *testing.T) {
	const rows, groupRows, pageRows, initial = 135, 64, 16, 1024
	for _, codec := range []string{"identity", "snappy"} {
		t.Run(codec, func(t *testing.T) {
			allocations := make(map[int]int)
			bm := memory.NewBufferManager(func(size int) ([]byte, error) {
				allocations[size]++
				return make([]byte, size), nil
			})
			scope := memory.NewManager(1 << 20).NewScope()
			source := memory.NewManager(1 << 20).NewScope()
			typ := types.LogicalType{ID: types.BigInt}
			sink := &captureSink{}
			w, err := NewParquetWriter([]Column{{Name: "a", Type: typ}, {Name: "b", Type: typ}}, ParquetWriteOptions{
				RowGroupRows: groupRows, PageRows: pageRows, PageBufferSize: initial, Compression: codec,
			}, scope, bm, sink)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Abort(context.Background())
			cols := make([]*vector.Vector, 2)
			for col := range cols {
				cols[col] = newVector(t, source, memory.NewBufferManager(nil), typ, rows)
				for row := 0; row < rows; row++ {
					cols[col].SetInt64(row, int64(col*10000+row))
				}
			}
			chunk, _ := vector.NewDataChunk(cols...)
			defer chunk.Release()
			if err := w.WriteChunk(context.Background(), chunk); err != nil {
				t.Fatal(err)
			}
			if scope.UsedByTag(memory.TagExportBuffer) != 0 {
				t.Fatal("flush retained page workspace while buffering the next group")
			}
			if _, err := w.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			chunk.Release()
			if scope.Used() != 0 || source.Used() != 0 {
				t.Fatalf("managed leak=%d/%d", scope.Used(), source.Used())
			}
			if allocations[initial] != 3 {
				t.Fatalf("body allocations=%d, want one per row group", allocations[initial])
			}
			if codec == "snappy" && allocations[snappy.MaxEncodedLen(6+pageRows*8)] != 4 {
				t.Fatalf("full-page compression allocations=%d, want one per column per full group", allocations[snappy.MaxEncodedLen(6+pageRows*8)])
			}
			data := sink.buf.Bytes()
			footerSize := int(binary.LittleEndian.Uint32(data[len(data)-8:]))
			meta, err := parquet.DecodeFileMetaData(data[len(data)-8-footerSize : len(data)-8])
			if err != nil || meta.NumRows != rows || len(meta.RowGroups) != 3 {
				t.Fatalf("metadata=%+v err=%v", meta, err)
			}
			for groupIndex, group := range meta.RowGroups {
				for col, column := range group.Columns {
					offset := column.MetaData.DataPageOffset
					for start := 0; start < int(group.NumRows); start += pageRows {
						header, n, err := parquet.DecodePageHeader(data[offset:])
						if err != nil || header.Data == nil {
							t.Fatalf("page header=%+v err=%v", header, err)
						}
						count := min(pageRows, int(group.NumRows)-start)
						if int(header.Data.NumValues) != count {
							t.Fatalf("page rows=%d want=%d", header.Data.NumValues, count)
						}
						payload := data[offset+int64(n) : offset+int64(n)+int64(header.CompressedPageSize)]
						if codec == "snappy" {
							payload, err = snappy.Decode(nil, payload)
							if err != nil {
								t.Fatal(err)
							}
						}
						if len(payload) != 6+count*8 || !bytes.Equal(payload[:6], []byte{2, 0, 0, 0, byte(count * 2), 1}) {
							t.Fatalf("invalid definition levels or stale page tail: %x", payload)
						}
						for row := 0; row < count; row++ {
							got := int64(binary.LittleEndian.Uint64(payload[6+row*8:]))
							want := int64(col*10000 + groupIndex*groupRows + start + row)
							if got != want {
								t.Fatalf("group=%d col=%d row=%d got=%d want=%d", groupIndex, col, start+row, got, want)
							}
						}
						offset += int64(n) + int64(header.CompressedPageSize)
					}
				}
			}
		})
	}
}

func TestParquetPageWorkspaceSinkFailure(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		for _, failAt := range []int{2, 3, 4, 5} {
			t.Run(fmt.Sprintf("encrypted=%v/write=%d", encrypted, failAt), func(t *testing.T) {
				scope := memory.NewManager(1 << 20).NewScope()
				bm := memory.NewBufferManager(nil)
				typ := types.LogicalType{ID: types.BigInt}
				opts := ParquetWriteOptions{RowGroupRows: 64, PageRows: 16, PageBufferSize: 1024}
				if encrypted {
					opts.Encryption = &ParquetEncryptionConfig{Algorithm: ParquetAES128GCM, KeyID: "test", Key: bytes.Repeat([]byte{7}, 16)}
				}
				sink := &dictionaryFailSink{failAt: failAt}
				w, err := NewParquetWriter([]Column{{Name: "id", Type: typ}}, opts, scope, bm, sink)
				if err != nil {
					t.Fatal(err)
				}
				v := newVector(t, scope, bm, typ, 64)
				for row := 0; row < 64; row++ {
					v.SetInt64(row, int64(row))
				}
				chunk, _ := vector.NewDataChunk(v)
				if err := w.WriteChunk(context.Background(), chunk); !errors.Is(err, io.ErrClosedPipe) || sink.writes != failAt {
					t.Fatalf("error=%v writes=%d", err, sink.writes)
				}
				if scope.UsedByTag(memory.TagExportBuffer) != 0 {
					t.Fatal("failed flush retained page workspace")
				}
				if err := w.Abort(context.Background()); err != nil {
					t.Fatal(err)
				}
				chunk.Release()
				if scope.Used() != 0 || !sink.aborted {
					t.Fatalf("managed=%d aborted=%v", scope.Used(), sink.aborted)
				}
			})
		}
	}
}

func TestParquetPageWorkspaceTrimAndBudget(t *testing.T) {
	scope := memory.NewManager(512).NewScope()
	p := &parquetPageWorkspace{scope: scope, bm: memory.NewBufferManager(nil), initial: 64}
	defer p.release()
	body, err := p.page()
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Append(bytes.Repeat([]byte{7}, 160)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.compress(body.Bytes()); err != nil {
		t.Fatal(err)
	}
	p.trim()
	if scope.Used() != 0 {
		t.Fatalf("oversized page retained %d bytes", scope.Used())
	}
	// Idle encoded bytes must be evicted when the next body cannot be allocated.
	if _, err := p.buffer(&p.compressed, 300); err != nil {
		t.Fatal(err)
	}
	hold, err := scope.Reserve(memory.TagMetadata, 160)
	if err != nil {
		t.Fatal(err)
	}
	defer hold.Release()
	if _, err := p.page(); err != nil || scope.Used() != 224 {
		t.Fatalf("idle buffer reclaim: err=%v managed=%d", err, scope.Used())
	}
	p.release()
	hold.Release()
	// Growing compression must reclaim an idle frame rather than fail a budget
	// that can hold the active output. A later short page releases excess capacity.
	if _, err := p.buffer(&p.frame, 300); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{9}, 160)
	encoded, err := p.compress(payload)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := snappy.Decode(nil, encoded)
	if err != nil || !bytes.Equal(decoded, payload) || scope.Used() != int64(snappy.MaxEncodedLen(len(payload))) {
		t.Fatalf("compression reclaim: err=%v managed=%d", err, scope.Used())
	}
	if _, err := p.compress([]byte{1}); err != nil || scope.Used() != int64(snappy.MaxEncodedLen(1)) {
		t.Fatalf("short-page reservation: err=%v managed=%d", err, scope.Used())
	}
	p.release()
	if scope.Used() != 0 {
		t.Fatalf("managed leak=%d", scope.Used())
	}
}

func TestParquetPageWorkspaceAllocationFailure(t *testing.T) {
	allocationError := errors.New("injected page allocation failure")
	for _, encrypted := range []bool{false, true} {
		last := 2
		if encrypted {
			last = 4
		}
		for failAt := 1; failAt <= last; failAt++ {
			t.Run(fmt.Sprintf("encrypted=%v/allocation=%d", encrypted, failAt), func(t *testing.T) {
				scope := memory.NewManager(1 << 20).NewScope()
				calls, inject := 0, false
				bm := memory.NewBufferManager(func(size int) ([]byte, error) {
					if inject {
						calls++
						if calls == failAt {
							return nil, allocationError
						}
					}
					return make([]byte, size), nil
				})
				typ := types.LogicalType{ID: types.BigInt}
				opts := ParquetWriteOptions{RowGroupRows: 64, PageRows: 16, PageBufferSize: 1024}
				if encrypted {
					opts.Encryption = &ParquetEncryptionConfig{Algorithm: ParquetAES128GCM, KeyID: "test", Key: bytes.Repeat([]byte{7}, 16)}
				}
				w, err := NewParquetWriter([]Column{{Name: "id", Type: typ}}, opts, scope, bm, &captureSink{})
				if err != nil {
					t.Fatal(err)
				}
				defer w.Abort(context.Background())
				v := newVector(t, scope, bm, typ, 64)
				for row := 0; row < 64; row++ {
					v.SetInt64(row, int64(row))
				}
				chunk, _ := vector.NewDataChunk(v)
				defer chunk.Release()
				inject = true
				if err := w.WriteChunk(context.Background(), chunk); !errors.Is(err, allocationError) {
					t.Fatalf("allocation error=%v calls=%d", err, calls)
				}
				if scope.UsedByTag(memory.TagExportBuffer) != 0 {
					t.Fatal("allocation failure retained workspace")
				}
				if err := w.Abort(context.Background()); err != nil {
					t.Fatal(err)
				}
				chunk.Release()
				if scope.Used() != 0 {
					t.Fatalf("managed leak=%d", scope.Used())
				}
			})
		}
	}
}

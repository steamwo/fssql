package fault_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/format/parquet"
	"github.com/steamwo/fssql/internal/format/scan"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// Independent Thrift Compact fixtures use the public parquet-format field IDs.
func indexVarint(dst []byte, value uint64) []byte {
	for value >= 128 {
		dst = append(dst, byte(value)|128)
		value >>= 7
	}
	return append(dst, byte(value))
}
func indexInteger(dst []byte, value int64) []byte {
	return indexVarint(dst, uint64(value<<1)^uint64(value>>63))
}
func indexList(dst []byte, typ byte, count int) []byte {
	dst = append(dst, 0x19) // next field, LIST
	if count < 15 {
		return append(dst, byte(count<<4)|typ)
	}
	return indexVarint(append(dst, 0xf0|typ), uint64(count))
}

type rangeIndexPage struct {
	offset, firstRow int64
	size             int32
	value            int32
}

func rangeOffsetIndex(pages []rangeIndexPage) []byte {
	raw := indexList(nil, 12, len(pages))
	for _, page := range pages {
		raw = indexInteger(append(raw, 0x16), page.offset)
		raw = indexInteger(append(raw, 0x15), int64(page.size))
		raw = indexInteger(append(raw, 0x16), page.firstRow)
		raw = append(raw, 0)
	}
	return append(raw, 0)
}
func rangeColumnIndex(pages []rangeIndexPage) []byte {
	raw := indexList(nil, 1, len(pages))
	for range pages {
		raw = append(raw, 2)
	} // no all-null pages
	for field := 0; field < 2; field++ {
		raw = indexList(raw, 8, len(pages))
		for _, page := range pages {
			var value [4]byte
			binary.LittleEndian.PutUint32(value[:], uint32(page.value))
			raw = append(raw, 4)
			raw = append(raw, value[:]...)
		}
	}
	raw = append(raw, 0x15, 0) // boundary_order = UNORDERED
	raw = indexList(raw, 6, len(pages))
	for range pages {
		raw = append(raw, 0)
	} // null_counts
	return append(raw, 0)
}

func TestS3PageIndexesEliminateRejectedPageProbes(t *testing.T) {
	const pageCount = 64
	const pageRows = 512
	var data []byte
	var idPages, payloadPages []rangeIndexPage
	appendPage := func(value int32, statistics bool) rangeIndexPage {
		body := make([]byte, pageRows*4)
		for row := 0; row < pageRows; row++ {
			binary.LittleEndian.PutUint32(body[row*4:], uint32(value))
		}
		header := parquet.PageHeader{Type: parquet.PageData, UncompressedPageSize: int32(len(body)), CompressedPageSize: int32(len(body)), Data: &parquet.DataPageHeader{NumValues: pageRows, Encoding: parquet.EncodingPlain, DefinitionLevelEncoding: parquet.EncodingRLE, RepetitionLevelEncoding: parquet.EncodingRLE}}
		if statistics {
			zero := int64(0)
			bound := make([]byte, 4)
			binary.LittleEndian.PutUint32(bound, uint32(value))
			header.Data.Statistics = &parquet.Statistics{MinValue: bound, MaxValue: bound, NullCount: &zero}
		}
		raw, err := parquet.EncodePageHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		page := rangeIndexPage{offset: int64(len(data)), size: int32(len(raw) + len(body)), value: value}
		data = append(data, raw...)
		data = append(data, body...)
		return page
	}
	for page := 0; page < pageCount; page++ {
		value := int32(0)
		if page == pageCount-1 {
			value = 1
		}
		location := appendPage(value, true)
		location.firstRow = int64(page * pageRows)
		idPages = append(idPages, location)
	}
	payloadAt := int64(len(data))
	for page := 0; page < pageCount; page++ {
		location := appendPage(int32(page), false)
		location.firstRow = int64(page * pageRows)
		payloadPages = append(payloadPages, location)
	}
	dataEnd := int64(len(data))
	idOffset := rangeOffsetIndex(idPages)
	idOffsetAt, idOffsetLen := int64(len(data)), int32(len(idOffset))
	data = append(data, idOffset...)
	payloadOffset := rangeOffsetIndex(payloadPages)
	payloadOffsetAt, payloadOffsetLen := int64(len(data)), int32(len(payloadOffset))
	data = append(data, payloadOffset...)
	idColumn := rangeColumnIndex(idPages)
	idColumnAt, idColumnLen := int64(len(data)), int32(len(idColumn))
	data = append(data, idColumn...)
	var bytes, requests atomic.Int64
	var indexed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var start, end int64
		if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil || start < 0 || end < start || end >= int64(len(data)) {
			http.Error(w, "invalid range", 416)
			return
		}
		if r.Header.Get("If-Match") != "\"e\"" {
			t.Error("missing object identity precondition")
		}
		if indexed.Load() {
			for _, span := range [][2]int64{{0, idPages[pageCount-1].offset}, {payloadAt, payloadPages[pageCount-1].offset}} {
				if start < span[1] && end >= span[0] {
					t.Errorf("indexed scan probed rejected page: bytes=%d-%d", start, end)
				}
			}
		}
		requests.Add(1)
		bytes.Add(end - start + 1)
		w.Header().Set("ETag", "\"e\"")
		w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.WriteHeader(206)
		_, _ = w.Write(data[start : end+1])
	}))
	defer server.Close()
	driver := newPhase1S3Driver(t, server, time.Second, 1)
	children := int32(2)
	required := parquet.Required
	physical := parquet.TypeInt32
	tree, err := parquet.BuildSchemaTree([]parquet.SchemaElement{{Name: "schema", NumChildren: &children}, {Name: "id", Type: &physical, Repetition: &required}, {Name: "payload", Type: &physical, Repetition: &required}})
	if err != nil {
		t.Fatal(err)
	}
	uri, _ := vfs.ParseURI("s3://bucket/indexed.parquet")
	var baselineBytes, baselineRequests int64
	for _, useIndex := range []bool{false, true} {
		bytes.Store(0)
		requests.Store(0)
		indexed.Store(useIndex)
		rg := parquet.RowGroup{NumRows: pageCount * pageRows, Columns: []parquet.ColumnChunk{
			{MetaData: &parquet.ColumnMetaData{Type: physical, Codec: parquet.CodecUncompressed, NumValues: pageCount * pageRows, TotalCompressedSize: payloadAt, PathInSchema: []string{"id"}}},
			{MetaData: &parquet.ColumnMetaData{Type: physical, Codec: parquet.CodecUncompressed, NumValues: pageCount * pageRows, TotalCompressedSize: dataEnd - payloadAt, DataPageOffset: payloadAt, PathInSchema: []string{"payload"}}},
		}}
		if useIndex {
			rg.Columns[0].OffsetIndexOffset = &idOffsetAt
			rg.Columns[0].OffsetIndexLength = &idOffsetLen
			rg.Columns[0].ColumnIndexOffset = &idColumnAt
			rg.Columns[0].ColumnIndexLength = &idColumnLen
			rg.Columns[1].OffsetIndexOffset = &payloadOffsetAt
			rg.Columns[1].OffsetIndexLength = &payloadOffsetLen
		}
		manager := memory.NewManager(2 << 20)
		scope := manager.NewScope()
		scanner, err := parquet.NewRowGroupScanner(driver, uri, vfs.ObjectIdentity{ETag: "e", Size: int64(len(data))}, tree, rg, []string{"id", "payload"}, []scan.Filter{{Column: "id", Op: scan.Equal, Value: scan.Literal{Type: types.LogicalType{ID: types.Integer}, Int64: 1}}}, scope, memory.NewBufferManager(nil), 2048)
		if err != nil {
			t.Fatal(err)
		}
		rows := 0
		for {
			chunk, err := scanner.Next(context.Background())
			if err == io.EOF {
				break
			}
			if err != nil {
				scanner.Close()
				t.Fatal(err)
			}
			for row := 0; row < chunk.Cardinality; row++ {
				value, valid := chunk.Columns[1].Int32(row)
				if !valid || value != pageCount-1 {
					t.Fatalf("payload=%d/%v", value, valid)
				}
			}
			rows += chunk.Cardinality
			chunk.Release()
		}
		scanner.Close()
		metrics := scanner.Metrics()
		if rows != pageRows || scope.Used() != 0 || metrics.TransferredBytes != bytes.Load() || metrics.UsefulBytes > metrics.TransferredBytes {
			t.Fatalf("rows=%d memory=%d HTTP=%d metrics=%+v", rows, scope.Used(), bytes.Load(), metrics)
		}
		if !useIndex {
			baselineBytes, baselineRequests = bytes.Load(), requests.Load()
		} else {
			if metrics.PagesSkipped != 126 || metrics.PagesRead != 2 || requests.Load()*4 >= baselineRequests || bytes.Load() >= baselineBytes {
				t.Fatalf("baseline=%d bytes/%d requests indexed=%+v", baselineBytes, baselineRequests, metrics)
			}
			t.Logf("S3 indexes: requests %d -> %d, bytes %d -> %d", baselineRequests, requests.Load(), baselineBytes, bytes.Load())
		}
	}
}

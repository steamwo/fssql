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

func TestS3ParquetPageSkippingReducesRangeBytes(t *testing.T) {
	const rowsPerPage = 4096
	page := func(value int32, stats bool) []byte {
		body := make([]byte, rowsPerPage*4)
		for row := 0; row < rowsPerPage; row++ {
			binary.LittleEndian.PutUint32(body[row*4:], uint32(value))
		}
		header := parquet.PageHeader{Type: parquet.PageData, CompressedPageSize: int32(len(body)), UncompressedPageSize: int32(len(body)), Data: &parquet.DataPageHeader{NumValues: rowsPerPage, Encoding: parquet.EncodingPlain, DefinitionLevelEncoding: parquet.EncodingRLE, RepetitionLevelEncoding: parquet.EncodingRLE}}
		if stats {
			zero := int64(0)
			bound := make([]byte, 4)
			binary.LittleEndian.PutUint32(bound, uint32(value))
			header.Data.Statistics = &parquet.Statistics{MinValue: bound, MaxValue: bound, NullCount: &zero}
		}
		raw, err := parquet.EncodePageHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		return append(raw, body...)
	}
	idRejected, idKept := page(0, true), page(1, true)
	payloadRejected, payloadKept := page(10, false), page(20, false)
	idData := append(append([]byte(nil), idRejected...), idKept...)
	payloadData := append(append([]byte(nil), payloadRejected...), payloadKept...)
	data := append(append([]byte(nil), idData...), payloadData...)
	var transferred atomic.Int64
	var early atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var start, end int64
		if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil || start < 0 || end < start || end >= int64(len(data)) {
			http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if r.Header.Get("If-Match") != "\"e\"" {
			t.Error("missing snapshot precondition")
		}
		if early.Load() {
			for _, guard := range []int64{4096, int64(len(idData)) + 4096} {
				if start <= guard && end >= guard {
					t.Errorf("read rejected page interior: bytes=%d-%d", start, end)
				}
			}
		}
		transferred.Add(end - start + 1)
		w.Header().Set("ETag", "\"e\"")
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[start : end+1])
	}))
	defer server.Close()
	driver := newPhase1S3Driver(t, server, time.Second, 1)
	count := int32(2)
	required := parquet.Required
	physical := parquet.TypeInt32
	tree, err := parquet.BuildSchemaTree([]parquet.SchemaElement{{Name: "schema", NumChildren: &count}, {Name: "id", Type: &physical, Repetition: &required}, {Name: "payload", Type: &physical, Repetition: &required}})
	if err != nil {
		t.Fatal(err)
	}
	rg := parquet.RowGroup{NumRows: 2 * rowsPerPage, Columns: []parquet.ColumnChunk{
		{MetaData: &parquet.ColumnMetaData{Type: physical, Codec: parquet.CodecUncompressed, NumValues: 2 * rowsPerPage, TotalCompressedSize: int64(len(idData)), PathInSchema: []string{"id"}}},
		{MetaData: &parquet.ColumnMetaData{Type: physical, Codec: parquet.CodecUncompressed, NumValues: 2 * rowsPerPage, TotalCompressedSize: int64(len(payloadData)), DataPageOffset: int64(len(idData)), PathInSchema: []string{"payload"}}},
	}}
	uri, _ := vfs.ParseURI("s3://bucket/pages.parquet")
	var baseline int64
	for _, optimized := range []bool{false, true} {
		early.Store(optimized)
		transferred.Store(0)
		manager := memory.NewManager(1 << 20)
		scope := manager.NewScope()
		var filters []scan.Filter
		if optimized {
			filters = []scan.Filter{{Column: "id", Op: scan.Equal, Value: scan.Literal{Type: types.LogicalType{ID: types.Integer}, Int64: 1}}}
		}
		scanner, err := parquet.NewRowGroupScanner(driver, uri, vfs.ObjectIdentity{ETag: "e", Size: int64(len(data))}, tree, rg, []string{"id", "payload"}, filters, scope, memory.NewBufferManager(nil), 2048)
		if err != nil {
			t.Fatal(err)
		}
		matched := 0
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
				id, _ := chunk.Columns[0].Int32(row)
				if id != 1 {
					continue
				}
				value, valid := chunk.Columns[1].Int32(row)
				if !valid || value != 20 {
					t.Fatalf("payload=%d/%v", value, valid)
				}
				matched++
			}
			chunk.Release()
		}
		scanner.Close()
		metrics := scanner.Metrics()
		if matched != rowsPerPage || scope.Used() != 0 {
			t.Fatalf("matched=%d memory=%d", matched, scope.Used())
		}
		if metrics.TransferredBytes != transferred.Load() || metrics.UsefulBytes > metrics.TransferredBytes {
			t.Fatalf("HTTP bytes=%d metrics=%+v", transferred.Load(), metrics)
		}
		if !optimized {
			baseline = transferred.Load()
		} else {
			if metrics.PagesSkipped != 2 || metrics.PagesRead != 2 || transferred.Load()*5 >= baseline*3 {
				t.Fatalf("baseline=%d optimized=%+v", baseline, metrics)
			}
			t.Logf("S3 range bytes: baseline=%d optimized=%d; pages read=%d skipped=%d", baseline, transferred.Load(), metrics.PagesRead, metrics.PagesSkipped)
		}
	}
}

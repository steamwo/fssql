package parquet

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/golang/snappy"
	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/format/scan"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func skipTestIntPage(t testing.TB, values []int32, optional, v2 bool, codec CompressionCodec, stats *Statistics) []byte {
	t.Helper()
	var payload, levels []byte
	nulls := 0
	for _, value := range values {
		if optional {
			level := 1
			if value < 0 {
				level = 0
				nulls++
			}
			levels = append(levels, 2, byte(level))
			if level == 0 {
				continue
			}
		}
		var raw [4]byte
		binary.LittleEndian.PutUint32(raw[:], uint32(value))
		payload = append(payload, raw[:]...)
	}
	header := PageHeader{Type: PageData}
	var body []byte
	if v2 {
		header.Type = PageDataV2
		header.DataV2 = &DataPageHeaderV2{NumValues: int32(len(values)), NumRows: int32(len(values)), NumNulls: int32(nulls), Encoding: EncodingPlain, DefinitionLevelsByteLength: int32(len(levels)), IsCompressed: codec != CodecUncompressed, Statistics: stats}
		header.UncompressedPageSize = int32(len(levels) + len(payload))
		if codec == CodecSnappy {
			payload = snappy.Encode(nil, payload)
		}
		body = append(levels, payload...)
	} else {
		header.Data = &DataPageHeader{NumValues: int32(len(values)), Encoding: EncodingPlain, DefinitionLevelEncoding: EncodingRLE, RepetitionLevelEncoding: EncodingRLE, Statistics: stats}
		if optional {
			body = make([]byte, 4)
			binary.LittleEndian.PutUint32(body, uint32(len(levels)))
			body = append(body, levels...)
		}
		body = append(body, payload...)
		header.UncompressedPageSize = int32(len(body))
		if codec == CodecSnappy {
			body = snappy.Encode(nil, body)
		}
	}
	header.CompressedPageSize = int32(len(body))
	raw, err := EncodePageHeader(header)
	if err != nil {
		t.Fatal(err)
	}
	return append(raw, body...)
}

func skipTestCursor(t *testing.T, data []byte, rows int64, optional bool, codec CompressionCodec) (*ColumnCursor, *recordingDriver, *memory.Scope) {
	t.Helper()
	manager := memory.NewManager(2 << 20)
	scope := manager.NewScope()
	driver := &recordingDriver{data: data}
	uri, _ := vfs.ParseURI("mem://skip/pages")
	leaf := requiredInt32Leaf()
	if optional {
		leaf = optionalInt32Leaf()
	}
	cursor, err := NewColumnCursor(driver, uri, vfs.ObjectIdentity{ETag: "e", Size: int64(len(data))}, &ColumnMetaData{Type: TypeInt32, Codec: codec, NumValues: rows, TotalCompressedSize: int64(len(data)), PathInSchema: leaf.Path}, leaf, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cursor.Close()
		if scope.Used() != 0 {
			t.Errorf("managed leak=%d", scope.Used())
		}
	})
	return cursor, driver, scope
}

func TestSkipScalarWholePagesAndPartialAlignment(t *testing.T) {
	for _, optional := range []bool{false, true} {
		for _, v2 := range []bool{false, true} {
			for _, codec := range []CompressionCodec{CodecUncompressed, CodecSnappy} {
				values := make([]int32, 512)
				for row := range values {
					values[row] = int32(row)
					if optional && row%3 == 0 {
						values[row] = -1
					}
				}
				var data []byte
				for page := 0; page < 3; page++ {
					data = append(data, skipTestIntPage(t, values, optional, v2, codec, nil)...)
				}
				cursor, driver, _ := skipTestCursor(t, data, 1536, optional, codec)
				if err := cursor.skipScalar(context.Background(), 1024); err != nil {
					t.Fatal(err)
				}
				if m := cursor.Metrics(); m.PagesSkipped != 2 || m.PagesRead != 0 || m.DataValues != 0 {
					t.Fatalf("metrics=%+v", m)
				}
				for _, request := range driver.ranges {
					if request.Length > initialPageHeaderProbe {
						t.Fatalf("skip fetched page body: %+v", request)
					}
				}
				out, err := cursor.ReadScalar(context.Background(), 13)
				if err != nil {
					t.Fatal(err)
				}
				for row := 0; row < out.Count(); row++ {
					got, valid := out.Int32(row)
					if valid != (values[row] >= 0) || valid && got != values[row] {
						t.Fatalf("row=%d got=%d/%v", row, got, valid)
					}
				}
				out.Release()
				if err := cursor.skipScalar(context.Background(), 100); err != nil {
					t.Fatal(err)
				}
				out, err = cursor.ReadScalar(context.Background(), 1)
				if err != nil {
					t.Fatal(err)
				}
				got, valid := out.Int32(0)
				if !valid || got != 113 {
					t.Fatalf("post-skip value=%d/%v", got, valid)
				}
				out.Release()
				if err := cursor.skipScalar(context.Background(), 398); err != nil {
					t.Fatal(err)
				}
				if cursor.Consumed() != 1536 {
					t.Fatalf("consumed=%d", cursor.Consumed())
				}
			}
		}
	}
}

func TestSkipWholeDictionaryPagePreservesLaterDictionary(t *testing.T) {
	for _, plain := range []bool{false, true} {
		cursor, _ := dictionaryBatchCursor(t, []string{"alpha", "long dictionary payload beyond inline storage"}, []dictionaryBatchTestPage{{indices: []int{0, 1, -1, 1}}, {indices: []int{1, 0, -1, 1}, plain: plain}}, false, nil)
		if err := cursor.skipScalar(context.Background(), 4); err != nil {
			t.Fatal(err)
		}
		if cursor.Metrics().PagesSkipped != 1 {
			t.Fatalf("metrics=%+v", cursor.Metrics())
		}
		out, err := cursor.ReadScalar(context.Background(), 4)
		if err != nil {
			t.Fatal(err)
		}
		assertDictionaryBatchValues(t, out, []string{"alpha", "long dictionary payload beyond inline storage"}, []int{1, 0, -1, 1})
		out.Release()
	}
}

func TestPageStatisticsPruneBeforeBodyRead(t *testing.T) {
	for _, v2 := range []bool{false, true} {
		zero := int64(0)
		bound := make([]byte, 4)
		first := skipTestIntPage(t, make([]int32, 4096), false, v2, CodecUncompressed, &Statistics{MinValue: bound, MaxValue: bound, NullCount: &zero})
		secondValues := make([]int32, 4096)
		for row := range secondValues {
			secondValues[row] = 1
		}
		second := skipTestIntPage(t, secondValues, false, v2, CodecUncompressed, nil)
		data := append(first, second...)
		cursor, driver, scope := skipTestCursor(t, data, 8192, false, CodecUncompressed)
		filter := scan.Filter{Column: "x", Op: scan.Equal, Value: scan.Literal{Type: types.LogicalType{ID: types.Integer}, Int64: 1}}
		scanner := &RowGroupScanner{rowGroup: RowGroup{NumRows: 8192}, batchSize: 1024, readers: []projectedColumnReader{&scalarColumnReader{cursor: cursor}}}
		scanner.initFilters([]scan.Filter{filter}, scope, cursor.bm)
		rows := 0
		for {
			chunk, err := scanner.Next(context.Background())
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			for row := 0; row < chunk.Cardinality; row++ {
				value, valid := chunk.Columns[0].Int32(row)
				if !valid || value != 1 {
					t.Fatalf("value=%d/%v", value, valid)
				}
			}
			rows += chunk.Cardinality
			chunk.Release()
		}
		if rows != 4096 || scanner.Position() != 8192 {
			t.Fatalf("rows=%d position=%d", rows, scanner.Position())
		}
		metrics := scanner.Metrics()
		if metrics.PagesSkipped != 1 || metrics.PagesRead != 1 || metrics.DataValues != 4096 || metrics.UsefulBytes > metrics.TransferredBytes {
			t.Fatalf("metrics=%+v", metrics)
		}
		for _, request := range driver.ranges {
			if request.Offset < int64(len(first)) && request.Offset+request.Length > 4096 {
				t.Fatalf("read rejected page body: %+v", request)
			}
		}
		scanner.Close()
	}
}

func TestPagePruningConservativeStatistics(t *testing.T) {
	zero := int64(0)
	badNulls := int64(99)
	inexact := false
	lo, hi := make([]byte, 4), make([]byte, 4)
	binary.LittleEndian.PutUint32(lo, 1)
	binary.LittleEndian.PutUint32(hi, 2)
	for _, stats := range []*Statistics{nil, {NullCount: &badNulls}, {MinValue: lo, MaxValue: hi, NullCount: &zero, IsMinValueExact: &inexact}, {MinValue: hi, MaxValue: lo, NullCount: &zero}} {
		data := skipTestIntPage(t, []int32{1, 2}, false, false, CodecUncompressed, stats)
		cursor, _, _ := skipTestCursor(t, data, 2, false, CodecUncompressed)
		filter := scan.Filter{Column: "x", Op: scan.Equal, Value: scan.Literal{Type: types.LogicalType{ID: types.Integer}, Int64: 100}}
		if skipped, _, err := cursor.pruneFilterPage(context.Background(), filter); err != nil || skipped != 0 {
			t.Fatalf("stats=%+v skipped=%d err=%v", stats, skipped, err)
		}
		out, err := cursor.ReadScalar(context.Background(), 2)
		if err != nil {
			t.Fatal(err)
		}
		out.Release()
		if cursor.Metrics().PagesSkipped != 0 || cursor.Metrics().PagesRead != 1 {
			t.Fatalf("metrics=%+v", cursor.Metrics())
		}
	}
}

func TestSkipPageRejectsTruncationAndObjectChange(t *testing.T) {
	valid := skipTestIntPage(t, make([]int32, 512), false, false, CodecUncompressed, nil)
	cursor, _, _ := skipTestCursor(t, valid[:len(valid)-1], 512, false, CodecUncompressed)
	if err := cursor.skipScalar(context.Background(), 512); err == nil || cursor.Consumed() != 0 {
		t.Fatalf("truncated page err=%v consumed=%d", err, cursor.Consumed())
	}
	cursor, driver, _ := skipTestCursor(t, valid, 512, false, CodecUncompressed)
	driver.mutateAt = 1
	if err := cursor.skipScalar(context.Background(), 512); !errors.Is(err, vfs.ErrObjectChanged) || cursor.Consumed() != 0 {
		t.Fatalf("identity err=%v consumed=%d", err, cursor.Consumed())
	}
	cursor, _, _ = skipTestCursor(t, valid, 511, false, CodecUncompressed)
	if err := cursor.skipScalar(context.Background(), 511); err == nil || cursor.Consumed() != 0 {
		t.Fatalf("oversized page err=%v consumed=%d", err, cursor.Consumed())
	}
}

func TestPagePruningNullPredicates(t *testing.T) {
	zero, two := int64(0), int64(2)
	first := skipTestIntPage(t, []int32{1, 2}, true, true, CodecSnappy, &Statistics{NullCount: &zero})
	second := skipTestIntPage(t, []int32{-1, -1}, true, true, CodecSnappy, &Statistics{NullCount: &two})
	cursor, _, _ := skipTestCursor(t, append(first, second...), 4, true, CodecSnappy)
	for _, op := range []scan.CompareOp{scan.IsNull, scan.IsNotNull} {
		skipped, _, err := cursor.pruneFilterPage(context.Background(), scan.Filter{Column: "x", Op: op})
		if err != nil || skipped != 2 {
			t.Fatalf("op=%d skipped=%d err=%v", op, skipped, err)
		}
	}
	if cursor.Metrics().PagesRead != 0 || cursor.Metrics().PagesSkipped != 2 {
		t.Fatalf("metrics=%+v", cursor.Metrics())
	}
}

func TestPagePruningKeepsUnalignedPayloadRows(t *testing.T) {
	zero := int64(0)
	bound := make([]byte, 4)
	first := skipTestIntPage(t, []int32{0, 0, 0}, false, false, CodecUncompressed, &Statistics{MinValue: bound, MaxValue: bound, NullCount: &zero})
	idData := append(first, skipTestIntPage(t, []int32{0, 1, 0, 1, 0}, false, false, CodecUncompressed, nil)...)
	payloadData := append(skipTestIntPage(t, []int32{100, 101, 102, 103}, false, false, CodecUncompressed, nil), skipTestIntPage(t, []int32{104, 105, 106, 107}, false, false, CodecUncompressed, nil)...)
	data := append(append([]byte(nil), idData...), payloadData...)
	driver := &recordingDriver{data: data}
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	uri, _ := vfs.ParseURI("mem://skip/alignment")
	tree := &SchemaTree{Leaves: []*SchemaNode{scannerInt32Leaf("id"), scannerInt32Leaf("payload")}}
	rg := RowGroup{NumRows: 8, Columns: []ColumnChunk{
		{MetaData: &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 8, TotalCompressedSize: int64(len(idData)), PathInSchema: []string{"id"}}},
		{MetaData: &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 8, TotalCompressedSize: int64(len(payloadData)), DataPageOffset: int64(len(idData)), PathInSchema: []string{"payload"}}},
	}}
	scanner, err := NewRowGroupScanner(driver, uri, vfs.ObjectIdentity{}, tree, rg, []string{"payload", "id"}, []scan.Filter{{Column: "id", Op: scan.Equal, Value: scan.Literal{Type: types.LogicalType{ID: types.Integer}, Int64: 1}}}, scope, memory.NewBufferManager(nil), 2)
	if err != nil {
		t.Fatal(err)
	}
	var got []int32
	for {
		chunk, err := scanner.Next(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		for row := 0; row < chunk.Cardinality; row++ {
			value, valid := chunk.Columns[0].Int32(row)
			if !valid {
				t.Fatal("NULL payload")
			}
			got = append(got, value)
		}
		chunk.Release()
	}
	if len(got) != 2 || got[0] != 104 || got[1] != 106 {
		t.Fatalf("payload rows=%v", got)
	}
	metrics := scanner.Metrics()
	if _, err := BuildScanProfile(ScanProfileInput{RowGroupsTotal: 1, RowGroupMetrics: []RowGroupScanMetrics{metrics}}); err != nil {
		t.Fatal(err)
	}
	scanner.Close()
	if scope.Used() != 0 {
		t.Fatalf("managed leak=%d", scope.Used())
	}
}

func TestSkipPageCancellationAndMemoryLimit(t *testing.T) {
	data := skipTestIntPage(t, make([]int32, 512), false, false, CodecUncompressed, nil)
	cursor, driver, _ := skipTestCursor(t, data, 512, false, CodecUncompressed)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cursor.skipScalar(ctx, 512); !errors.Is(err, context.Canceled) || len(driver.ranges) != 0 {
		t.Fatalf("cancel err=%v ranges=%v", err, driver.ranges)
	}
	manager := memory.NewManager(64)
	scope := manager.NewScope()
	cursor, err := NewColumnCursor(driver, cursor.uri, vfs.ObjectIdentity{}, cursor.meta, cursor.leaf, scope, cursor.bm)
	if err != nil {
		t.Fatal(err)
	}
	if err := cursor.skipScalar(context.Background(), 512); !errors.Is(err, memory.ErrLimitExceeded) || cursor.Consumed() != 0 {
		t.Fatalf("budget err=%v consumed=%d", err, cursor.Consumed())
	}
	cursor.Close()
	if scope.Used() != 0 {
		t.Fatalf("managed leak=%d", scope.Used())
	}
}

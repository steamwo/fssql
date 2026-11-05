package parquet

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/format/scan"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func testOffsetIndex(pages []indexedPage) []byte {
	w := &compactWriter{}
	w.listHeader(1, ctStruct, len(pages))
	for _, page := range pages {
		w.b = appendStruct(w.b, func(p *compactWriter) {
			p.i64Field(1, page.offset)
			p.i32Field(2, page.size)
			p.i64Field(3, page.firstRow)
		})
	}
	w.stop()
	return w.b
}

func testColumnIndex(pages []indexedPage, counts bool) []byte {
	w := &compactWriter{}
	w.listHeader(1, ctBoolTrue, len(pages))
	for _, page := range pages {
		value := ctBoolFalse
		if page.nullPage {
			value = ctBoolTrue
		}
		w.b = append(w.b, value)
	}
	for field := int16(2); field <= 3; field++ {
		w.listHeader(field, ctBinary, len(pages))
		for _, page := range pages {
			value := page.min
			if field == 3 {
				value = page.max
			}
			w.b = putVarint(w.b, uint64(len(value)))
			w.b = append(w.b, value...)
		}
	}
	w.i32Field(4, 0)
	if counts {
		w.listHeader(5, ctI64, len(pages))
		for _, page := range pages {
			w.b = putVarint(w.b, zig(page.nulls))
		}
	}
	w.stop()
	return w.b
}

func indexTestFixture(t *testing.T, optional, v2 bool, codec CompressionCodec) ([]byte, []indexedPage) {
	t.Helper()
	var data []byte
	var pages []indexedPage
	for page := 0; page < 3; page++ {
		values := make([]int32, 512)
		for row := range values {
			values[row] = int32(page)
		}
		raw := skipTestIntPage(t, values, optional, v2, codec, nil)
		bound := make([]byte, 4)
		binary.LittleEndian.PutUint32(bound, uint32(page))
		pages = append(pages, indexedPage{offset: int64(len(data)), firstRow: int64(page * 512), size: int32(len(raw)), min: bound, max: bound})
		data = append(data, raw...)
	}
	return data, pages
}

func indexTestCursor(t *testing.T, data []byte, pages []indexedPage, rows int64, optional bool, codec CompressionCodec, column []byte) (*ColumnCursor, *recordingDriver, *memory.Scope) {
	t.Helper()
	dataEnd := int64(len(data))
	offset := testOffsetIndex(pages)
	offsetAt := int64(len(data))
	offsetLen := int32(len(offset))
	data = append(append([]byte(nil), data...), offset...)
	columnAt, columnLen := int64(len(data)), int32(len(column))
	data = append(data, column...)
	cursor, driver, scope := skipTestCursor(t, data, rows, optional, codec)
	cursor.end = dataEnd
	cursor.meta.TotalCompressedSize = dataEnd
	chunk := ColumnChunk{MetaData: cursor.meta, OffsetIndexOffset: &offsetAt, OffsetIndexLength: &offsetLen}
	if column != nil {
		chunk.ColumnIndexOffset = &columnAt
		chunk.ColumnIndexLength = &columnLen
	}
	cursor.configurePageIndex(chunk)
	return cursor, driver, scope
}

func TestColumnIndexPrunesWithoutReadingRejectedHeaders(t *testing.T) {
	for _, optional := range []bool{false, true} {
		for _, v2 := range []bool{false, true} {
			for _, codec := range []CompressionCodec{CodecUncompressed, CodecSnappy} {
				data, pages := indexTestFixture(t, optional, v2, codec)
				cursor, driver, scope := indexTestCursor(t, data, pages, 1536, optional, codec, testColumnIndex(pages, true))
				filter := scan.Filter{Column: "x", Op: scan.Equal, Value: scan.Literal{Type: types.LogicalType{ID: types.Integer}, Int64: 2}}
				for page := 0; page < 2; page++ {
					skipped, _, err := cursor.pruneFilterPage(context.Background(), filter)
					if err != nil || skipped != 512 {
						t.Fatalf("page=%d skipped=%d err=%v", page, skipped, err)
					}
				}
				if skipped, available, err := cursor.pruneFilterPage(context.Background(), filter); err != nil || skipped != 0 || available != 512 {
					t.Fatalf("retained page=%d/%d err=%v", skipped, available, err)
				}
				out, err := cursor.ReadScalar(context.Background(), 512)
				if err != nil {
					t.Fatal(err)
				}
				for row := 0; row < out.Count(); row++ {
					value, valid := out.Int32(row)
					if !valid || value != 2 {
						t.Fatalf("value=%d/%v", value, valid)
					}
				}
				out.Release()
				for _, request := range driver.ranges {
					if request.Offset < pages[2].offset {
						t.Fatalf("read rejected header/body: %+v", request)
					}
					if request.Expected == nil || request.Expected.ETag != "e" {
						t.Fatal("unconditional index read")
					}
				}
				if m := cursor.Metrics(); m.PagesSkipped != 2 || m.PagesRead != 1 || m.DataValues != 512 || m.UsefulBytes > m.TransferredBytes {
					t.Fatalf("metrics=%+v", m)
				}
				if scope.UsedByTag(memory.TagMetadata) == 0 {
					t.Fatal("indexes are not memory-accounted")
				}
			}
		}
	}
}

func TestOffsetIndexWorksWithoutColumnIndex(t *testing.T) {
	data, pages := indexTestFixture(t, false, false, CodecUncompressed)
	cursor, driver, _ := indexTestCursor(t, data, pages, 1536, false, CodecUncompressed, nil)
	if err := cursor.skipScalar(context.Background(), 1024); err != nil {
		t.Fatal(err)
	}
	out, err := cursor.ReadScalar(context.Background(), 512)
	if err != nil {
		t.Fatal(err)
	}
	if value, valid := out.Int32(0); !valid || value != 2 {
		t.Fatalf("value=%d/%v", value, valid)
	}
	out.Release()
	for _, request := range driver.ranges {
		if request.Offset < pages[2].offset {
			t.Fatalf("skip read discarded page: %+v", request)
		}
	}
	if cursor.index.columnTried {
		t.Fatal("payload skip loaded column index")
	}
}

func TestPageIndexMalformedMetadataFallsBack(t *testing.T) {
	for _, failure := range []string{"overlap", "row_order", "first_row", "chunk_end", "truncated_column", "count_mismatch"} {
		t.Run(failure, func(t *testing.T) {
			data, pages := indexTestFixture(t, false, false, CodecUncompressed)
			column := testColumnIndex(pages, true)
			switch failure {
			case "overlap":
				pages[1].offset = pages[0].offset
			case "row_order":
				pages[1].firstRow = pages[0].firstRow
			case "first_row":
				pages[0].firstRow = 1
			case "chunk_end":
				pages[2].size++
			case "truncated_column":
				column = column[:len(column)-1]
			case "count_mismatch":
				column = testColumnIndex(pages[:2], true)
			}
			cursor, _, _ := indexTestCursor(t, data, pages, 1536, false, CodecUncompressed, column)
			filter := scan.Filter{Column: "x", Op: scan.Equal, Value: scan.Literal{Type: types.LogicalType{ID: types.Integer}, Int64: 2}}
			if skipped, _, err := cursor.pruneFilterPage(context.Background(), filter); err != nil || skipped != 0 {
				t.Fatalf("fallback skipped=%d err=%v", skipped, err)
			}
			if cursor.index.columnValid {
				t.Fatal("invalid index accepted")
			}
			out, err := cursor.ReadScalar(context.Background(), 512)
			if err != nil {
				t.Fatal(err)
			}
			if value, valid := out.Int32(0); !valid || value != 0 {
				t.Fatalf("fallback value=%d/%v", value, valid)
			}
			out.Release()
		})
	}
}

func TestPageIndexBudgetAndBoundsFallback(t *testing.T) {
	data, pages := indexTestFixture(t, false, false, CodecUncompressed)
	cursor, driver, scope := indexTestCursor(t, data, pages, 1536, false, CodecUncompressed, testColumnIndex(pages, true))
	held, err := scope.Reserve(memory.TagVector, (2<<20)-64)
	if err != nil {
		t.Fatal(err)
	}
	if err := cursor.ensurePageIndex(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if len(cursor.index.pages) != 0 || scope.Used() != held.Size() {
		t.Fatalf("failed index retained memory: %d", scope.Used())
	}
	held.Release()
	out, err := cursor.ReadScalar(context.Background(), 512)
	if err != nil {
		t.Fatal(err)
	}
	out.Release()
	for _, ref := range []pageIndexRef{{-1, 16}, {int64(len(driver.data)), 16}, {0, maxPageIndexBytes + 1}, {1, 16}} {
		if cursor.validIndexRef(ref) {
			t.Fatalf("unsafe index range accepted: %+v", ref)
		}
	}
}

func TestPageIndexIOErrorsAreNotSwallowed(t *testing.T) {
	for _, request := range []int{1, 2} {
		data, pages := indexTestFixture(t, false, false, CodecUncompressed)
		cursor, driver, _ := indexTestCursor(t, data, pages, 1536, false, CodecUncompressed, testColumnIndex(pages, true))
		driver.mutateAt = request
		if err := cursor.ensurePageIndex(context.Background(), true); !errors.Is(err, vfs.ErrObjectChanged) || cursor.Consumed() != 0 {
			t.Fatalf("request=%d error=%v consumed=%d", request, err, cursor.Consumed())
		}
	}
}

func TestRetainedPageMustAgreeWithOffsetIndex(t *testing.T) {
	data, pages := indexTestFixture(t, false, false, CodecUncompressed)
	pages[0].size--
	cursor, _, _ := indexTestCursor(t, data, pages, 1536, false, CodecUncompressed, nil)
	if err := cursor.ensurePageIndex(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if out, err := cursor.ReadScalar(context.Background(), 512); err == nil || out != nil {
		if out != nil {
			out.Release()
		}
		t.Fatal("retained page/index mismatch accepted")
	}
}

func TestColumnIndexNullPagesAndUnknownNullCounts(t *testing.T) {
	for _, allNull := range []bool{false, true} {
		values := []int32{-1, 1}
		if allNull {
			values[1] = -1
		}
		data := skipTestIntPage(t, values, true, true, CodecUncompressed, nil)
		bound := make([]byte, 4)
		binary.LittleEndian.PutUint32(bound, 1)
		page := indexedPage{offset: 0, firstRow: 0, size: int32(len(data)), min: bound, max: bound, nullPage: allNull}
		if allNull {
			page.min = nil
			page.max = nil
		}
		cursor, _, _ := indexTestCursor(t, data, []indexedPage{page}, 2, true, CodecUncompressed, testColumnIndex([]indexedPage{page}, false))
		op := scan.IsNull
		if allNull {
			op = scan.IsNotNull
		}
		skipped, _, err := cursor.pruneFilterPage(context.Background(), scan.Filter{Column: "x", Op: op})
		if err != nil {
			t.Fatal(err)
		}
		if allNull {
			if skipped != 2 {
				t.Fatalf("all NULL skipped=%d", skipped)
			}
		} else {
			if skipped != 0 {
				t.Fatal("missing null counts treated as zero")
			}
			out, err := cursor.ReadScalar(context.Background(), 2)
			if err != nil {
				t.Fatal(err)
			}
			if out.IsValid(0) {
				t.Fatal("NULL lost")
			}
			out.Release()
		}
	}
}

func TestIndexedSkipLoadsDictionaryOnlyForRetainedPages(t *testing.T) {
	for _, plain := range []bool{false, true} {
		words := []string{"alpha", "a dictionary value longer than inline storage"}
		cursor, _ := dictionaryBatchCursor(t, words, []dictionaryBatchTestPage{{indices: []int{0, 1, -1, 0}}, {indices: []int{1, 0, -1, 1}, plain: plain}}, false, nil)
		driver := cursor.driver.(*recordingDriver)
		var pages []indexedPage
		firstRow := int64(0)
		for offset := cursor.meta.DataPageOffset; offset < cursor.end; {
			header, length, err := DecodePageHeader(driver.data[offset:])
			if err != nil {
				t.Fatal(err)
			}
			size := int64(length) + int64(header.CompressedPageSize)
			pages = append(pages, indexedPage{offset: offset, size: int32(size), firstRow: firstRow})
			firstRow += int64(header.Data.NumValues)
			offset += size
		}
		raw := testOffsetIndex(pages)
		at := int64(len(driver.data))
		length := int32(len(raw))
		driver.data = append(driver.data, raw...)
		cursor.identity.Size = int64(len(driver.data))
		cursor.configurePageIndex(ColumnChunk{OffsetIndexOffset: &at, OffsetIndexLength: &length})
		if err := cursor.skipScalar(context.Background(), 4); err != nil {
			t.Fatal(err)
		}
		if cursor.dictionary != nil {
			t.Fatal("discarded prefix eagerly decoded dictionary")
		}
		for _, request := range driver.ranges {
			if request.Offset < at {
				t.Fatalf("skip read dictionary/data: %+v", request)
			}
		}
		out, err := cursor.ReadScalar(context.Background(), 4)
		if err != nil {
			t.Fatal(err)
		}
		assertDictionaryBatchValues(t, out, words, []int{1, 0, -1, 1})
		out.Release()
	}
}

func TestPageIndexRejectsIntegerOverflow(t *testing.T) {
	if _, err := indexI32(newCompactReader(putVarint(nil, zig(1<<40)))); err == nil {
		t.Fatal("i32 overflow accepted")
	}
	if _, err := indexI64(newCompactReader([]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 127})); err == nil {
		t.Fatal("i64 overflow accepted")
	}
}

func TestRuntimeSourceUsesIndexesDiscoveredInFooter(t *testing.T) {
	payload, pages := indexTestFixture(t, false, false, CodecUncompressed)
	data := append([]byte("PAR1"), payload...)
	for position := range pages {
		pages[position].offset += 4
	}
	offset := testOffsetIndex(pages)
	offsetAt, offsetLen := int64(len(data)), int32(len(offset))
	data = append(data, offset...)
	column := testColumnIndex(pages, true)
	columnAt, columnLen := int64(len(data)), int32(len(column))
	data = append(data, column...)
	children := int32(1)
	physical := TypeInt32
	required := Required
	metadata := FileMetaData{Version: 1, NumRows: 1536, Schema: []SchemaElement{{Name: "schema", NumChildren: &children}, {Name: "x", Type: &physical, Repetition: &required}}, RowGroups: []RowGroup{{NumRows: 1536, TotalByteSize: int64(len(payload)), Columns: []ColumnChunk{{MetaData: &ColumnMetaData{Type: physical, Codec: CodecUncompressed, Encodings: []Encoding{EncodingPlain}, NumValues: 1536, DataPageOffset: 4, TotalCompressedSize: int64(len(payload)), TotalUncompressedSize: int64(len(payload)), PathInSchema: []string{"x"}}, OffsetIndexOffset: &offsetAt, OffsetIndexLength: &offsetLen, ColumnIndexOffset: &columnAt, ColumnIndexLength: &columnLen}}}}}
	footer, err := EncodeFileMetaData(metadata)
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, footer...)
	var trailer [8]byte
	binary.LittleEndian.PutUint32(trailer[:4], uint32(len(footer)))
	copy(trailer[4:], "PAR1")
	data = append(data, trailer[:]...)
	const path = "mem://x/with-indexes.parquet"
	driver := newBindDriver(t, map[string][]byte{path: data})
	binder, _, scope := newTestBinder(t, driver)
	binding, err := binder.Bind(context.Background(), path, SourceBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewRuntimeSource(context.Background(), RuntimeSourceConfig{Binder: binder, Pattern: path, ExpectedFingerprint: binding.Fingerprint(), Pushdown: scan.Pushdown{Projection: []int{0}, Filters: []scan.IndexedFilter{{ColumnIndex: 0, Op: scan.Equal, Value: scan.Literal{Type: types.LogicalType{ID: types.Integer}, Int64: 2}}}}})
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for {
		chunk, ok, err := source.Next(context.Background())
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		for row := 0; row < chunk.Cardinality; row++ {
			value, valid := chunk.Columns[0].Int32(row)
			if !valid || value != 2 {
				t.Fatalf("value=%d/%v", value, valid)
			}
		}
		rows += chunk.Cardinality
		chunk.Release()
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if rows != 512 || scope.Used() != 0 {
		t.Fatalf("rows=%d memory=%d", rows, scope.Used())
	}
	for _, request := range driver.ranges {
		if request.Offset < pages[2].offset {
			t.Fatalf("runtime read rejected page: %+v", request)
		}
	}
}

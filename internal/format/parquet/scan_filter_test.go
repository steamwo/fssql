package parquet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/format/scan"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func TestScannerFiltersBeforePayloadDecode(t *testing.T) {
	for _, threshold := range []int64{0, 1, 100} {
		t.Run(fmt.Sprint(threshold), func(t *testing.T) {
			pageA := scannerRequiredInt32Page(0, 0, 1, 0, 1, 0)
			pageB := append(scannerRequiredInt32Page(10, 20, 30), scannerRequiredInt32Page(40, 50, 60)...)
			data := append(append([]byte(nil), pageA...), pageB...)
			a := &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 6, TotalCompressedSize: int64(len(pageA)), PathInSchema: []string{"a"}}
			b := &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 6, TotalCompressedSize: int64(len(pageB)), DataPageOffset: int64(len(pageA)), PathInSchema: []string{"b"}}
			tree := &SchemaTree{Leaves: []*SchemaNode{scannerInt32Leaf("a"), scannerInt32Leaf("b")}}
			driver := &recordingDriver{data: data}
			uri, _ := vfs.ParseURI("mem://scan/filtered")
			manager := memory.NewManager(1 << 20)
			scope := manager.NewScope()
			scanner, err := NewRowGroupScanner(driver, uri, vfs.ObjectIdentity{}, tree, RowGroup{NumRows: 6, Columns: []ColumnChunk{{MetaData: a}, {MetaData: b}}}, []string{"a", "b"}, []scan.Filter{{Column: "a", Op: scan.GreaterEqual, Value: scan.Literal{Type: types.LogicalType{ID: types.Integer}, Int64: threshold}}}, scope, memory.NewBufferManager(nil), 2)
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
				if chunk.Cardinality == 0 {
					t.Fatal("empty filtered batch")
				}
				for row := 0; row < chunk.Cardinality; row++ {
					value, valid := chunk.Columns[1].Int32(row)
					if !valid {
						t.Fatal("NULL payload")
					}
					got = append(got, value)
				}
				chunk.Release()
			}
			want := []int32{10, 20, 30, 40, 50, 60}
			if threshold == 1 {
				want = []int32{30, 50}
			}
			if threshold == 100 {
				want = nil
			}
			if len(got) != len(want) {
				t.Fatalf("got=%v want=%v", got, want)
			}
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("got=%v want=%v", got, want)
				}
			}
			payload := scanner.readers[1].Metrics()
			if payload.DataValues != int64(len(want)) {
				t.Fatalf("decoded payload=%d want=%d", payload.DataValues, len(want))
			}
			if threshold == 100 && payload.Requests != 0 {
				t.Fatalf("rejected payload performed I/O: %+v", payload)
			}
			scanner.Close()
			if scope.Used() != 0 {
				t.Fatalf("leak=%d", scope.Used())
			}
		})
	}
}

func BenchmarkScannerSelectivePayload(b *testing.B) {
	const rows = vector.DefaultVectorSize
	ids := make([]int32, rows)
	var payload []byte
	for row := range ids {
		ids[row] = int32(row % 100)
		payload = append(payload, byteArrayValue("a wide string payload that must be owned after the scan batch has been released")...)
	}
	pageA := scannerRequiredInt32Page(ids...)
	pageB := append(cursorDataPageHeader(rows, len(payload), EncodingPlain), payload...)
	data := append(append([]byte(nil), pageA...), pageB...)
	leaf := optionalVarcharLeaf()
	leaf.Name, leaf.Path, leaf.MaxDef = "payload", []string{"payload"}, 0
	required := Required
	leaf.Element.Repetition = &required
	tree := &SchemaTree{Leaves: []*SchemaNode{scannerInt32Leaf("id"), leaf}}
	rg := RowGroup{NumRows: rows, Columns: []ColumnChunk{
		{MetaData: &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: rows, TotalCompressedSize: int64(len(pageA)), PathInSchema: []string{"id"}}},
		{MetaData: &ColumnMetaData{Type: TypeByteArray, Codec: CodecUncompressed, NumValues: rows, DataPageOffset: int64(len(pageA)), TotalCompressedSize: int64(len(pageB)), PathInSchema: []string{"payload"}}},
	}}
	filter := scan.Filter{Column: "id", Op: scan.Equal, Value: scan.Literal{Type: types.LogicalType{ID: types.Integer}, Int64: 0}}
	uri, _ := vfs.ParseURI("mem://benchmark/selective")
	for _, early := range []bool{false, true} {
		b.Run(map[bool]string{false: "residual_only", true: "early_filter"}[early], func(b *testing.B) {
			manager := memory.NewManager(8 << 20)
			scope := manager.NewScope()
			buffers := memory.NewBufferManager(nil)
			selectRows := compileScanFilter(tree.Leaves[0].Logical, filter)
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				scanner, err := NewRowGroupScanner(&recordingDriver{data: data}, uri, vfs.ObjectIdentity{}, tree, rg, []string{"id", "payload"}, []scan.Filter{filter}, scope, buffers, rows)
				if err != nil {
					b.Fatal(err)
				}
				if !early {
					scanner.filters = nil
				}
				chunk, err := scanner.Next(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				selection := make([]int, chunk.Cardinality)
				for row := range selection {
					selection[row] = row
				}
				selection = selectRows(chunk.Columns[0], selection) // The residual Filter remains in both plans.
				for _, column := range chunk.Columns {
					out, err := vector.CopySelection(scope, buffers, column, selection)
					if err != nil {
						b.Fatal(err)
					}
					out.Release()
				}
				if len(selection) != 21 {
					b.Fatalf("selected=%d", len(selection))
				}
				chunk.Release()
				scanner.Close()
				if scope.Used() != 0 {
					b.Fatalf("leak=%d", scope.Used())
				}
			}
		})
	}
}

func TestSelectedScalarAcrossNullsPagesAndEncodings(t *testing.T) {
	for _, plain := range []bool{false, true} {
		t.Run(map[bool]string{false: "dictionary", true: "plain"}[plain], func(t *testing.T) {
			dictionary := []string{"alpha", "a payload longer than inline storage", "gamma"}
			cursor, _ := dictionaryBatchCursor(t, dictionary, []dictionaryBatchTestPage{{indices: []int{0, -1, 1, 2}, plain: plain}, {indices: []int{-1, 2, 0, 1}, plain: plain}}, false, nil)
			if _, err := cursor.PeekEntry(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := cursor.skipScalar(context.Background(), 2); err != nil {
				t.Fatal(err)
			}
			out, err := cursor.readSelected(context.Background(), 6, []int{0, 2, 3, 5})
			if err != nil {
				t.Fatal(err)
			}
			assertDictionaryBatchValues(t, out, dictionary, []int{1, -1, 2, 1})
			out.Release()
			if cursor.Consumed() != 8 {
				t.Fatalf("consumed=%d", cursor.Consumed())
			}
			if err := cursor.skipScalar(context.Background(), 1); err == nil {
				t.Fatal("accepted out-of-bounds skip")
			}
		})
	}
}

func TestCompiledScanFilterNullsAndConjunction(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	typ := types.LogicalType{ID: types.Integer}
	v, err := vector.NewFlat(scope, memory.NewBufferManager(nil), typ, 4)
	if err != nil {
		t.Fatal(err)
	}
	v.SetInt32(0, 1)
	v.SetNull(1)
	v.SetInt32(2, 3)
	v.SetInt32(3, 4)
	rows := []int{0, 1, 2, 3}
	rows = compileScanFilter(typ, scan.Filter{Op: scan.GreaterThan, Value: scan.Literal{Type: typ, Int64: 1}})(v, rows)
	rows = compileScanFilter(typ, scan.Filter{Op: scan.LessThan, Value: scan.Literal{Type: typ, Int64: 4}})(v, rows)
	if len(rows) != 1 || rows[0] != 2 {
		t.Fatalf("selection=%v", rows)
	}
	rows = compileScanFilter(typ, scan.Filter{Op: scan.IsNull})(v, []int{0, 1, 2, 3})
	if len(rows) != 1 || rows[0] != 1 {
		t.Fatalf("null selection=%v", rows)
	}
	v.Release()
	if scope.Used() != 0 {
		t.Fatalf("leak=%d", scope.Used())
	}
}

func TestCompiledScanFilterTimestampAndDate(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	typ := types.LogicalType{ID: types.TimestampType, Timestamp: types.TimestampInfo{Unit: types.TimestampMicros}}
	v, err := vector.NewFlat(scope, buffers, typ, 4)
	if err != nil {
		t.Fatal(err)
	}
	v.SetInt64(0, 500)
	v.SetNull(1)
	v.SetInt64(2, 1500)
	v.SetInt64(3, 2500)
	rows := []int{0, 1, 2, 3}
	lower := compileScanFilter(typ, scan.Filter{Op: scan.GreaterEqual, Value: scan.Literal{Type: typ, Int64: 1000}})
	upper := compileScanFilter(typ, scan.Filter{Op: scan.LessThan, Value: scan.Literal{Type: typ, Int64: 2000}})
	if lower == nil || upper == nil {
		t.Fatal("timestamp range was not compiled")
	}
	rows = lower(v, rows)
	rows = upper(v, rows)
	if len(rows) != 1 || rows[0] != 2 {
		t.Fatalf("timestamp selection=%v", rows)
	}
	millis := types.LogicalType{ID: types.TimestampType, Timestamp: types.TimestampInfo{Unit: types.TimestampMillis}}
	rows = compileScanFilter(typ, scan.Filter{Op: scan.GreaterEqual, Value: scan.Literal{Type: millis, Int64: 1}})(v, []int{0, 1, 2, 3})
	if len(rows) != 2 || rows[0] != 2 || rows[1] != 3 {
		t.Fatalf("unit conversion selection=%v", rows)
	}
	v.Release()

	dateType := types.LogicalType{ID: types.DateType}
	dates, err := vector.NewFlat(scope, buffers, dateType, 3)
	if err != nil {
		t.Fatal(err)
	}
	dates.SetInt32(0, 19000)
	dates.SetInt32(1, 19500)
	dates.SetInt32(2, 20000)
	rows = compileScanFilter(dateType, scan.Filter{Op: scan.GreaterEqual, Value: scan.Literal{Type: dateType, Int64: 19500}})(dates, []int{0, 1, 2})
	if len(rows) != 2 || rows[0] != 1 || rows[1] != 2 {
		t.Fatalf("date selection=%v", rows)
	}
	dates.Release()
	if scope.Used() != 0 {
		t.Fatalf("leak=%d", scope.Used())
	}
}

func TestSelectedScalarCancellationAndTruncation(t *testing.T) {
	cursor, _ := dictionaryBatchCursor(t, []string{"x"}, []dictionaryBatchTestPage{{indices: []int{0, 0, 0}}}, false, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cursor.skipScalar(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if cursor.Consumed() != 0 {
		t.Fatal("cancellation consumed rows")
	}
	stream := newValueStream([]byte{5, 0, 0, 0, 'a'}, TypeByteArray, nil)
	if err := stream.skip(1); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncation err=%v", err)
	}
}

func TestFilteredScannerRetainsNestedFallbackAlignment(t *testing.T) {
	_, leaf := standardOptionalInt32ListSchema()
	ids := scannerRequiredInt32Page(0, 0, 1, 0)
	list := standardListDataPage()
	data := append(append([]byte(nil), ids...), list...)
	tree := &SchemaTree{Leaves: []*SchemaNode{scannerInt32Leaf("id"), leaf}}
	rg := RowGroup{NumRows: 4, Columns: []ColumnChunk{
		{MetaData: &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 4, TotalCompressedSize: int64(len(ids)), PathInSchema: []string{"id"}}},
		{MetaData: &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 6, TotalCompressedSize: int64(len(list)), DataPageOffset: int64(len(ids)), PathInSchema: leaf.Path}},
	}}
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	uri, _ := vfs.ParseURI("mem://scan/nested-filter")
	scanner, err := NewRowGroupScanner(&recordingDriver{data: data}, uri, vfs.ObjectIdentity{}, tree, rg, []string{"items", "id"}, []scan.Filter{{Column: "id", Op: scan.Equal, Value: scan.Literal{Type: types.LogicalType{ID: types.Integer}, Int64: 1}}}, scope, memory.NewBufferManager(nil), 2)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := scanner.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	entry, valid := chunk.Columns[0].ListEntry(0)
	if chunk.Cardinality != 1 || !valid || entry.Length != 3 {
		t.Fatalf("cardinality=%d entry=%+v/%v", chunk.Cardinality, entry, valid)
	}
	base, _, dictionary := chunk.Columns[0].Dictionary()
	if !dictionary {
		t.Fatal("expected owned nested selection")
	}
	if base.Children()[0].IsValid(int(entry.Offset)) {
		t.Fatal("nested NULL lost")
	}
	chunk.Release()
	if _, err := scanner.Next(context.Background()); err != io.EOF {
		t.Fatalf("EOF=%v", err)
	}
	scanner.Close()
	if scope.Used() != 0 {
		t.Fatalf("leak=%d", scope.Used())
	}
}

func TestScanFilterFloatingAndTextSemantics(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	for _, typ := range []types.LogicalType{{ID: types.Double}, {ID: types.Varchar}, {ID: types.Boolean}} {
		v, err := vector.NewFlat(scope, memory.NewBufferManager(nil), typ, 3)
		if err != nil {
			t.Fatal(err)
		}
		literal := scan.Literal{Type: typ}
		switch typ.ID {
		case types.Double:
			v.SetFloat64(0, 1)
			v.SetFloat64(1, math.NaN())
			literal.Float64 = math.NaN()
		case types.Varchar:
			if err := v.SetString(0, "alpha"); err != nil {
				t.Fatal(err)
			}
			if err := v.SetString(1, "beta"); err != nil {
				t.Fatal(err)
			}
			literal.String = "beta"
		case types.Boolean:
			v.SetBool(0, false)
			v.SetBool(1, true)
			literal.Bool = true
		}
		v.SetNull(2)
		rows := compileScanFilter(typ, scan.Filter{Op: scan.Equal, Value: literal})(v, []int{0, 1, 2})
		if len(rows) != 1 || rows[0] != 1 {
			t.Fatalf("type=%v selected=%v", typ, rows)
		}
		v.Release()
	}
	if scope.Used() != 0 {
		t.Fatalf("leak=%d", scope.Used())
	}
}

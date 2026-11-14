package json

import (
	"fmt"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
)

func TestProjectedPlanNestedListsAndLiteralNames(t *testing.T) {
	schema := []types.StructField{
		{Name: "a.b", Type: types.LogicalType{ID: types.BigInt}},
		{Name: "items", Type: types.NewList(types.NewStruct(
			types.StructField{Name: "keep", Type: types.NewList(types.LogicalType{ID: types.BigInt})},
			types.StructField{Name: "skip", Type: types.LogicalType{ID: types.BigInt}},
		))},
		{Name: "skip", Type: types.LogicalType{ID: types.BigInt}},
	}
	projection := ProjectedPathSet{paths: [][]string{{"a.b"}, {"items", "keep"}}}
	scope := memory.NewManager(1 << 20).NewScope()
	chunk, err := DecodeRecords([][]byte{
		[]byte(`{"a.b":7,"items":[{"keep":[1,null,3],"skip":"unselected"},{"keep":[],"skip":{}}],"skip":true}`),
		[]byte(`{"items":[{"keep":[4]}]}`),
	}, schema, projection, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := chunk.Columns[0].Int64(0); !ok || got != 7 || chunk.Columns[0].IsValid(1) {
		t.Fatalf("literal field=%d valid=%v", got, ok)
	}
	items := chunk.Columns[1]
	entry, valid := items.ListEntry(0)
	if !valid || entry.Offset != 0 || entry.Length != 2 {
		t.Fatalf("items=%+v valid=%v", entry, valid)
	}
	fields := items.Children()[0].Children()
	for row, want := range []uint32{3, 0, 1} {
		entry, ok := fields[0].ListEntry(row)
		if !ok || entry.Length != want || fields[1].IsValid(row) {
			t.Fatalf("nested row=%d entry=%+v valid=%v", row, entry, ok)
		}
	}
	values := fields[0].Children()[0]
	for row, want := range []int64{1, 0, 3, 4} {
		got, valid := values.Int64(row)
		if valid != (row != 1) || valid && got != want {
			t.Fatalf("value %d=%d valid=%v", row, got, valid)
		}
	}
	if chunk.Columns[2].IsValid(0) || chunk.Columns[2].IsValid(1) {
		t.Fatal("unselected root field must be NULL")
	}
	chunk.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed leak=%d", scope.Used())
	}
}

func BenchmarkWideProjectedDecodeRecords(b *testing.B) {
	for _, columns := range []int{6, 64} {
		b.Run(fmt.Sprint(columns), func(b *testing.B) {
			schema := wideDecodeSchema(columns)
			projection := ProjectedPathSet{}
			var record strings.Builder
			record.WriteByte('{')
			for i := columns - 1; i >= 0; i-- {
				if i != columns-1 {
					record.WriteByte(',')
				}
				fmt.Fprintf(&record, "%q:%d", schema[i].Name, i)
				projection.paths = append(projection.paths, []string{schema[i].Name})
			}
			record.WriteByte('}')
			records := make([][]byte, 1024)
			for i := range records {
				records[i] = []byte(record.String())
			}
			scope := memory.NewManager(16 << 20).NewScope()
			buffers := memory.NewBufferManager(nil)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				chunk, err := DecodeRecords(records, schema, projection, scope, buffers)
				if err != nil {
					b.Fatal(err)
				}
				chunk.Release()
			}
			b.StopTimer()
			if scope.Used() != 0 {
				b.Fatalf("managed leak=%d", scope.Used())
			}
		})
	}
}

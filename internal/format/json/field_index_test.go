package json

import (
	"fmt"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
)

func wideDecodeSchema(columns int) []types.StructField {
	fields := make([]types.StructField, columns)
	for i := range fields {
		fields[i] = types.StructField{Name: fmt.Sprintf("c%02d", i), Type: types.LogicalType{ID: types.BigInt}}
	}
	return fields
}

func TestWideFieldLookup(t *testing.T) {
	schema := wideDecodeSchema(64)
	schema[1].Name = "中文"
	// The nested object has the same width and overlapping names as the root.
	nested := wideDecodeSchema(64)
	schema[63].Type = types.NewStruct(nested...)
	for _, tc := range []struct {
		name, record string
		wantErr      bool
	}{
		{"reordered", `{"unknown":[{"x":1}],"c63":{"c00":9,"c62":8},"c62":62,"\u4e2d\u6587":1,"c00":0}`, false},
		{"duplicate escaped key", `{"c00":0,"\u006300":1}`, true},
		{"duplicate nested key", `{"c63":{"c00":0,"\u006300":1}}`, true},
		{"invalid escaped key", `{"c\q":1}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mm := memory.NewManager(1 << 20)
			scope := mm.NewScope()
			chunk, err := DecodeRecords([][]byte{[]byte(tc.record), []byte(`{}`)}, schema, ProjectedPathSet{}, scope, memory.NewBufferManager(nil))
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v wantErr=%v", err, tc.wantErr)
			}
			if chunk != nil {
				for _, col := range []int{0, 1, 62} {
					want := int64(col)
					if got, ok := chunk.Columns[col].Int64(0); !ok || got != want {
						t.Fatalf("column=%d got=%d valid=%v", col, got, ok)
					}
				}
				children := chunk.Columns[63].Children()
				if got, ok := children[0].Int64(0); !ok || got != 9 {
					t.Fatalf("nested value=%d valid=%v", got, ok)
				}
				for _, col := range chunk.Columns {
					if col.IsValid(1) {
						t.Fatal("missing field must be NULL")
					}
				}
				chunk.Release()
			}
			if scope.Used() != 0 {
				t.Fatalf("managed leak=%d", scope.Used())
			}
		})
	}
}

func TestWideFieldLookupBudgetFallback(t *testing.T) {
	scope := memory.NewManager(2000).NewScope()
	chunk, err := DecodeRecords([][]byte{[]byte(`{"c63":63,"c00":0}`)}, wideDecodeSchema(64), ProjectedPathSet{}, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := chunk.Columns[63].Int64(0); !ok || got != 63 {
		t.Fatalf("value=%d valid=%v", got, ok)
	}
	chunk.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed leak=%d", scope.Used())
	}
}

func BenchmarkWideDecodeRecords(b *testing.B) {
	for _, columns := range []int{6, 64} {
		b.Run(fmt.Sprint(columns), func(b *testing.B) {
			schema := wideDecodeSchema(columns)
			var record strings.Builder
			record.WriteByte('{')
			for i := columns - 1; i >= 0; i-- {
				if i != columns-1 {
					record.WriteByte(',')
				}
				fmt.Fprintf(&record, "%q:%d", schema[i].Name, i)
			}
			record.WriteByte('}')
			records := make([][]byte, 1024)
			for i := range records {
				records[i] = []byte(record.String())
			}
			mm := memory.NewManager(16 << 20)
			scope := mm.NewScope()
			bm := memory.NewBufferManager(nil)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				chunk, err := DecodeRecords(records, schema, ProjectedPathSet{}, scope, bm)
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

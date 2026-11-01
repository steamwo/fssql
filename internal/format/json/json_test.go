package json

import (
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
)

func jsonEnv(t *testing.T) (*memory.Manager, *memory.Scope, *memory.BufferManager) {
	t.Helper()
	m := memory.NewManager(8 << 20)
	return m, m.NewScope(), memory.NewBufferManager(nil)
}

func TestTokenizerStructural(t *testing.T) {
	raw := []byte(`{"a":[1,true,null,"x\\\"y"]}`)
	tok := NewTokenizer(raw)
	var count int
	for {
		v, err := tok.Next()
		if err != nil {
			t.Fatal(err)
		}
		count++
		if v.Kind == TokenEOF {
			break
		}
	}
	if count < 10 {
		t.Fatalf("token count=%d", count)
	}
}

func TestProjectedPathSet(t *testing.T) {
	p := NewProjectedPathSet("user.id", "name")
	if !p.Includes("user") || !p.Includes("name") || p.Includes("age") {
		t.Fatal("projection mismatch")
	}
}

func TestTypedDecodeNestedListStruct(t *testing.T) {
	m, scope, bm := jsonEnv(t)
	fields := []types.StructField{
		{Name: "id", Type: types.LogicalType{ID: types.BigInt}},
		{Name: "name", Type: types.LogicalType{ID: types.Varchar}},
		{Name: "tags", Type: types.NewList(types.LogicalType{ID: types.Varchar})},
		{Name: "meta", Type: types.NewStruct(
			types.StructField{Name: "active", Type: types.LogicalType{ID: types.Boolean}},
			types.StructField{Name: "score", Type: types.LogicalType{ID: types.Double}},
		)},
	}
	records := [][]byte{
		[]byte(`{"id":1,"name":"a","tags":["x","y"],"meta":{"active":true,"score":1.5}}`),
		[]byte(`{"id":2,"name":"b","tags":[],"meta":null}`),
	}
	chunk, err := DecodeRecords(records, fields, nil, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	if x, _ := chunk.Columns[0].Int64(1); x != 2 {
		t.Fatal(x)
	}
	if x, _ := chunk.Columns[1].String(0); x != "a" {
		t.Fatal(x)
	}
	entry, ok := chunk.Columns[2].ListEntry(0)
	if !ok || entry.Length != 2 {
		t.Fatalf("entry=%+v valid=%v", entry, ok)
	}
	if x, _ := chunk.Columns[2].Children()[0].String(1); x != "y" {
		t.Fatal(x)
	}
	if chunk.Columns[3].IsValid(1) {
		t.Fatal("expected null struct")
	}
	active, _ := chunk.Columns[3].Children()[0].Bool(0)
	if !active {
		t.Fatal("active=false")
	}
	chunk.Release()
	if got := m.Used(); got != 0 {
		t.Fatalf("managed leak=%d", got)
	}
}

func TestProjectionSkipsUnselectedTypedDecode(t *testing.T) {
	m, scope, bm := jsonEnv(t)
	fields := []types.StructField{
		{Name: "id", Type: types.LogicalType{ID: types.BigInt}},
		{Name: "bad", Type: types.LogicalType{ID: types.BigInt}},
	}
	chunk, err := DecodeRecords([][]byte{[]byte(`{"id":7,"bad":"not-int"}`)}, fields, NewProjectedPathSet("id"), scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunk.Columns) != 1 {
		t.Fatalf("columns=%d", len(chunk.Columns))
	}
	chunk.Release()
	if got := m.Used(); got != 0 {
		t.Fatalf("managed leak=%d", got)
	}
}

func TestMalformedJSONAndAllocationFailureNoLeak(t *testing.T) {
	m := memory.NewManager(16)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	_, err := DecodeRecords([][]byte{[]byte(`{"id":1`)}, []types.StructField{{Name: "id", Type: types.LogicalType{ID: types.BigInt}}}, nil, scope, bm)
	if err == nil {
		t.Fatal("expected error")
	}
	if got := m.Used(); got != 0 {
		t.Fatalf("managed leak=%d", got)
	}
}

func TestInferSchemaNested(t *testing.T) {
	schema, err := InferSchema([][]byte{
		[]byte(`{"id":1,"v":1.5,"tags":[1,2],"meta":{"ok":true}}`),
		[]byte(`{"id":2,"v":2,"tags":[3.5],"meta":{"ok":false,"name":"x"}}`),
	}, 10)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]types.LogicalType{}
	for _, f := range schema.Fields {
		got[f.Name] = f.Type
	}
	if got["v"].ID != types.Double || got["tags"].ID != types.List || got["tags"].Child.ID != types.Double || got["meta"].ID != types.Struct {
		t.Fatalf("schema=%+v", got)
	}
}

func BenchmarkTypedDecode(b *testing.B) {
	m := memory.NewManager(1 << 30)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	fields := []types.StructField{
		{Name: "id", Type: types.LogicalType{ID: types.BigInt}},
		{Name: "name", Type: types.LogicalType{ID: types.Varchar}},
	}
	records := make([][]byte, 2048)
	for i := range records {
		records[i] = []byte(`{"id":123,"name":"abcdefghijklmno"}`)
	}
	b.SetBytes(int64(len(records) * len(records[0])))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		chunk, err := DecodeRecords(records, fields, nil, scope, bm)
		if err != nil {
			b.Fatal(err)
		}
		chunk.Release()
	}
}

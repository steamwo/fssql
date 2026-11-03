package json

import (
	"context"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func TestScalarProjectionKeepsPositionsAndValidatesUnreadJSON(t *testing.T) {
	m := memory.NewManager(1 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	projection, _ := NewProjectedPathSet("id")
	records := [][]byte{[]byte(`{"id":7,"profile":{"name":"alpha","score":1.5},"tags":["a","b"]}`), []byte(`{"profile":{},"tags":[]}`)}
	chunk, err := DecodeRecords(records, nestedSchema(), projection, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunk.Columns) != 3 || chunk.Cardinality != 2 {
		t.Fatal("projection changed relation indexes")
	}
	if id, valid := chunk.Columns[0].Int64(0); !valid || id != 7 || chunk.Columns[0].IsValid(1) {
		t.Fatal("selected/missing field decoded incorrectly")
	}
	for _, column := range chunk.Columns[1:] {
		if column.Kind != vector.Constant || column.IsValid(0) || len(column.Children()) != 0 {
			t.Fatal("unread nested column allocated payload")
		}
	}
	chunk.Release()
	for _, raw := range []string{`{"id":1,"tags":[1,]}`, `{"id":1,"profile":{"x":}}`, `{"id":1,"tags":["bad\q"]}`, `{"id":1,"tags":[],"tags":[]}`, `{"id":1} true`} {
		out, err := DecodeRecords([][]byte{[]byte(raw)}, nestedSchema(), projection, scope, bm)
		if out != nil {
			out.Release()
		}
		if err == nil {
			t.Fatalf("invalid unread JSON accepted: %s", raw)
		}
	}
	if m.Used() != 0 {
		t.Fatalf("managed leak=%d", m.Used())
	}
}

func TestProjectedListRequiresOnlySelectedCountPass(t *testing.T) {
	typ := types.NewStruct(types.StructField{Name: "name", Type: types.LogicalType{ID: types.Varchar}}, types.StructField{Name: "items", Type: types.NewList(types.LogicalType{ID: types.BigInt})})
	for _, tc := range []struct {
		path string
		want bool
	}{{"profile.name", false}, {"profile.items", true}, {"profile", true}, {"other", false}} {
		projection, _ := NewProjectedPathSet(tc.path)
		if got := projectedList(typ, projection, []string{"profile"}); got != tc.want {
			t.Fatalf("path=%s needsCount=%v", tc.path, got)
		}
	}
	m := memory.NewManager(1 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	schema := []types.StructField{{Name: "profile", Type: typ}}
	records := [][]byte{[]byte(`{"profile":{"name":"x","items":[1,2,3]}}`), []byte(`{"profile":{"name":"y","items":[4]}}`)}
	for _, path := range []string{"profile.name", "profile.items"} {
		projection, _ := NewProjectedPathSet(path)
		out, err := DecodeRecords(records, schema, projection, scope, bm)
		if err != nil {
			t.Fatal(err)
		}
		children := out.Columns[0].Children()
		if path == "profile.items" {
			entry, valid := children[1].ListEntry(1)
			value, valueValid := children[1].Children()[0].Int64(int(entry.Offset))
			if !valid || entry.Length != 1 || !valueValid || value != 4 {
				t.Fatal("selected nested list lost element offsets")
			}
		} else if name, valid := children[0].String(1); !valid || name != "y" || children[1].IsValid(1) {
			t.Fatal("selected nested scalar decoded incorrectly")
		}
		out.Release()
	}
	if m.Used() != 0 {
		t.Fatalf("managed leak=%d", m.Used())
	}
}

func TestNDJSONRuntimeIndexedProjectionKeepsLiteralNames(t *testing.T) {
	driver := newMem(t, []byte("{\"ignore\":[1,2],\"a.b\":17,\"tail\":\"x\"}\n{\"ignore\":[],\"a.b\":23,\"tail\":\"y\"}\n"), vfs.Capabilities(vfs.RandomRead))
	registry, _ := vfs.NewRegistry(driver)
	m := memory.NewManager(4 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	binder, err := NewNDJSONBinder(registry, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := binder.Bind(context.Background(), driver.uri.String(), NDJSONBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	fields := binding.Schema().Fields
	selected := -1
	for index, field := range fields {
		if field.Name == "a.b" {
			selected = index
		}
	}
	if selected < 0 {
		t.Fatal("literal dotted name missing from binding")
	}
	cfg := NDJSONRuntimeSourceConfig{Binder: binder, Pattern: driver.uri.String(), ExpectedFingerprint: binding.Fingerprint(), Projection: []int{selected}}
	source, err := NewNDJSONRuntimeSource(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	chunk, ok, err := source.Next(context.Background())
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if len(chunk.Columns) != len(fields) || chunk.Cardinality != 2 {
		t.Fatal("runtime projection changed schema cardinality")
	}
	if got, valid := chunk.Columns[selected].Int64(1); !valid || got != 23 {
		t.Fatalf("literal dotted field=%d/%v", got, valid)
	}
	for index, column := range chunk.Columns {
		if index != selected && (column.Kind != vector.Constant || column.IsValid(0)) {
			t.Fatal("runtime failed to push projection")
		}
	}
	chunk.Release()
	if out, ok, err := source.Next(context.Background()); err != nil || ok || out != nil {
		t.Fatalf("EOF ok=%v err=%v", ok, err)
	}
	cfg.Projection = []int{len(fields)}
	if out, err := NewNDJSONRuntimeSource(context.Background(), cfg); err == nil || out != nil {
		t.Fatal("out of range projection accepted")
	}
	if m.Used() != 0 {
		t.Fatalf("managed leak=%d", m.Used())
	}
}

func BenchmarkNDJSONScalarProjection(b *testing.B) {
	records := make([][]byte, 1024)
	payload := strings.Repeat("payload", 32)
	for row := range records {
		records[row] = []byte(`{"id":42,"profile":{"name":"` + payload + `","score":1.25},"tags":["a","b"]}`)
	}
	m := memory.NewManager(16 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	for _, full := range []bool{false, true} {
		projection, _ := NewProjectedPathSet("id")
		name := "id"
		if full {
			projection, name = ProjectedPathSet{}, "all"
		}
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(records) * len(records[0])))
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				out, err := DecodeRecords(records, nestedSchema(), projection, scope, bm)
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
	}
}

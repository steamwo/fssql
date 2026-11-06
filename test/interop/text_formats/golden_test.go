package text_formats

import (
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	csvformat "github.com/steamwo/fssql/internal/format/csv"
	jsonformat "github.com/steamwo/fssql/internal/format/json"
)

// These fixtures and expected results are frozen from DuckDB v2.0.0's own
// regression corpus. They provide an offline compatibility oracle when a
// DuckDB executable is unavailable in the test environment.
func TestDuckDBV200GoldenNDJSONExample(t *testing.T) {
	raw := []byte("{\"id\":1,\"name\":\"O Brother, Where Art Thou?\"}\n" +
		"{\"id\":2,\"name\":\"Home for the Holidays\"}\n" +
		"{\"id\":3,\"name\":\"The Firm\"}\n" +
		"{\"id\":4,\"name\":\"Broadcast News\"}\n" +
		"{\"id\":5,\"name\":\"Raising Arizona\"}")
	records, consumed, err := jsonformat.FrameNDJSON(raw, true, 2048)
	if err != nil {
		t.Fatal(err)
	}
	if consumed != len(raw) || len(records) != 5 {
		t.Fatalf("framing consumed=%d rows=%d", consumed, len(records))
	}
	schema := []types.StructField{
		{Name: "id", Type: types.LogicalType{ID: types.BigInt}},
		{Name: "name", Type: types.LogicalType{ID: types.Varchar}},
	}
	m := memory.NewManager(1 << 20)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	chunk, err := jsonformat.DecodeRecords(records, schema, jsonformat.ProjectedPathSet{}, s, bm)
	if err != nil {
		t.Fatal(err)
	}
	names := []string{"O Brother, Where Art Thou?", "Home for the Holidays", "The Firm", "Broadcast News", "Raising Arizona"}
	for i := range names {
		id, ok := chunk.Columns[0].Int64(i)
		if !ok || id != int64(i+1) {
			t.Fatalf("row %d id=%d valid=%v", i, id, ok)
		}
		name, ok := chunk.Columns[1].String(i)
		if !ok || name != names[i] {
			t.Fatalf("row %d name=%q valid=%v", i, name, ok)
		}
	}
	chunk.Release()
	if s.Used() != 0 {
		t.Fatalf("managed leak=%d", s.Used())
	}
}

func TestDuckDBV200GoldenJSONListInference(t *testing.T) {
	raw := []byte("{\"id\":1,\"name\":[\"O\",\"Brother,\",\"Where\",\"Art\",\"Thou?\"]}\n" +
		"{\"id\":2,\"name\":[\"Home\",\"for\",\"the\",\"Holidays\"]}\n" +
		"{\"id\":3,\"name\":[\"The\",\"Firm\"]}\n" +
		"{\"id\":4,\"name\":[\"Broadcast\",\"News\"]}\n" +
		"{\"id\":5,\"name\":[\"Raising\",\"Arizona\"]}\n")
	records, _, err := jsonformat.FrameNDJSON(raw, true, 2048)
	if err != nil {
		t.Fatal(err)
	}
	inferred, err := jsonformat.InferRecords(records, 2048)
	if err != nil {
		t.Fatal(err)
	}
	if len(inferred.Fields) != 2 {
		t.Fatalf("fields=%d", len(inferred.Fields))
	}
	if inferred.Fields[0].Name != "id" || inferred.Fields[0].Type.ID != types.BigInt {
		t.Fatalf("id field=%+v", inferred.Fields[0])
	}
	list := inferred.Fields[1]
	if list.Name != "name" || list.Type.ID != types.List || list.Type.Child == nil || list.Type.Child.ID != types.Varchar {
		t.Fatalf("name field=%+v", list)
	}
}

func TestDuckDBV200GoldenCSVQuotedNewline(t *testing.T) {
	// Body of DuckDB's csv_quoted_newline_after_boundary regression fixture,
	// after its auto-detected header row "a,b".
	body := strings.Repeat("y", 26) + ",1\n" +
		strings.Repeat("z", 31) + ",2\n" +
		"\"\nab\",3\n\"c\",4\n"
	m := memory.NewManager(1 << 20)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	schema := []types.LogicalType{{ID: types.Varchar}, {ID: types.BigInt}}
	chunk, consumed, err := csvformat.DecodeChunk([]byte(body), schema, csvformat.DecodeOptions{Final: true}, s, bm)
	if err != nil {
		t.Fatal(err)
	}
	if consumed != len(body) || chunk.Cardinality != 4 {
		t.Fatalf("consumed=%d/%d rows=%d", consumed, len(body), chunk.Cardinality)
	}
	a3, ok := chunk.Columns[0].String(2)
	if !ok || a3 != "\nab" {
		t.Fatalf("row b=3 a=%q valid=%v", a3, ok)
	}
	b3, ok := chunk.Columns[1].Int64(2)
	if !ok || b3 != 3 {
		t.Fatalf("row b=3 value=%d valid=%v", b3, ok)
	}
	a4, ok := chunk.Columns[0].String(3)
	if !ok || a4 != "c" {
		t.Fatalf("row b=4 a=%q valid=%v", a4, ok)
	}
	chunk.Release()
	if s.Used() != 0 {
		t.Fatalf("managed leak=%d", s.Used())
	}
}

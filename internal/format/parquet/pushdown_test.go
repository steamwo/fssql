package parquet

import (
	"encoding/binary"
	"testing"

	coretypes "github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/format/scan"
)

func simpleTree(t *testing.T) *SchemaTree {
	t.Helper()
	n := int32(2)
	r := Required
	i64 := TypeInt64
	str := TypeByteArray
	utf := ConvertedUTF8
	flat := []SchemaElement{{Name: "schema", NumChildren: &n}, {Name: "id", Repetition: &r, Type: &i64}, {Name: "name", Repetition: &r, Type: &str, Converted: &utf}}
	tree, e := BuildSchemaTree(flat)
	if e != nil {
		t.Fatal(e)
	}
	return tree
}
func i64b(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}
func TestProjectionSelectsOnlyRequestedColumn(t *testing.T) {
	tree := simpleTree(t)
	rg := RowGroup{Columns: []ColumnChunk{{MetaData: &ColumnMetaData{PathInSchema: []string{"id"}}}, {MetaData: &ColumnMetaData{PathInSchema: []string{"name"}}}}}
	p, e := PlanProjection(tree, rg, []string{"name"})
	if e != nil {
		t.Fatal(e)
	}
	if len(p.ColumnIndexes) != 1 || p.ColumnIndexes[0] != 1 {
		t.Fatalf("plan=%+v", p)
	}
}
func TestPruningTriState(t *testing.T) {
	tree := simpleTree(t)
	zero := int64(0)
	rg := RowGroup{NumRows: 100, Columns: []ColumnChunk{{MetaData: &ColumnMetaData{PathInSchema: []string{"id"}, Statistics: &Statistics{MinValue: i64b(10), MaxValue: i64b(20), NullCount: &zero}}}}}
	typ := coretypes.LogicalType{ID: coretypes.BigInt}
	cases := []struct {
		op   scan.CompareOp
		v    int64
		want scan.Truth
	}{{scan.Equal, 5, scan.AlwaysFalse}, {scan.Equal, 15, scan.MaybeTrue}, {scan.GreaterThan, 30, scan.AlwaysFalse}, {scan.LessThan, 30, scan.AlwaysTrue}}
	for _, c := range cases {
		got := EvaluateRowGroup(tree, rg, []scan.Filter{{Column: "id", Op: c.op, Value: scan.Literal{Type: typ, Int64: c.v}}}).Truth
		if got != c.want {
			t.Fatalf("op=%v v=%d got=%v want=%v", c.op, c.v, got, c.want)
		}
	}
}
func TestNullPruning(t *testing.T) {
	tree := simpleTree(t)
	n := int64(100)
	rg := RowGroup{NumRows: 100, Columns: []ColumnChunk{{MetaData: &ColumnMetaData{PathInSchema: []string{"id"}, Statistics: &Statistics{NullCount: &n}}}}}
	if got := EvaluateRowGroup(tree, rg, []scan.Filter{{Column: "id", Op: scan.IsNotNull}}).Truth; got != scan.AlwaysFalse {
		t.Fatalf("got=%v", got)
	}
}
func TestResolveSchemasUnionByName(t *testing.T) {
	a := simpleTree(t)
	r := Required
	i64 := TypeInt64
	str := TypeByteArray
	utf := ConvertedUTF8
	n := int32(3)
	flat := []SchemaElement{{Name: "schema", NumChildren: &n}, {Name: "name", Repetition: &r, Type: &str, Converted: &utf}, {Name: "id", Repetition: &r, Type: &i64}, {Name: "extra", Repetition: &r, Type: &i64}}
	b, e := BuildSchemaTree(flat)
	if e != nil {
		t.Fatal(e)
	}
	if _, e := ResolveSchemas([]*SchemaTree{a, b}, false); e == nil {
		t.Fatal("expected strict mismatch")
	}
	u, e := ResolveSchemas([]*SchemaTree{a, b}, true)
	if e != nil {
		t.Fatal(e)
	}
	if len(u.Fields) != 3 || !u.UnionByName {
		t.Fatalf("union=%+v", u)
	}
}

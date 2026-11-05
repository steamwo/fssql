package result

import (
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// TestDuckDBTypeName 覆盖 Stable scalar、DECIMAL、LIST 与 STRUCT 文本。
func TestDuckDBTypeName(t *testing.T) {
	decimal, err := types.NewDecimal(18, 3)
	if err != nil {
		t.Fatal(err)
	}
	nested := types.NewList(types.NewStruct(
		types.StructField{Name: "id", Type: types.LogicalType{ID: types.BigInt}},
		types.StructField{Name: "x space", Type: decimal},
	))
	cases := []struct {
		typ  types.LogicalType
		want string
	}{
		{types.LogicalType{ID: types.Boolean}, "BOOLEAN"},
		{types.LogicalType{ID: types.HugeInt}, "HUGEINT"},
		{decimal, "DECIMAL(18,3)"},
		{types.BlobType(), "BLOB"},
		{types.UTinyIntType(), "UTINYINT"},
		{types.USmallIntType(), "USMALLINT"},
		{types.UIntegerType(), "UINTEGER"},
		{types.UBigIntType(), "UBIGINT"},
		{types.TimestampNSType(), "TIMESTAMP_NS"},
		{types.TimestampTZType(), "TIMESTAMP WITH TIME ZONE"},
		{types.NewList(types.BlobType()), "BLOB[]"},
		{types.NewStruct(types.StructField{Name: "instant", Type: types.TimestampTZType()}), "STRUCT(instant TIMESTAMP WITH TIME ZONE)"},
		{nested, `STRUCT(id BIGINT, "x space" DECIMAL(18,3))[]`},
	}
	for _, tc := range cases {
		got, err := DuckDBTypeName(tc.typ)
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Fatalf("type=%+v got=%q want=%q", tc.typ, got, tc.want)
		}
	}
}

// TestDuckDBTypeNameRejectsMalformedNestedType 确保 malformed LogicalType 不会被格式化成伪合法 SQL type。
func TestDuckDBTypeNameRejectsMalformedNestedType(t *testing.T) {
	if _, err := DuckDBTypeName(types.LogicalType{ID: types.List}); err == nil {
		t.Fatal("expected missing LIST child error")
	}
	if _, err := DuckDBTypeName(types.LogicalType{ID: types.Decimal, Decimal: types.DecimalInfo{Width: 2, Scale: 3}}); err == nil {
		t.Fatal("expected invalid decimal error")
	}
}

// TestDescribeCursorMaterializesSixColumns 验证 DESCRIBE 只物化 schema，不执行 Query，并正确使用 SQL NULL metadata 字段。
func TestDescribeCursorMaterializesSixColumns(t *testing.T) {
	stmt := &binder.BoundDescribe{Columns: []binder.RelationColumn{
		{Name: "id", Type: types.LogicalType{ID: types.BigInt}, Nullable: false},
		{Name: "name", Type: types.LogicalType{ID: types.Varchar}, Nullable: true},
	}}
	cursor, err := NewDescribeCursor(stmt)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	chunk, done, err := cursor.Next(scope, buffers)
	if err != nil {
		t.Fatal(err)
	}
	if done || chunk == nil || chunk.Cardinality != 2 || len(chunk.Columns) != 6 {
		t.Fatalf("done=%v chunk=%+v", done, chunk)
	}
	defer func() {
		chunk.Release()
		if err := scope.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	wantNames := []string{"id", "name"}
	wantTypes := []string{"BIGINT", "VARCHAR"}
	for row := 0; row < 2; row++ {
		name, ok := chunk.Columns[0].String(row)
		if !ok || name != wantNames[row] {
			t.Fatalf("row=%d name=%q ok=%v", row, name, ok)
		}
		typeName, ok := chunk.Columns[1].String(row)
		if !ok || typeName != wantTypes[row] {
			t.Fatalf("row=%d type=%q ok=%v", row, typeName, ok)
		}
		nullable, ok := chunk.Columns[2].String(row)
		if !ok || nullable != "YES" {
			t.Fatalf("row=%d null=%q ok=%v", row, nullable, ok)
		}
		for col := 3; col < 6; col++ {
			if chunk.Columns[col].IsValid(row) {
				t.Fatalf("row=%d col=%d expected SQL NULL", row, col)
			}
		}
	}
	if next, done, err := cursor.Next(scope, buffers); err != nil || !done || next != nil {
		t.Fatalf("next=%v done=%v err=%v", next, done, err)
	}
}

// TestDescribeSchemaIsImmutableCopy 防止调用方修改稳定输出 schema。
func TestDescribeSchemaIsImmutableCopy(t *testing.T) {
	a := DescribeSchema()
	b := DescribeSchema()
	a[0].Name = "mutated"
	if b[0].Name != "column_name" {
		t.Fatalf("schema alias leaked: %+v", b)
	}
}

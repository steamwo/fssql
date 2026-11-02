package binder

import (
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/ast"
)

func id(name string) ast.Identifier { return ast.Identifier{Name: name, Raw: name} }

func q(parts ...string) ast.QualifiedName {
	ids := make([]ast.Identifier, len(parts))
	for i, p := range parts {
		ids[i] = id(p)
	}
	return ast.QualifiedName{Parts: ids}
}

func name(parts ...string) *ast.NameExpr { return &ast.NameExpr{Name: q(parts...)} }

func lit(k ast.LiteralKind, v string) *ast.LiteralExpr { return &ast.LiteralExpr{Kind: k, Value: v} }

func TestScopeResolutionAndAmbiguity(t *testing.T) {
	s := Scope{Columns: []ColumnBinding{
		{Table: "a", Name: "id", Type: types.LogicalType{ID: types.Integer}, Index: 0},
		{Table: "b", Name: "id", Type: types.LogicalType{ID: types.BigInt}, Index: 1},
	}}
	if _, err := s.Resolve(q("id")); err == nil {
		t.Fatal("expected ambiguity")
	}
	c, err := s.Resolve(q("b", "id"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Index != 1 {
		t.Fatalf("index=%d", c.Index)
	}
}

func TestBindNumericPromotionAndDivision(t *testing.T) {
	b := New(nil, Scope{Columns: []ColumnBinding{
		{Name: "a", Type: types.LogicalType{ID: types.Integer}, Index: 0},
		{Name: "b", Type: types.LogicalType{ID: types.BigInt}, Index: 1},
	}})
	e, err := b.BindExpression(&ast.BinaryExpr{Left: name("a"), Op: "+", Right: name("b")})
	if err != nil {
		t.Fatal(err)
	}
	if e.Type.ID != types.BigInt || e.Children[0].Kind != BoundCast {
		t.Fatalf("add=%+v left=%+v", e, e.Children[0])
	}
	e, err = b.BindExpression(&ast.BinaryExpr{Left: name("a"), Op: "/", Right: name("b")})
	if err != nil {
		t.Fatal(err)
	}
	if e.Type.ID != types.Double || e.Children[0].Type.ID != types.Double || e.Children[1].Type.ID != types.Double {
		t.Fatalf("division=%+v", e)
	}
}

func TestBindNullUsesContextButNeverCoreSQLNullType(t *testing.T) {
	b := New(nil, Scope{})
	raw, err := b.BindExpression(lit(ast.LiteralNull, "NULL"))
	if err != nil {
		t.Fatal(err)
	}
	if !raw.SQLNull || raw.Type.ID != types.Invalid {
		t.Fatalf("raw=%+v", raw)
	}
	e, err := b.BindExpression(&ast.BinaryExpr{Left: lit(ast.LiteralNull, "NULL"), Op: "=", Right: lit(ast.LiteralInteger, "1")})
	if err != nil {
		t.Fatal(err)
	}
	if e.Type.ID != types.Boolean || e.Children[0].SQLNull || e.Children[0].Type.ID != types.Integer {
		t.Fatalf("comparison=%+v left=%+v", e, e.Children[0])
	}
}

func TestBindAggregateFunctionThroughCatalog(t *testing.T) {
	b := New(function.NewBuiltinCatalog(), Scope{Columns: []ColumnBinding{{Name: "v", Type: types.LogicalType{ID: types.BigInt}, Index: 0}}})
	e, err := b.BindExpression(&ast.CallExpr{Function: q("sum"), Args: []ast.Expr{name("v")}})
	if err != nil {
		t.Fatal(err)
	}
	if e.Kind != BoundFunction || e.FunctionKind != function.Aggregate || e.FunctionID != function.SumID || e.Type.ID != types.HugeInt {
		t.Fatalf("sum=%+v", e)
	}
	c, err := b.BindExpression(&ast.CallExpr{Function: q("count"), Star: true})
	if err != nil {
		t.Fatal(err)
	}
	if c.Type.ID != types.BigInt || c.Nullable {
		t.Fatalf("count=%+v", c)
	}
}

func TestBindRejectsTableFunctionInScalarPosition(t *testing.T) {
	b := New(nil, Scope{})
	_, err := b.BindExpression(&ast.CallExpr{Function: q("read_parquet"), Args: []ast.Expr{lit(ast.LiteralString, "x.parquet")}})
	if err == nil {
		t.Fatal("expected table function error")
	}
}

func TestBindBooleanAndTypeErrors(t *testing.T) {
	b := New(nil, Scope{Columns: []ColumnBinding{{Name: "v", Type: types.LogicalType{ID: types.Varchar}, Index: 0}}})
	if _, err := b.BindExpression(&ast.BinaryExpr{Left: name("v"), Op: "+", Right: lit(ast.LiteralInteger, "1")}); err == nil {
		t.Fatal("expected numeric error")
	}
	e, err := b.BindExpression(&ast.BinaryExpr{Left: lit(ast.LiteralNull, "NULL"), Op: "AND", Right: lit(ast.LiteralBool, "TRUE")})
	if err != nil {
		t.Fatal(err)
	}
	if e.Children[0].Type.ID != types.Boolean || e.Children[0].SQLNull {
		t.Fatalf("and=%+v", e)
	}
}

func TestBindLiteralTypes(t *testing.T) {
	b := New(nil, Scope{})
	cases := []struct {
		expr ast.Expr
		want types.LogicalTypeID
	}{
		{lit(ast.LiteralInteger, "1"), types.Integer},
		{lit(ast.LiteralInteger, "2147483648"), types.BigInt},
		{lit(ast.LiteralString, "x"), types.Varchar},
		{lit(ast.LiteralDecimal, "12.30"), types.Decimal},
	}
	for _, tc := range cases {
		x, err := b.BindExpression(tc.expr)
		if err != nil {
			t.Fatal(err)
		}
		if x.Type.ID != tc.want {
			t.Fatalf("got %v want %v", x.Type.ID, tc.want)
		}
	}
}

func TestScopeHonorsQuotedIdentifierCase(t *testing.T) {
	s := Scope{Columns: []ColumnBinding{{Name: "MiX", Type: types.LogicalType{ID: types.Integer}, Index: 0}}}
	quoted := ast.QualifiedName{Parts: []ast.Identifier{{Name: "mix", Quoted: true}}}
	if _, err := s.Resolve(quoted); err == nil {
		t.Fatal("quoted identifier must be case sensitive")
	}
	quoted.Parts[0].Name = "MiX"
	if _, err := s.Resolve(quoted); err != nil {
		t.Fatal(err)
	}
}

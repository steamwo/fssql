package session

import (
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	formatscan "github.com/steamwo/fssql/internal/format/scan"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func TestTranslateParquetPushdownFlattensSafeConjunctions(t *testing.T) {
	columnID := &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: 0}
	columnGroup := &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.Integer}, ColumnIndex: 1}
	lowerLiteral := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Integer}, Literal: "500000"}
	lowerCast := &binder.BoundExpression{Kind: binder.BoundCast, Type: types.LogicalType{ID: types.BigInt}, Children: []*binder.BoundExpression{lowerLiteral}}
	upperLiteral := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Integer}, Literal: "501000"}
	upperCast := &binder.BoundExpression{Kind: binder.BoundCast, Type: types.LogicalType{ID: types.BigInt}, Children: []*binder.BoundExpression{upperLiteral}}
	lower := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: ">=", Children: []*binder.BoundExpression{columnID, lowerCast}}
	upper := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: "<", Children: []*binder.BoundExpression{columnID, upperCast}}
	nullCheck := &binder.BoundExpression{Kind: binder.BoundIsNull, Type: types.LogicalType{ID: types.Boolean}, Op: "IS NOT NULL", Children: []*binder.BoundExpression{columnGroup}}
	and := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: "AND", Children: []*binder.BoundExpression{
		lower,
		&binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: "AND", Children: []*binder.BoundExpression{upper, nullCheck}},
	}}

	out := translateParquetPushdown(planner.ScanPushdown{Projection: []int{0, 2}, Filters: []*binder.BoundExpression{and}, Limit: 100})
	if out.Limit != 100 || len(out.Projection) != 2 || out.Projection[0] != 0 || out.Projection[1] != 2 {
		t.Fatalf("structural pushdown=%+v", out)
	}
	if len(out.Filters) != 3 {
		t.Fatalf("filters=%+v", out.Filters)
	}
	if out.Filters[0].ColumnIndex != 0 || out.Filters[0].Op != formatscan.GreaterEqual || out.Filters[0].Value.Int64 != 500000 || out.Filters[0].Value.Type.ID != types.BigInt {
		t.Fatalf("lower=%+v", out.Filters[0])
	}
	if out.Filters[1].ColumnIndex != 0 || out.Filters[1].Op != formatscan.LessThan || out.Filters[1].Value.Int64 != 501000 {
		t.Fatalf("upper=%+v", out.Filters[1])
	}
	if out.Filters[2].ColumnIndex != 1 || out.Filters[2].Op != formatscan.IsNotNull {
		t.Fatalf("null=%+v", out.Filters[2])
	}
}

func TestTranslateParquetPushdownReversesLiteralColumnComparison(t *testing.T) {
	literal := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Integer}, Literal: "7"}
	column := &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.Integer}, ColumnIndex: 2}
	expression := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: "<", Children: []*binder.BoundExpression{literal, column}}
	out := translateParquetPushdown(planner.ScanPushdown{Filters: []*binder.BoundExpression{expression}})
	if len(out.Filters) != 1 || out.Filters[0].ColumnIndex != 2 || out.Filters[0].Op != formatscan.GreaterThan || out.Filters[0].Value.Int64 != 7 {
		t.Fatalf("filters=%+v", out.Filters)
	}
}

func TestTranslateParquetPushdownKeepsUnsupportedOrResidualOnly(t *testing.T) {
	left := &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.Integer}, ColumnIndex: 0}
	right := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Integer}, Literal: "1"}
	comparison := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: "=", Children: []*binder.BoundExpression{left, right}}
	or := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: "OR", Children: []*binder.BoundExpression{comparison, comparison}}
	out := translateParquetPushdown(planner.ScanPushdown{Filters: []*binder.BoundExpression{or}})
	if len(out.Filters) != 0 {
		t.Fatalf("OR must remain residual only: %+v", out.Filters)
	}
}

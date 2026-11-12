package session

import (
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/types"
	formatscan "github.com/steamwo/fssql/internal/format/scan"
	"github.com/steamwo/fssql/internal/function"
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

func TestParquetPushdownRejectsLossyColumnAndLiteralCasts(t *testing.T) {
	integer := types.LogicalType{ID: types.Integer}
	bigint := types.LogicalType{ID: types.BigInt}
	text := types.LogicalType{ID: types.Varchar}
	for _, pair := range [][2]types.LogicalType{{bigint, integer}, {integer, text}, {bigint, {ID: types.Double}}} {
		column := &binder.BoundExpression{Kind: binder.BoundColumn, Type: pair[0], ColumnIndex: 0}
		cast := &binder.BoundExpression{Kind: binder.BoundCast, Type: pair[1], Children: []*binder.BoundExpression{column}}
		if _, ok := parquetColumnIndex(cast); ok {
			t.Fatalf("unsafe column cast accepted: %+v", pair)
		}
		literal := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: pair[0], Literal: "1"}
		cast.Children = []*binder.BoundExpression{literal}
		if _, ok := parquetLiteral(cast); ok {
			t.Fatalf("unsafe literal cast accepted: %+v", pair)
		}
	}
	column := &binder.BoundExpression{Kind: binder.BoundColumn, Type: integer, ColumnIndex: 2}
	cast := &binder.BoundExpression{Kind: binder.BoundCast, Type: bigint, Children: []*binder.BoundExpression{column}}
	if index, ok := parquetColumnIndex(cast); !ok || index != 2 {
		t.Fatalf("safe widening rejected: %d/%v", index, ok)
	}
}

func TestParquetPushdownAcceptsTemporalLiteralCasts(t *testing.T) {
	catalog := function.NewQueryCatalog()
	timestampCast, ok := catalog.Lookup("__fssql_cast_timestamp")
	if !ok {
		t.Fatal("timestamp cast function missing")
	}
	dateCast, ok := catalog.Lookup("__fssql_cast_date")
	if !ok {
		t.Fatal("date cast function missing")
	}
	text := "2024-07-01 00:00:00"
	literal := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Varchar}, Literal: text}
	wantTimestamp := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	for _, cast := range []*binder.BoundExpression{
		{Kind: binder.BoundFunction, FunctionKind: function.Scalar, FunctionID: timestampCast.ID, Type: types.LogicalType{ID: types.TimestampType}, Children: []*binder.BoundExpression{literal}},
		{Kind: binder.BoundCast, Type: types.LogicalType{ID: types.TimestampType}, Children: []*binder.BoundExpression{literal}},
	} {
		out, ok := parquetLiteral(cast)
		if !ok || out.Type.ID != types.TimestampType || out.Int64 != wantTimestamp {
			t.Fatalf("timestamp literal=%+v ok=%v want=%d", out, ok, wantTimestamp)
		}
	}
	dateLiteral := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Varchar}, Literal: "2024-07-01"}
	dateExpression := &binder.BoundExpression{Kind: binder.BoundFunction, FunctionKind: function.Scalar, FunctionID: dateCast.ID, Type: types.LogicalType{ID: types.DateType}, Children: []*binder.BoundExpression{dateLiteral}}
	out, ok := parquetLiteral(dateExpression)
	wantDate := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC).Unix() / 86400
	if !ok || out.Type.ID != types.DateType || out.Int64 != wantDate {
		t.Fatalf("date literal=%+v ok=%v want=%d", out, ok, wantDate)
	}
	badLiteral := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Varchar}, Literal: "not-a-time"}
	bad := &binder.BoundExpression{Kind: binder.BoundFunction, FunctionKind: function.Scalar, FunctionID: timestampCast.ID, Type: types.LogicalType{ID: types.TimestampType}, Children: []*binder.BoundExpression{badLiteral}}
	if _, ok := parquetLiteral(bad); ok {
		t.Fatal("invalid timestamp text accepted")
	}
}

func TestParquetPushdownKeepsTryCastResidual(t *testing.T) {
	catalog := function.NewQueryCatalog()
	def, ok := catalog.Lookup("__fssql_try_cast_timestamp")
	if !ok {
		t.Fatal("missing TRY_CAST")
	}
	literal := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Varchar}, Literal: "not a timestamp"}
	expr := &binder.BoundExpression{Kind: binder.BoundFunction, FunctionKind: function.Scalar, FunctionID: def.ID, Type: types.LogicalType{ID: types.TimestampType}, Children: []*binder.BoundExpression{literal}}
	if _, ok := parquetCastChild(expr); ok {
		t.Fatal("TRY_CAST treated as throwing CAST")
	}
	if _, ok := parquetLiteral(expr); ok {
		t.Fatal("TRY_CAST was folded by CAST pushdown")
	}
}

func TestParquetTimestampOffsetLiteralSemantics(t *testing.T) {
	text := "2024-07-01T00:00:00.123456789+08:30"
	base := time.Date(2024, 7, 1, 0, 0, 0, 123456789, time.UTC)
	for _, typ := range []types.LogicalType{types.TimestampNSType(), types.TimestampTZType()} {
		want := base.UnixNano()
		if typ.Timestamp.AdjustedToUTC {
			want = base.Add(-8*time.Hour - 30*time.Minute).UnixMicro()
		}
		got, ok := parquetTemporalLiteral(text, typ)
		if !ok || got != want {
			t.Fatalf("type=%v got=%d want=%d ok=%v", typ, got, want, ok)
		}
	}
	if _, ok := parquetTemporalLiteral("2263-01-01", types.TimestampNSType()); ok {
		t.Fatal("overflow accepted for pruning")
	}
}

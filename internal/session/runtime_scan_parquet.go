package session

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/format/parquet"
	formatscan "github.com/steamwo/fssql/internal/format/scan"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/semantics"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// NewParquetScanOpenAdapter 把 Stable read_parquet ScanSpec 接到真实 Parquet RuntimeSource。
func NewParquetScanOpenAdapter(sourceBinder *parquet.SourceBinder) (ScanOpenAdapter, error) {
	if sourceBinder == nil {
		return ScanOpenAdapter{}, fmt.Errorf("session: read_parquet runtime scan requires SourceBinder")
	}
	return ScanOpenAdapter{
		FunctionID: function.ReadParquetID,
		OpenCount: func(ctx context.Context, spec planner.ScanSpec) (pipeline.Source, error) {
			return parquet.NewMetadataCountSource(ctx, parquet.RuntimeSourceConfig{
				Binder: sourceBinder, Pattern: spec.Source, ExpectedFingerprint: string(spec.Fingerprint),
			})
		},
		OpenStats: func(ctx context.Context, spec planner.ScanSpec, aggregates []planner.AggregateSpec) (pipeline.Source, bool, error) {
			if len(spec.Pushdown.Filters) != 0 || spec.Pushdown.Limit != 0 {
				return nil, false, nil
			}
			requests := make([]parquet.StatsRequest, 0, len(aggregates))
			for _, aggregate := range aggregates {
				id := function.ID(aggregate.FunctionID)
				if id != function.MinID && id != function.MaxID {
					return nil, false, nil
				}
				if len(aggregate.Arguments) != 1 {
					return nil, false, nil
				}
				argument := aggregate.Arguments[0]
				if argument == nil || argument.Kind != binder.BoundColumn || argument.ColumnIndex < 0 {
					return nil, false, nil
				}
				kind := parquet.StatsMin
				if id == function.MaxID {
					kind = parquet.StatsMax
				}
				requests = append(requests, parquet.StatsRequest{ColumnIndex: argument.ColumnIndex, Kind: kind, OutputType: aggregate.Output.Type})
			}
			return parquet.NewMetadataStatsSource(ctx, parquet.RuntimeSourceConfig{
				Binder: sourceBinder, Pattern: spec.Source, ExpectedFingerprint: string(spec.Fingerprint),
			}, requests)
		},
		Open: func(ctx context.Context, spec planner.ScanSpec) (pipeline.Source, error) {
			if spec.Function != "read_parquet" {
				return nil, fmt.Errorf("session: read_parquet runtime adapter received function %q", spec.Function)
			}
			return parquet.NewRuntimeSource(ctx, parquet.RuntimeSourceConfig{
				Binder:              sourceBinder,
				Pattern:             spec.Source,
				ExpectedFingerprint: string(spec.Fingerprint),
				Pushdown:            translateParquetPushdown(spec.Pushdown),
			})
		},
	}, nil
}

// translateParquetPushdown keeps the planner/format boundary generic. Projection
// and LIMIT are already structural candidates; filters are lowered only when a
// conjunction leaf can be proven equivalent to one metadata comparison. Every
// unsupported predicate remains as a residual physical Filter.
func translateParquetPushdown(in planner.ScanPushdown) formatscan.Pushdown {
	out := formatscan.Pushdown{
		Projection: append([]int(nil), in.Projection...),
		Limit:      in.Limit,
	}
	for _, expression := range in.Filters {
		appendParquetFilterConjuncts(&out.Filters, expression)
	}
	return out
}

func appendParquetFilterConjuncts(out *[]formatscan.IndexedFilter, expression *binder.BoundExpression) {
	if expression == nil {
		return
	}
	if expression.Kind == binder.BoundBinary && strings.EqualFold(expression.Op, "AND") && len(expression.Children) == 2 {
		appendParquetFilterConjuncts(out, expression.Children[0])
		appendParquetFilterConjuncts(out, expression.Children[1])
		return
	}
	if filter, ok := parquetFilterConjunct(expression); ok {
		*out = append(*out, filter)
	}
}

func parquetFilterConjunct(expression *binder.BoundExpression) (formatscan.IndexedFilter, bool) {
	if expression == nil {
		return formatscan.IndexedFilter{}, false
	}
	if expression.Kind == binder.BoundIsNull && len(expression.Children) == 1 {
		column, ok := parquetColumnIndex(expression.Children[0])
		if !ok {
			return formatscan.IndexedFilter{}, false
		}
		op := formatscan.IsNull
		if strings.EqualFold(expression.Op, "IS NOT NULL") {
			op = formatscan.IsNotNull
		} else if !strings.EqualFold(expression.Op, "IS NULL") {
			return formatscan.IndexedFilter{}, false
		}
		return formatscan.IndexedFilter{ColumnIndex: column, Op: op}, true
	}
	if expression.Kind != binder.BoundBinary || len(expression.Children) != 2 {
		return formatscan.IndexedFilter{}, false
	}
	op, ok := parquetCompareOp(expression.Op)
	if !ok {
		return formatscan.IndexedFilter{}, false
	}
	if column, columnOK := parquetColumnIndex(expression.Children[0]); columnOK {
		literal, literalOK := parquetLiteral(expression.Children[1])
		if !literalOK {
			return formatscan.IndexedFilter{}, false
		}
		return formatscan.IndexedFilter{ColumnIndex: column, Op: op, Value: literal}, true
	}
	if column, columnOK := parquetColumnIndex(expression.Children[1]); columnOK {
		literal, literalOK := parquetLiteral(expression.Children[0])
		if !literalOK {
			return formatscan.IndexedFilter{}, false
		}
		return formatscan.IndexedFilter{ColumnIndex: column, Op: reverseParquetCompareOp(op), Value: literal}, true
	}
	return formatscan.IndexedFilter{}, false
}

func parquetColumnIndex(expression *binder.BoundExpression) (int, bool) {
	if expression == nil {
		return 0, false
	}
	for expression.Kind == binder.BoundCast && len(expression.Children) == 1 {
		if !parquetSafeCast(expression.Children[0].Type, expression.Type) {
			return 0, false
		}
		expression = expression.Children[0]
	}
	if expression.Kind != binder.BoundColumn || expression.ColumnIndex < 0 {
		return 0, false
	}
	return expression.ColumnIndex, true
}

func parquetCompareOp(op string) (formatscan.CompareOp, bool) {
	switch strings.ToUpper(strings.TrimSpace(op)) {
	case "=":
		return formatscan.Equal, true
	case "<>":
		return formatscan.NotEqual, true
	case "<":
		return formatscan.LessThan, true
	case "<=":
		return formatscan.LessEqual, true
	case ">":
		return formatscan.GreaterThan, true
	case ">=":
		return formatscan.GreaterEqual, true
	default:
		return 0, false
	}
}

func reverseParquetCompareOp(op formatscan.CompareOp) formatscan.CompareOp {
	switch op {
	case formatscan.LessThan:
		return formatscan.GreaterThan
	case formatscan.LessEqual:
		return formatscan.GreaterEqual
	case formatscan.GreaterThan:
		return formatscan.LessThan
	case formatscan.GreaterEqual:
		return formatscan.LessEqual
	default:
		return op
	}
}

func parquetLiteral(expression *binder.BoundExpression) (formatscan.Literal, bool) {
	if expression == nil {
		return formatscan.Literal{}, false
	}
	target := expression.Type
	literal := expression
	for {
		child, isCast := parquetCastChild(literal)
		if !isCast {
			break
		}
		if child.Kind == binder.BoundNull {
			return formatscan.Literal{Type: target, Null: true}, true
		}
		if parquetSafeCast(child.Type, literal.Type) {
			literal = child
			continue
		}
		// Text -> DATE/TIMESTAMP is exact when the canonical CAST text parses;
		// without this the predicate stays residual and re-casts every row.
		if (literal.Type.ID == types.DateType || literal.Type.ID == types.TimestampType) &&
			child.Kind == binder.BoundLiteral && child.Type.ID == types.Varchar {
			if raw, ok := parquetTemporalLiteral(child.Literal, literal.Type); ok {
				return formatscan.Literal{Type: literal.Type, Int64: raw}, true
			}
		}
		return formatscan.Literal{}, false
	}
	if literal.Kind == binder.BoundNull {
		return formatscan.Literal{Type: target, Null: true}, true
	}
	if literal.Kind != binder.BoundLiteral {
		return formatscan.Literal{}, false
	}
	out := formatscan.Literal{Type: target}
	switch target.ID {
	case types.Boolean:
		value, err := strconv.ParseBool(strings.ToLower(literal.Literal))
		if err != nil {
			return formatscan.Literal{}, false
		}
		out.Bool = value
	case types.TinyInt, types.SmallInt, types.Integer, types.BigInt:
		value, err := strconv.ParseInt(literal.Literal, 10, 64)
		if err != nil {
			return formatscan.Literal{}, false
		}
		out.Int64 = value
	case types.Float, types.Double:
		value, err := strconv.ParseFloat(literal.Literal, 64)
		if err != nil {
			return formatscan.Literal{}, false
		}
		out.Float64 = value
	case types.Varchar:
		out.String = literal.Literal
	case types.DateType, types.TimestampType:
		value, ok := parquetTemporalLiteral(literal.Literal, target)
		if !ok {
			return formatscan.Literal{}, false
		}
		out.Int64 = value
	default:
		// Parquet statistics decoding does not currently provide exact safe
		// comparisons for DECIMAL/HUGEINT/nested values; keep them residual.
		return formatscan.Literal{}, false
	}
	return out, true
}

func parquetCastChild(expression *binder.BoundExpression) (*binder.BoundExpression, bool) {
	if expression!=nil&&(expression.Type.ID==types.TimestampType||expression.Type.ID==types.DateType)&&expression.TimeZoneName!=""&&expression.TimeZoneName!="UTC" {return nil,false}
	if expression == nil || len(expression.Children) != 1 {
		return nil, false
	}
	if expression.Kind == binder.BoundCast {
		return expression.Children[0], true
	}
	if expression.Kind == binder.BoundFunction && expression.FunctionKind == function.Scalar &&
		expression.FunctionID >= function.FirstCastFunctionID && expression.FunctionID < function.FirstTryCastFunctionID {
		return expression.Children[0], true
	}
	return nil, false
}

func parquetTemporalLiteral(text string, target types.LogicalType) (int64, bool) {
	switch target.ID {
	case types.DateType:
		value, err := semantics.ParseDateLiteral(text)
		return value, err == nil
	case types.TimestampType:
		value, err := semantics.ParseTimestampText(text, target.Timestamp.Unit, target.Timestamp.AdjustedToUTC)
		return value, err == nil
	default:
		return 0, false
	}
}

// Stripping a cast is safe only when it preserves every input value exactly.
// Narrowing, text/numeric and lossy floating conversions remain residual.
func parquetSafeCast(from, to types.LogicalType) bool {
	if from.Same(to) {
		return true
	}
	if from.IsUnsignedInteger() || to.IsUnsignedInteger() {
		return false
	}
	rank := func(t types.LogicalType) int {
		switch t.ID {
		case types.TinyInt:
			return 1
		case types.SmallInt:
			return 2
		case types.Integer:
			return 3
		case types.BigInt:
			return 4
		}
		return 0
	}
	a, b := rank(from), rank(to)
	return (a > 0 && b > a) || (from.ID == types.Float && to.ID == types.Double)
}

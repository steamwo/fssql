package expression

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/semantics"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// ExecuteSQL 执行 SQL Binder 的强类型表达式。
// 已有 Phase 0 kernel 优先通过 LowerSQLExpression 复用；缺失但已在本文件实现的 typed kernel 直接执行。
func (e *Executor) ExecuteSQL(expr *binder.BoundExpression, chunk *vector.DataChunk) (*vector.Vector, error) {
	if e == nil || e.Scope == nil || e.Buffers == nil {
		return nil, fmt.Errorf("SQL expression executor requires managed memory")
	}
	if expr == nil || chunk == nil {
		return nil, fmt.Errorf("SQL expression executor requires expression and chunk")
	}
	if expr.Kind == binder.BoundRuntimeConstant {
		return e.executeSQLRuntimeConstant(expr, chunk.Cardinality)
	}
	if out, handled, err := e.executeBatchComparison(expr, chunk); handled {
		return out, err
	}
	if lowered, err := LowerSQLExpression(expr); err == nil {
		return e.Execute(lowered, chunk)
	}

	switch expr.Kind {
	case binder.BoundLiteral:
		return e.executeSQLLiteral(expr, chunk.Cardinality)
	case binder.BoundNull:
		return e.executeSQLNull(expr, chunk.Cardinality)
	case binder.BoundUnary:
		return e.executeSQLUnary(expr, chunk)
	case binder.BoundBinary:
		return e.executeSQLBinary(expr, chunk)
	case binder.BoundCast:
		return e.executeSQLCast(expr, chunk)
	default:
		return nil, fmt.Errorf("SQL expression executor: no exact kernel for kind %d type %d op %q", expr.Kind, expr.Type.ID, expr.Op)
	}
}

// executeSQLLiteral 物化 Binder 已确定类型的 INTEGER/VARCHAR/DOUBLE literal；BIGINT/BOOLEAN 已由旧 executor 处理。
func (e *Executor) executeSQLLiteral(expr *binder.BoundExpression, count int) (*vector.Vector, error) {
	out, err := vector.NewConstant(e.Scope, e.Buffers, expr.Type, count)
	if err != nil {
		return nil, err
	}
	switch expr.Type.ID {
	case types.Integer:
		value, parseErr := strconv.ParseInt(expr.Literal, 10, 32)
		if parseErr != nil {
			out.Release()
			return nil, fmt.Errorf("SQL expression executor: invalid INTEGER literal %q", expr.Literal)
		}
		out.SetInt32(0, int32(value))
	case types.Varchar:
		if err := out.SetString(0, expr.Literal); err != nil {
			out.Release()
			return nil, err
		}
	case types.Double:
		value, parseErr := strconv.ParseFloat(expr.Literal, 64)
		if parseErr != nil {
			out.Release()
			return nil, fmt.Errorf("SQL expression executor: invalid DOUBLE literal %q", expr.Literal)
		}
		out.SetFloat64(0, value)
	default:
		out.Release()
		return nil, fmt.Errorf("SQL expression executor: literal kernel for type %d is not implemented", expr.Type.ID)
	}
	return out, nil
}

// executeSQLNull 物化已被 Binder 消解到真实 LogicalType 的 SQL NULL constant。
func (e *Executor) executeSQLNull(expr *binder.BoundExpression, count int) (*vector.Vector, error) {
	if expr.Type.ID == types.Invalid {
		return nil, fmt.Errorf("SQL expression executor: unresolved SQL NULL type")
	}
	out, err := vector.NewConstant(e.Scope, e.Buffers, expr.Type, count)
	if err != nil {
		return nil, err
	}
	out.SetNull(0)
	return out, nil
}

// executeSQLUnary 执行 INTEGER/DOUBLE 的一元正负号；BOOLEAN NOT 等已有 kernel 继续由旧 executor 处理。
func (e *Executor) executeSQLUnary(expr *binder.BoundExpression, chunk *vector.DataChunk) (*vector.Vector, error) {
	if len(expr.Children) != 1 {
		return nil, fmt.Errorf("SQL expression executor: unary %q requires one child", expr.Op)
	}
	child, err := e.ExecuteSQL(expr.Children[0], chunk)
	if err != nil {
		return nil, err
	}
	defer child.Release()
	out, err := vector.NewFlat(e.Scope, e.Buffers, expr.Type, chunk.Cardinality)
	if err != nil {
		return nil, err
	}
	switch expr.Type.ID {
	case types.Integer:
		for row := 0; row < chunk.Cardinality; row++ {
			value, ok := child.Int32(row)
			if !ok {
				out.SetNull(row)
				continue
			}
			switch expr.Op {
			case "+":
				out.SetInt32(row, value)
			case "-":
				if value == math.MinInt32 {
					out.Release()
					return nil, fmt.Errorf("SQL expression executor: INTEGER negation overflow")
				}
				out.SetInt32(row, -value)
			default:
				out.Release()
				return nil, fmt.Errorf("SQL expression executor: unsupported INTEGER unary operator %q", expr.Op)
			}
		}
	case types.Double:
		for row := 0; row < chunk.Cardinality; row++ {
			value, ok := child.Float64(row)
			if !ok {
				out.SetNull(row)
				continue
			}
			switch expr.Op {
			case "+":
				out.SetFloat64(row, value)
			case "-":
				out.SetFloat64(row, -value)
			default:
				out.Release()
				return nil, fmt.Errorf("SQL expression executor: unsupported DOUBLE unary operator %q", expr.Op)
			}
		}
	default:
		out.Release()
		return nil, fmt.Errorf("SQL expression executor: unary numeric kernel for type %d is not implemented", expr.Type.ID)
	}
	return out, nil
}

// executeSQLCast 补齐 legacy executor 尚未覆盖的 Binder numeric widening；不实现 narrowing 或字符串转换。
func (e *Executor) executeSQLCast(expr *binder.BoundExpression, chunk *vector.DataChunk) (*vector.Vector, error) {
	if len(expr.Children) != 1 {
		return nil, fmt.Errorf("SQL expression executor: CAST requires one child")
	}
	childExpr := expr.Children[0]
	child, err := e.ExecuteSQL(childExpr, chunk)
	if err != nil {
		return nil, err
	}
	defer child.Release()
	out, err := vector.NewFlat(e.Scope, e.Buffers, expr.Type, chunk.Cardinality)
	if err != nil {
		return nil, err
	}
	switch {
	case childExpr.Type.ID == types.Integer && expr.Type.ID == types.BigInt:
		for row := 0; row < chunk.Cardinality; row++ {
			value, ok := child.Int32(row)
			if !ok {
				out.SetNull(row)
				continue
			}
			out.SetInt64(row, int64(value))
		}
	case childExpr.Type.ID == types.Integer && expr.Type.ID == types.Double:
		for row := 0; row < chunk.Cardinality; row++ {
			value, ok := child.Int32(row)
			if !ok {
				out.SetNull(row)
				continue
			}
			out.SetFloat64(row, float64(value))
		}
	default:
		out.Release()
		return nil, fmt.Errorf("SQL expression executor: CAST kernel %d -> %d is not implemented", childExpr.Type.ID, expr.Type.ID)
	}
	return out, nil
}

// executeSQLBinary 分发 INTEGER/VARCHAR/DOUBLE typed kernel；NULL 传播遵循 SQL 三值语义。
func (e *Executor) executeSQLBinary(expr *binder.BoundExpression, chunk *vector.DataChunk) (*vector.Vector, error) {
	if len(expr.Children) != 2 {
		return nil, fmt.Errorf("SQL expression executor: binary %q requires two children", expr.Op)
	}
	leftType := expr.Children[0].Type.ID
	rightType := expr.Children[1].Type.ID
	if leftType != rightType {
		return nil, fmt.Errorf("SQL expression executor: binary inputs must already be coerced, got %d/%d", leftType, rightType)
	}
	switch leftType {
	case types.Boolean:
		return e.executeSQLBooleanBinary(expr, chunk)
	case types.Integer:
		return e.executeSQLIntegerBinary(expr, chunk)
	case types.BigInt:
		return e.executeSQLBigIntBinary(expr, chunk)
	case types.Varchar:
		return e.executeSQLVarcharBinary(expr, chunk)
	case types.Double:
		return e.executeSQLDoubleBinary(expr, chunk)
	default:
		return nil, fmt.Errorf("SQL expression executor: binary kernel for type %d op %q is not implemented", leftType, expr.Op)
	}
}

// executeSQLIntegerBinary 执行 INTEGER arithmetic/comparison，并在 overflow/division-by-zero 时显式失败。
func (e *Executor) executeSQLIntegerBinary(expr *binder.BoundExpression, chunk *vector.DataChunk) (*vector.Vector, error) {
	left, err := e.ExecuteSQL(expr.Children[0], chunk)
	if err != nil {
		return nil, err
	}
	defer left.Release()
	right, err := e.ExecuteSQL(expr.Children[1], chunk)
	if err != nil {
		return nil, err
	}
	defer right.Release()

	comparison := isComparisonOperator(expr.Op)
	outType := expr.Type
	if comparison {
		outType = types.LogicalType{ID: types.Boolean}
	}
	out, err := vector.NewFlat(e.Scope, e.Buffers, outType, chunk.Cardinality)
	if err != nil {
		return nil, err
	}
	for row := 0; row < chunk.Cardinality; row++ {
		x, xok := left.Int32(row)
		y, yok := right.Int32(row)
		if !xok || !yok {
			out.SetNull(row)
			continue
		}
		if comparison {
			out.SetBool(row, compareInt32(x, y, expr.Op))
			continue
		}
		var value int64
		switch expr.Op {
		case "+":
			value = int64(x) + int64(y)
		case "-":
			value = int64(x) - int64(y)
		case "*":
			value = int64(x) * int64(y)
		case "%":
			if y == 0 {
				out.Release()
				return nil, fmt.Errorf("SQL expression executor: INTEGER modulo by zero")
			}
			value = int64(x) % int64(y)
		default:
			out.Release()
			return nil, fmt.Errorf("SQL expression executor: unsupported INTEGER operator %q", expr.Op)
		}
		if value < math.MinInt32 || value > math.MaxInt32 {
			out.Release()
			return nil, fmt.Errorf("SQL expression executor: INTEGER overflow for operator %s", expr.Op)
		}
		out.SetInt32(row, int32(value))
	}
	return out, nil
}

// executeSQLVarcharBinary 执行默认 binary collation 下的 Stable VARCHAR 比较；v1 未引入自定义 collation。
func (e *Executor) executeSQLVarcharBinary(expr *binder.BoundExpression, chunk *vector.DataChunk) (*vector.Vector, error) {
	if !isComparisonOperator(expr.Op) {
		return nil, fmt.Errorf("SQL expression executor: VARCHAR operator %q is not a comparison", expr.Op)
	}
	left, err := e.ExecuteSQL(expr.Children[0], chunk)
	if err != nil {
		return nil, err
	}
	defer left.Release()
	right, err := e.ExecuteSQL(expr.Children[1], chunk)
	if err != nil {
		return nil, err
	}
	defer right.Release()
	out, err := vector.NewFlat(e.Scope, e.Buffers, types.LogicalType{ID: types.Boolean}, chunk.Cardinality)
	if err != nil {
		return nil, err
	}
	for row := 0; row < chunk.Cardinality; row++ {
		x, xok := left.String(row)
		y, yok := right.String(row)
		if !xok || !yok {
			out.SetNull(row)
			continue
		}
		cmp := strings.Compare(x, y)
		out.SetBool(row, comparisonFromOrder(cmp, expr.Op))
	}
	return out, nil
}

// executeSQLDoubleBinary 执行 DOUBLE arithmetic/comparison。
// 除零保留 IEEE 754 Inf/NaN；比较统一复用 semantics.CompareFloat64 的 DuckDB total-order 规则。
func (e *Executor) executeSQLDoubleBinary(expr *binder.BoundExpression, chunk *vector.DataChunk) (*vector.Vector, error) {
	left, err := e.ExecuteSQL(expr.Children[0], chunk)
	if err != nil {
		return nil, err
	}
	defer left.Release()
	right, err := e.ExecuteSQL(expr.Children[1], chunk)
	if err != nil {
		return nil, err
	}
	defer right.Release()
	comparison := isComparisonOperator(expr.Op)
	outType := expr.Type
	if comparison {
		outType = types.LogicalType{ID: types.Boolean}
	}
	out, err := vector.NewFlat(e.Scope, e.Buffers, outType, chunk.Cardinality)
	if err != nil {
		return nil, err
	}
	for row := 0; row < chunk.Cardinality; row++ {
		x, xok := left.Float64(row)
		y, yok := right.Float64(row)
		if !xok || !yok {
			out.SetNull(row)
			continue
		}
		if comparison {
			out.SetBool(row, comparisonFromOrder(semantics.CompareFloat64(x, y), expr.Op))
			continue
		}
		switch expr.Op {
		case "+":
			out.SetFloat64(row, x+y)
		case "-":
			out.SetFloat64(row, x-y)
		case "*":
			out.SetFloat64(row, x*y)
		case "/":
			// DuckDB 默认开启 IEEE floating-point operations；Go float64 对运行时零值除法给出相同 Inf/NaN。
			out.SetFloat64(row, x/y)
		case "%":
			out.Release()
			return nil, fmt.Errorf("SQL expression executor: DOUBLE modulo kernel is not implemented")
		default:
			out.Release()
			return nil, fmt.Errorf("SQL expression executor: unsupported DOUBLE operator %q", expr.Op)
		}
	}
	return out, nil
}

// compareInt32 执行已知为比较运算符的 INTEGER 比较。
func compareInt32(x, y int32, op string) bool {
	cmp := 0
	if x < y {
		cmp = -1
	} else if x > y {
		cmp = 1
	}
	return comparisonFromOrder(cmp, op)
}

// comparisonFromOrder 把三路比较结果映射为 SQL 比较操作符结果。
func comparisonFromOrder(cmp int, op string) bool {
	switch op {
	case "=":
		return cmp == 0
	case "<>":
		return cmp != 0
	case "<":
		return cmp < 0
	case "<=":
		return cmp <= 0
	case ">":
		return cmp > 0
	case ">=":
		return cmp >= 0
	default:
		return false
	}
}

// isComparisonOperator 判断 Stable v1 六种比较操作符。
func isComparisonOperator(op string) bool {
	switch op {
	case "=", "<>", "<", "<=", ">", ">=":
		return true
	default:
		return false
	}
}

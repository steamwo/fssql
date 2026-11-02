package expression

import (
	"fmt"
	"strconv"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// executeSQLRuntimeConstant 把 query-scoped 单值 managed Vector 广播到当前 batch。
// scalar Vector ownership 仍属于 PreparedSQLQuery.Run；本方法只创建当前表达式结果 Vector。
func (e *Executor) executeSQLRuntimeConstant(expr *binder.BoundExpression, count int) (*vector.Vector, error) {
	if e == nil || e.Scope == nil || e.Buffers == nil || expr == nil {
		return nil, fmt.Errorf("SQL expression executor: runtime constant executor is uninitialized")
	}
	id, err := strconv.Atoi(expr.Literal)
	if err != nil || id < 0 {
		return nil, fmt.Errorf("SQL expression executor: invalid runtime constant id %q", expr.Literal)
	}
	source := e.RuntimeConstants[id]
	if source == nil {
		return nil, fmt.Errorf("SQL expression executor: runtime constant %d is unavailable", id)
	}
	if !runtimeConstantTypeEqual(source.Type, expr.Type) {
		return nil, fmt.Errorf("SQL expression executor: runtime constant %d type mismatch", id)
	}
	out, err := vector.NewFlat(e.Scope, e.Buffers, expr.Type, count)
	if err != nil {
		return nil, err
	}
	for row := 0; row < count; row++ {
		if err := vector.CopyValue(source, 0, out, row); err != nil {
			out.Release()
			return nil, fmt.Errorf("SQL expression executor: copy runtime constant %d row %d: %w", id, row, err)
		}
	}
	return out, nil
}

func runtimeConstantTypeEqual(left, right types.LogicalType) bool {
	if left.ID != right.ID || left.Decimal != right.Decimal {
		return false
	}
	if (left.Child == nil) != (right.Child == nil) {
		return false
	}
	if left.Child != nil && !runtimeConstantTypeEqual(*left.Child, *right.Child) {
		return false
	}
	if len(left.Fields) != len(right.Fields) {
		return false
	}
	for i := range left.Fields {
		if left.Fields[i].Name != right.Fields[i].Name || !runtimeConstantTypeEqual(left.Fields[i].Type, right.Fields[i].Type) {
			return false
		}
	}
	return true
}

package expression

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// LowerSQLExpression 把 SQL Binder 的强类型 BoundExpression 降低为现有 Vector ExpressionExecutor IR。
// 当前只映射已有且语义完整的 kernel；缺失 kernel 必须显式报错，禁止近似或类型偷换。
func LowerSQLExpression(expr *binder.BoundExpression) (*BoundExpression, error) {
	if expr == nil {
		return nil, fmt.Errorf("expression lowering: nil SQL expression")
	}
	switch expr.Kind {
	case binder.BoundColumn:
		if expr.ColumnIndex < 0 {
			return nil, fmt.Errorf("expression lowering: negative column index %d", expr.ColumnIndex)
		}
		return &BoundExpression{Kind: Column, Type: cloneSQLType(expr.Type), ColumnIndex: expr.ColumnIndex}, nil
	case binder.BoundLiteral:
		return lowerSQLLiteral(expr)
	case binder.BoundNull:
		return nil, fmt.Errorf("expression lowering: unresolved SQL NULL reached execution")
	case binder.BoundUnary:
		return lowerSQLUnary(expr)
	case binder.BoundBinary:
		return lowerSQLBinary(expr)
	case binder.BoundIsNull:
		return lowerSQLIsNull(expr)
	case binder.BoundCast:
		return lowerSQLCast(expr)
	case binder.BoundFunction:
		return nil, fmt.Errorf("expression lowering: function id %d must be lowered by function/aggregate execution", expr.FunctionID)
	case binder.BoundSubquery:
		return nil, fmt.Errorf("expression lowering: scalar subquery requires planner lowering before expression execution")
	default:
		return nil, fmt.Errorf("expression lowering: unsupported SQL expression kind %d", expr.Kind)
	}
}

// lowerSQLLiteral 只开放现有执行器能够按同一 LogicalType 正确物化的 literal。
func lowerSQLLiteral(expr *binder.BoundExpression) (*BoundExpression, error) {
	switch expr.Type.ID {
	case types.Boolean:
		value, err := strconv.ParseBool(strings.ToLower(expr.Literal))
		if err != nil {
			return nil, fmt.Errorf("expression lowering: invalid BOOLEAN literal %q", expr.Literal)
		}
		return &BoundExpression{Kind: ConstantBool, Type: cloneSQLType(expr.Type), BoolValue: value}, nil
	case types.BigInt:
		value, err := strconv.ParseInt(expr.Literal, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("expression lowering: invalid BIGINT literal %q", expr.Literal)
		}
		return &BoundExpression{Kind: ConstantInt, Type: cloneSQLType(expr.Type), IntValue: value}, nil
	default:
		return nil, fmt.Errorf("expression lowering: literal kernel for logical type %d is not implemented", expr.Type.ID)
	}
}

// lowerSQLUnary 映射当前执行器已有的一元语义；数值正负号等待完整 typed numeric kernel。
func lowerSQLUnary(expr *binder.BoundExpression) (*BoundExpression, error) {
	if len(expr.Children) != 1 {
		return nil, fmt.Errorf("expression lowering: unary %q requires one child", expr.Op)
	}
	child, err := LowerSQLExpression(expr.Children[0])
	if err != nil {
		return nil, err
	}
	switch strings.ToUpper(expr.Op) {
	case "NOT":
		if expr.Type.ID != types.Boolean {
			return nil, fmt.Errorf("expression lowering: NOT result must be BOOLEAN")
		}
		return &BoundExpression{Kind: Not, Type: cloneSQLType(expr.Type), Children: []*BoundExpression{child}}, nil
	case "+", "-":
		return nil, fmt.Errorf("expression lowering: unary numeric operator %s requires typed numeric kernel", expr.Op)
	default:
		return nil, fmt.Errorf("expression lowering: unsupported unary operator %q", expr.Op)
	}
}

// lowerSQLBinary 只映射现有执行器已实现且类型安全的 BigInt/Boolean kernels。
func lowerSQLBinary(expr *binder.BoundExpression) (*BoundExpression, error) {
	if len(expr.Children) != 2 {
		return nil, fmt.Errorf("expression lowering: binary %q requires two children", expr.Op)
	}
	left, err := LowerSQLExpression(expr.Children[0])
	if err != nil {
		return nil, err
	}
	right, err := LowerSQLExpression(expr.Children[1])
	if err != nil {
		return nil, err
	}
	op := strings.ToUpper(expr.Op)
	var kind Kind
	switch op {
	case "AND":
		if !bothType(expr.Children, types.Boolean) || expr.Type.ID != types.Boolean {
			return nil, fmt.Errorf("expression lowering: AND requires BOOLEAN inputs/result")
		}
		kind = And
	case "OR":
		if !bothType(expr.Children, types.Boolean) || expr.Type.ID != types.Boolean {
			return nil, fmt.Errorf("expression lowering: OR requires BOOLEAN inputs/result")
		}
		kind = Or
	case "+", "-", "*":
		if !bothType(expr.Children, types.BigInt) || expr.Type.ID != types.BigInt {
			return nil, fmt.Errorf("expression lowering: %s currently requires BIGINT kernel", op)
		}
		switch op {
		case "+":
			kind = Add
		case "-":
			kind = Subtract
		default:
			kind = Multiply
		}
	case "=", "<":
		if !bothType(expr.Children, types.BigInt) || expr.Type.ID != types.Boolean {
			return nil, fmt.Errorf("expression lowering: %s currently requires BIGINT comparison kernel", op)
		}
		if op == "=" {
			kind = Equal
		} else {
			kind = LessThan
		}
	case "<>", "<=", ">", ">=", "/", "%":
		return nil, fmt.Errorf("expression lowering: operator %s kernel is not implemented", op)
	default:
		return nil, fmt.Errorf("expression lowering: unsupported binary operator %q", expr.Op)
	}
	return &BoundExpression{Kind: kind, Type: cloneSQLType(expr.Type), Children: []*BoundExpression{left, right}}, nil
}

// lowerSQLIsNull 保留 SQL NULL predicate；IS NOT NULL 组合为 NOT(IS NULL)。
func lowerSQLIsNull(expr *binder.BoundExpression) (*BoundExpression, error) {
	if len(expr.Children) != 1 {
		return nil, fmt.Errorf("expression lowering: %s requires one child", expr.Op)
	}
	child, err := LowerSQLExpression(expr.Children[0])
	if err != nil {
		return nil, err
	}
	isNull := &BoundExpression{Kind: IsNull, Type: types.LogicalType{ID: types.Boolean}, Children: []*BoundExpression{child}}
	switch strings.ToUpper(expr.Op) {
	case "IS NULL", "":
		return isNull, nil
	case "IS NOT NULL":
		return &BoundExpression{Kind: Not, Type: types.LogicalType{ID: types.Boolean}, Children: []*BoundExpression{isNull}}, nil
	default:
		return nil, fmt.Errorf("expression lowering: unsupported NULL predicate %q", expr.Op)
	}
}

// lowerSQLCast 仅开放现有执行器实现的 BIGINT↔DOUBLE cast。
func lowerSQLCast(expr *binder.BoundExpression) (*BoundExpression, error) {
	if len(expr.Children) != 1 {
		return nil, fmt.Errorf("expression lowering: CAST requires one child")
	}
	from := expr.Children[0].Type.ID
	to := expr.Type.ID
	if !((from == types.BigInt && to == types.Double) || (from == types.Double && to == types.BigInt)) {
		return nil, fmt.Errorf("expression lowering: CAST kernel %d -> %d is not implemented", from, to)
	}
	child, err := LowerSQLExpression(expr.Children[0])
	if err != nil {
		return nil, err
	}
	return &BoundExpression{Kind: Cast, Type: cloneSQLType(expr.Type), Children: []*BoundExpression{child}}, nil
}

// bothType 检查二元 SQL expression 的两个已绑定输入类型。
func bothType(children []*binder.BoundExpression, id types.LogicalTypeID) bool {
	return len(children) == 2 && children[0] != nil && children[1] != nil && children[0].Type.ID == id && children[1].Type.ID == id
}

// cloneSQLType 深拷贝 LIST/STRUCT metadata，避免 lowering 后两个 IR 共享可写 slice/pointer。
func cloneSQLType(t types.LogicalType) types.LogicalType {
	out := t
	if t.Child != nil {
		child := cloneSQLType(*t.Child)
		out.Child = &child
	}
	if t.Fields != nil {
		out.Fields = make([]types.StructField, len(t.Fields))
		for i, field := range t.Fields {
			out.Fields[i] = types.StructField{Name: field.Name, Type: cloneSQLType(field.Type)}
		}
	}
	return out
}

package planner

import (
	"context"
	"fmt"
	"math/big"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// UnlimitedRows 仅用于 LogicalLimit/PhysicalLimit 节点表示“没有 LIMIT，仅 OFFSET”。
// 合法 LIMIT 0 必须保留为 0；ScanPushdown.Limit 仍使用 0 表示未接受 pushdown。
const UnlimitedRows uint64 = ^uint64(0)

// PlanWithLimit 在 QueryPlanner foundation 之上增加 Stable LIMIT/OFFSET output modifier。
// ORDER BY/aggregate 等仍由基础 Planner 的 fail-closed gate 控制。
func (p *QueryPlanner) PlanWithLimit(ctx context.Context, query *binder.BoundQuery) (*LogicalPlan, error) {
	if query == nil {
		return nil, fmt.Errorf("query planner: nil BoundQuery")
	}
	limitExpr, offsetExpr := query.Limit, query.Offset
	baseQuery := *query
	baseQuery.Limit = nil
	baseQuery.Offset = nil
	root, err := p.Plan(ctx, &baseQuery)
	if err != nil {
		return nil, err
	}
	if limitExpr == nil && offsetExpr == nil {
		return root, nil
	}

	limit := UnlimitedRows
	if limitExpr != nil {
		limit, err = foldRowCountExpression(limitExpr, "LIMIT")
		if err != nil {
			return nil, err
		}
	}
	offset := uint64(0)
	if offsetExpr != nil {
		offset, err = foldRowCountExpression(offsetExpr, "OFFSET")
		if err != nil {
			return nil, err
		}
	}
	// OFFSET 0 without LIMIT is a semantic no-op；避免制造无意义 physical operator。
	if limitExpr == nil && offset == 0 {
		return root, nil
	}
	out := &LogicalPlan{
		Kind:     LogicalLimit,
		Children: []*LogicalPlan{root},
		Output:   clonePlannerColumns(root.Output),
		Limit:    limit,
		Offset:   offset,
	}
	if err := out.Validate(); err != nil {
		return nil, err
	}
	return out, nil
}

// foldRowCountExpression 计算 Binder 已保证无列/aggregate/subquery 的 integer constant expression。
// 每个中间结果都按 Binder 确定的 signed integer LogicalType 做 overflow 检查，最后再转换到 DuckDB LIMIT/OFFSET 的 UINT64 域。
func foldRowCountExpression(expression *binder.BoundExpression, label string) (uint64, error) {
	value, err := evalBoundIntegerConstant(expression)
	if err != nil {
		return 0, fmt.Errorf("query planner: %s constant: %w", label, err)
	}
	if value.Sign() < 0 {
		return 0, fmt.Errorf("query planner: %s cannot be negative", label)
	}
	if value.BitLen() > 64 {
		return 0, fmt.Errorf("query planner: %s value is out of UINT64 range", label)
	}
	return value.Uint64(), nil
}

// evalBoundIntegerConstant 只执行 Binder 已绑定的整数 literal/unary/binary/cast；不执行 scalar function 或 runtime expression。
func evalBoundIntegerConstant(expression *binder.BoundExpression) (*big.Int, error) {
	if expression == nil {
		return nil, fmt.Errorf("nil integer expression")
	}
	if !isPlannerIntegerType(expression.Type.ID) {
		return nil, fmt.Errorf("logical type %d is not integer", expression.Type.ID)
	}
	var out *big.Int
	switch expression.Kind {
	case binder.BoundLiteral:
		out = new(big.Int)
		if _, ok := out.SetString(expression.Literal, 10); !ok {
			return nil, fmt.Errorf("invalid integer literal %q", expression.Literal)
		}
	case binder.BoundUnary:
		if len(expression.Children) != 1 {
			return nil, fmt.Errorf("unary %q requires one child", expression.Op)
		}
		child, err := evalBoundIntegerConstant(expression.Children[0])
		if err != nil {
			return nil, err
		}
		switch expression.Op {
		case "+":
			out = new(big.Int).Set(child)
		case "-":
			out = new(big.Int).Neg(child)
		default:
			return nil, fmt.Errorf("unsupported integer unary operator %q", expression.Op)
		}
	case binder.BoundBinary:
		if len(expression.Children) != 2 {
			return nil, fmt.Errorf("binary %q requires two children", expression.Op)
		}
		left, err := evalBoundIntegerConstant(expression.Children[0])
		if err != nil {
			return nil, err
		}
		right, err := evalBoundIntegerConstant(expression.Children[1])
		if err != nil {
			return nil, err
		}
		switch expression.Op {
		case "+":
			out = new(big.Int).Add(left, right)
		case "-":
			out = new(big.Int).Sub(left, right)
		case "*":
			out = new(big.Int).Mul(left, right)
		case "%":
			if right.Sign() == 0 {
				return nil, fmt.Errorf("integer modulo by zero")
			}
			out = new(big.Int).Rem(left, right)
		default:
			return nil, fmt.Errorf("unsupported integer binary operator %q", expression.Op)
		}
	case binder.BoundCast:
		if len(expression.Children) != 1 {
			return nil, fmt.Errorf("integer cast requires one child")
		}
		child, err := evalBoundIntegerConstant(expression.Children[0])
		if err != nil {
			return nil, err
		}
		out = new(big.Int).Set(child)
	default:
		return nil, fmt.Errorf("expression kind %d is not a local integer constant", expression.Kind)
	}
	if err := validatePlannerIntegerRange(out, expression.Type.ID); err != nil {
		return nil, err
	}
	return out, nil
}

// validatePlannerIntegerRange 按 signed TINYINT/SMALLINT/INTEGER/BIGINT/HUGEINT 检查每个绑定节点的 overflow。
func validatePlannerIntegerRange(value *big.Int, id types.LogicalTypeID) error {
	bits := uint(0)
	switch id {
	case types.TinyInt:
		bits = 8
	case types.SmallInt:
		bits = 16
	case types.Integer:
		bits = 32
	case types.BigInt:
		bits = 64
	case types.HugeInt:
		bits = 128
	default:
		return fmt.Errorf("logical type %d is not integer", id)
	}
	limit := new(big.Int).Lsh(big.NewInt(1), bits-1)
	min := new(big.Int).Neg(new(big.Int).Set(limit))
	max := new(big.Int).Sub(new(big.Int).Set(limit), big.NewInt(1))
	if value.Cmp(min) < 0 || value.Cmp(max) > 0 {
		return fmt.Errorf("integer overflow for logical type %d", id)
	}
	return nil
}

func isPlannerIntegerType(id types.LogicalTypeID) bool {
	switch id {
	case types.TinyInt, types.SmallInt, types.Integer, types.BigInt, types.HugeInt:
		return true
	default:
		return false
	}
}

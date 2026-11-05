package planner

import (
	"fmt"

	"github.com/steamwo/fssql/internal/sql/binder"
)

// HashJoinKeys 是从已绑定 JOIN predicate 中提取的左右等值 key 列。
// Right 中的序号已重映射为 right child 自身的 0-based column index。
type HashJoinKeys struct {
	NullsEqual bool  // All keys use NULL-safe equality; mixed modes are rejected.
	Left       []int // Left 按 predicate conjunct 顺序保存 left child key index。
	Right      []int // Right 与 Left 一一对应保存 right child key index。
}

// ExtractHashJoinKeys 只接受纯 equi-join predicate：`left_col = right_col`，可用 AND 连接多个 key。
// 非等值、同侧比较、表达式 key 或其他 residual 都显式失败，避免 PhysicalHashJoin 静默改变 ON 语义。
func ExtractHashJoinKeys(predicate *binder.BoundExpression, leftColumns, rightColumns int) (HashJoinKeys, error) {
	if predicate == nil {
		return HashJoinKeys{}, fmt.Errorf("hash join requires predicate")
	}
	if leftColumns < 0 || rightColumns < 0 {
		return HashJoinKeys{}, fmt.Errorf("hash join child column count is invalid")
	}
	out := HashJoinKeys{}
	if err := extractHashJoinConjunct(predicate, leftColumns, rightColumns, &out); err != nil {
		return HashJoinKeys{}, err
	}
	if len(out.Left) == 0 || len(out.Left) != len(out.Right) {
		return HashJoinKeys{}, fmt.Errorf("hash join requires at least one equi key")
	}
	return out, nil
}

func extractHashJoinConjunct(expr *binder.BoundExpression, leftColumns, rightColumns int, out *HashJoinKeys) error {
	if expr == nil {
		return fmt.Errorf("hash join predicate contains nil expression")
	}
	if expr.Kind == binder.BoundBinary && expr.Op == "AND" {
		if len(expr.Children) != 2 {
			return fmt.Errorf("hash join AND predicate requires two children")
		}
		if err := extractHashJoinConjunct(expr.Children[0], leftColumns, rightColumns, out); err != nil {
			return err
		}
		return extractHashJoinConjunct(expr.Children[1], leftColumns, rightColumns, out)
	}
	nullSafe := isPhase5NullSafeEquality(expr)
	if nullSafe {
		expr = expr.Children[0].Children[0]
	}
	if len(out.Left) > 0 && out.NullsEqual != nullSafe {
		return fmt.Errorf("mixed NULL-safe and ordinary hash keys are unsupported")
	}
	out.NullsEqual = nullSafe
	if expr.Kind != binder.BoundBinary || expr.Op != "=" || len(expr.Children) != 2 {
		return fmt.Errorf("non-equi/residual JOIN predicate is not hash-join executable")
	}
	leftExpr, rightExpr := expr.Children[0], expr.Children[1]
	if leftExpr == nil || rightExpr == nil || leftExpr.Kind != binder.BoundColumn || rightExpr.Kind != binder.BoundColumn {
		return fmt.Errorf("hash join key must be materialized BoundColumn on both sides")
	}
	li, lok := joinChildIndex(leftExpr.ColumnIndex, leftColumns, rightColumns)
	ri, rok := joinChildIndex(rightExpr.ColumnIndex, leftColumns, rightColumns)
	if !lok.valid || !rok.valid || lok.side == rok.side {
		return fmt.Errorf("hash join equality must compare one left and one right column")
	}
	if lok.side == joinLeftSide {
		out.Left = append(out.Left, li)
		out.Right = append(out.Right, ri)
	} else {
		out.Left = append(out.Left, ri)
		out.Right = append(out.Right, li)
	}
	return nil
}

type joinKeySide uint8

const (
	joinLeftSide joinKeySide = iota
	joinRightSide
)

type joinChildRef struct {
	side  joinKeySide
	valid bool
}

func joinChildIndex(index, leftColumns, rightColumns int) (int, joinChildRef) {
	if index >= 0 && index < leftColumns {
		return index, joinChildRef{side: joinLeftSide, valid: true}
	}
	if index >= leftColumns && index < leftColumns+rightColumns {
		return index - leftColumns, joinChildRef{side: joinRightSide, valid: true}
	}
	return 0, joinChildRef{}
}

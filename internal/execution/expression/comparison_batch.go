package expression

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/semantics"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func batchComparator(t types.LogicalType) func([]byte, []byte) int {
	if t.IsUnsignedInteger() {
		switch t.ID {
		case types.TinyInt:
			return func(a, b []byte) int { return semantics.CompareUint64(uint64(a[0]), uint64(b[0])) }
		case types.SmallInt:
			return func(a, b []byte) int {
				return semantics.CompareUint64(uint64(binary.LittleEndian.Uint16(a)), uint64(binary.LittleEndian.Uint16(b)))
			}
		case types.Integer:
			return func(a, b []byte) int {
				return semantics.CompareUint64(uint64(binary.LittleEndian.Uint32(a)), uint64(binary.LittleEndian.Uint32(b)))
			}
		case types.BigInt:
			return func(a, b []byte) int {
				return semantics.CompareUint64(binary.LittleEndian.Uint64(a), binary.LittleEndian.Uint64(b))
			}
		}
	}
	switch t.ID {
	case types.Varchar:
		return bytes.Compare
	case types.Boolean:
		return func(a, b []byte) int {
			if a[0] == b[0] {
				return 0
			}
			if a[0] == 0 {
				return -1
			}
			return 1
		}
	case types.TinyInt:
		return func(a, b []byte) int { return semantics.CompareInt64(int64(int8(a[0])), int64(int8(b[0]))) }
	case types.SmallInt:
		return func(a, b []byte) int {
			return semantics.CompareInt64(int64(int16(binary.LittleEndian.Uint16(a))), int64(int16(binary.LittleEndian.Uint16(b))))
		}
	case types.Integer, types.DateType:
		return func(a, b []byte) int {
			return semantics.CompareInt64(int64(int32(binary.LittleEndian.Uint32(a))), int64(int32(binary.LittleEndian.Uint32(b))))
		}
	case types.BigInt, types.TimestampType:
		return func(a, b []byte) int {
			return semantics.CompareInt64(int64(binary.LittleEndian.Uint64(a)), int64(binary.LittleEndian.Uint64(b)))
		}
	case types.Float:
		return func(a, b []byte) int {
			return semantics.CompareFloat32(math.Float32frombits(binary.LittleEndian.Uint32(a)), math.Float32frombits(binary.LittleEndian.Uint32(b)))
		}
	case types.Double:
		return func(a, b []byte) int {
			return semantics.CompareFloat64(math.Float64frombits(binary.LittleEndian.Uint64(a)), math.Float64frombits(binary.LittleEndian.Uint64(b)))
		}
	}
	return nil
}

func comparisonPredicate(op string) func(int) bool {
	switch op {
	case "=":
		return func(c int) bool { return c == 0 }
	case "!=", "<>":
		return func(c int) bool { return c != 0 }
	case "<":
		return func(c int) bool { return c < 0 }
	case "<=":
		return func(c int) bool { return c <= 0 }
	case ">":
		return func(c int) bool { return c > 0 }
	case ">=":
		return func(c int) bool { return c >= 0 }
	}
	return nil
}

// compareSQLBatch evaluates the existing children in order but borrows their
// buffers once. Operation and physical type dispatch happen before the loop.
func (e *Executor) compareSQLBatch(expr *binder.BoundExpression, chunk *vector.DataChunk, consume func(int, bool, bool)) (bool, error) {
	if expr == nil || expr.Kind != binder.BoundBinary || len(expr.Children) != 2 {
		return false, nil
	}
	predicate := comparisonPredicate(expr.Op)
	if predicate == nil {
		return false, nil
	}
	leftType, rightType := expr.Children[0].Type, expr.Children[1].Type
	if !leftType.Same(rightType) {
		return false, nil
	}
	compare := batchComparator(leftType)
	if compare == nil {
		return false, nil
	}
	left, err := e.ExecuteSQL(expr.Children[0], chunk)
	if err != nil {
		return true, err
	}
	defer left.Release()
	right, err := e.ExecuteSQL(expr.Children[1], chunk)
	if err != nil {
		return true, err
	}
	defer right.Release()
	compareVectorBatch(left, right, chunk.Cardinality, compare, predicate, consume)

	return true, nil
}

func compareVectorBatch(left, right *vector.Vector, count int, compare func([]byte, []byte) int, predicate func(int) bool, consume func(int, bool, bool)) {
	if left.Type.ID == types.Varchar {
		l, _ := left.BorrowBytes()
		r, _ := right.BorrowBytes()
		for row := 0; row < count; row++ {
			a, av := l.Value(row)
			b, bv := r.Value(row)
			valid := av && bv
			value := false
			if valid {
				value = predicate(compare(a, b))
			}
			consume(row, value, valid)
		}
	} else {
		l, _ := left.BorrowFixedWidth()
		r, _ := right.BorrowFixedWidth()
		for row := 0; row < count; row++ {
			a, av := l.Value(row)
			b, bv := r.Value(row)
			valid := av && bv
			value := false
			if valid {
				value = predicate(compare(a, b))
			}
			consume(row, value, valid)
		}
	}
}

// SelectSQL directly produces WHERE selections for supported comparisons.
// Unsupported expressions keep the existing SQL executor fallback.
func (e *Executor) SelectSQL(expr *binder.BoundExpression, chunk *vector.DataChunk, rows []int) ([]int, bool, error) {
	handled, err := e.compareSQLBatch(expr, chunk, func(row int, value, valid bool) {
		if valid && value {
			rows = append(rows, row)
		}
	})
	return rows, handled, err
}

func (e *Executor) executeBatchComparison(expr *binder.BoundExpression, chunk *vector.DataChunk) (*vector.Vector, bool, error) {
	if expr.Kind != binder.BoundBinary || len(expr.Children) != 2 || comparisonPredicate(expr.Op) == nil || !expr.Children[0].Type.Same(expr.Children[1].Type) || batchComparator(expr.Children[0].Type) == nil {
		return nil, false, nil
	}
	left, err := e.ExecuteSQL(expr.Children[0], chunk)
	if err != nil {
		return nil, true, err
	}
	defer left.Release()
	right, err := e.ExecuteSQL(expr.Children[1], chunk)
	if err != nil {
		return nil, true, err
	}
	defer right.Release()
	out, err := vector.NewFlat(e.Scope, e.Buffers, types.LogicalType{ID: types.Boolean}, chunk.Cardinality)
	if err != nil {
		return nil, true, err
	}
	data, validity, _ := out.MutableFixedWidth()
	compareVectorBatch(left, right, chunk.Cardinality, batchComparator(left.Type), comparisonPredicate(expr.Op), func(row int, value, valid bool) {
		if !valid {
			validity.SetValid(row, false)
		} else if value {
			data[row] = 1
		} else {
			data[row] = 0
		}
	})
	return out, true, nil
}

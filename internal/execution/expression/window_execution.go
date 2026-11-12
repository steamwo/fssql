package expression

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"sync"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

type windowState struct {
	distinctPrevious *vector.DataChunk
	mu               sync.Mutex
	partition        []ScalarValue
	keyScratch       []ScalarValue // 复用当前行分区键；字节仅在输入向量存活期间借用。
	rowNumber        int64
	count            int64
	haveSum          bool
	intSum           big.Int
	floatSum         float64
}

var windowStateMu sync.Mutex
var windowStates = map[*Executor]map[*binder.BoundExpression]*windowState{}

func ReleaseWindowState(e *Executor) {
	windowStateMu.Lock()
	for _, state := range windowStates[e] {
		if state.distinctPrevious != nil {
			state.distinctPrevious.Release()
			state.distinctPrevious = nil
		}
	}
	delete(windowStates, e)
	windowStateMu.Unlock()
}
func windowStateFor(e *Executor, expr *binder.BoundExpression) *windowState {
	windowStateMu.Lock()
	defer windowStateMu.Unlock()
	m := windowStates[e]
	if m == nil {
		m = map[*binder.BoundExpression]*windowState{}
		windowStates[e] = m
	}
	s := m[expr]
	if s == nil {
		s = &windowState{}
		m[expr] = s
	}
	return s
}

type runtimeWindowSpec struct {
	partition, order int
	frame            string
}

func runtimeWindowSpecOf(expr *binder.BoundExpression) (runtimeWindowSpec, error) {
	if expr == nil || len(expr.Children) < 1 || expr.Children[0].Kind != binder.BoundLiteral {
		return runtimeWindowSpec{}, fmt.Errorf("expression: malformed window spec")
	}
	var s runtimeWindowSpec
	for _, part := range strings.Split(expr.Children[0].Literal, ";") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "p":
			s.partition, _ = strconv.Atoi(kv[1])
		case "o":
			s.order, _ = strconv.Atoi(kv[1])
		case "f":
			s.frame = kv[1]
		}
	}
	if s.partition < 0 || s.order < 0 {
		return s, fmt.Errorf("expression: invalid window spec")
	}
	return s, nil
}

// executeWindow 执行流式窗口计算，并复用每个表达式独立的分区键 scratch。
func (e *Executor) executeWindow(expr *binder.BoundExpression, chunk *vector.DataChunk) (*vector.Vector, error) {
	spec, err := runtimeWindowSpecOf(expr)
	if err != nil {
		return nil, err
	}
	state := windowStateFor(e, expr)
	state.mu.Lock()
	defer state.mu.Unlock()
	// 输入向量释放前清除 scratch 中的借用字节，仅保留可复用容量。
	defer func() {
		for i := range state.keyScratch {
			state.keyScratch[i].Bytes = nil
		}
	}()
	valueOffset := 1
	if expr.FunctionID == function.WindowSumID {
		valueOffset = 2
	}
	if len(expr.Children) < valueOffset+spec.partition+spec.order {
		return nil, fmt.Errorf("expression: incomplete window arguments")
	}
	parts := make([]*vector.Vector, spec.partition)
	for i := 0; i < spec.partition; i++ {
		v, er := e.ExecuteSQL(expr.Children[valueOffset+i], chunk)
		if er != nil {
			releaseScalarVectors(parts)
			return nil, er
		}
		parts[i] = v
	}
	defer releaseScalarVectors(parts)
	var values *vector.Vector
	if expr.FunctionID == function.WindowSumID {
		values, err = e.ExecuteSQL(expr.Children[1], chunk)
		if err != nil {
			return nil, err
		}
		defer values.Release()
	}
	out, err := vector.NewFlat(e.Scope, e.Buffers, expr.Type, chunk.Cardinality)
	if err != nil {
		return nil, err
	}
	for row := 0; row < chunk.Cardinality; row++ {
		key, er := state.partitionKey(parts, row)
		if er != nil {
			out.Release()
			return nil, er
		}
		if !samePartition(state.partition, key) {
			state.partition = cloneScalarValues(key)
			state.rowNumber = 0
			state.count = 0
			state.haveSum = false
			state.intSum.SetInt64(0)
			state.floatSum = 0
		}
		switch expr.FunctionID {
		case function.WindowRowNumberID:
			state.rowNumber++
			out.SetInt64(row, state.rowNumber)
		case function.WindowCountID:
			if spec.frame != "prefix1" {
				out.Release()
				return nil, fmt.Errorf("expression: COUNT window must be rewritten or use supported prefix frame")
			}
			out.SetInt64(row, state.count)
			state.count++
		case function.WindowSumID:
			if spec.frame != "prefix1" {
				out.Release()
				return nil, fmt.Errorf("expression: SUM window must be rewritten or use supported prefix frame")
			}
			if !state.haveSum {
				out.SetNull(row)
			} else if err := writeAccumulator(out, row, expr.Type, state); err != nil {
				out.Release()
				return nil, err
			}
			if values.IsValid(row) {
				if err := addAccumulator(state, values, row); err != nil {
					out.Release()
					return nil, err
				}
			}
		default:
			out.Release()
			return nil, fmt.Errorf("expression: unknown window function %d", expr.FunctionID)
		}
	}
	return out, nil
}

// windowScalarFromVector 仅在窗口键路径借用 VARCHAR 字节，其他类型沿用通用转换。
func windowScalarFromVector(v *vector.Vector, row int) (ScalarValue, error) {
	if v.Type.ID != types.Varchar {
		return scalarFromVector(v, row)
	}
	out := ScalarValue{Type: v.Type, Valid: v.IsValid(row)}
	if out.Valid {
		out.Bytes, _ = v.Bytes(row)
	}
	return out, nil
}

// partitionKey 复用状态内的当前行键切片，分区切换时由调用方复制保存。
func (s *windowState) partitionKey(vs []*vector.Vector, row int) ([]ScalarValue, error) {
	if cap(s.keyScratch) < len(vs) {
		s.keyScratch = make([]ScalarValue, len(vs))
	} else {
		s.keyScratch = s.keyScratch[:len(vs)]
	}
	for i, v := range vs {
		x, err := windowScalarFromVector(v, row)
		if err != nil {
			return nil, err
		}
		s.keyScratch[i] = x
	}
	return s.keyScratch, nil
}
func cloneScalarValues(in []ScalarValue) []ScalarValue {
	out := append([]ScalarValue(nil), in...)
	for i := range out {
		if out[i].Bytes != nil {
			out[i].Bytes = append([]byte(nil), out[i].Bytes...)
		}
	}
	return out
}
func samePartition(a, b []ScalarValue) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Valid != b[i].Valid {
			return false
		}
		if !a[i].Valid {
			continue
		}
		if (a[i].Type.ID == types.Float || a[i].Type.ID == types.Double) && math.IsNaN(a[i].Float64) != math.IsNaN(b[i].Float64) {
			return false
		}
		c, err := compareScalar(a[i], b[i])
		if err != nil || c != 0 {
			return false
		}
	}
	return true
}
func int128Big(v types.Int128) *big.Int {
	hi := new(big.Int).SetInt64(v.Hi)
	hi.Lsh(hi, 64)
	return hi.Add(hi, new(big.Int).SetUint64(v.Lo))
}

func addAccumulator(s *windowState, v *vector.Vector, row int) error {
	x, err := scalarFromVector(v, row)
	if err != nil {
		return err
	}
	if !x.Valid {
		return nil
	}
	s.haveSum = true
	if v.Type.IsUnsignedInteger() {
		s.intSum.Add(&s.intSum, new(big.Int).SetUint64(x.Uint64))
		return nil
	}
	switch v.Type.ID {
	case types.TinyInt, types.SmallInt, types.Integer, types.BigInt:
		s.intSum.Add(&s.intSum, big.NewInt(x.Int64))
	case types.HugeInt, types.Decimal:
		s.intSum.Add(&s.intSum, int128Big(x.Int128))
	case types.Float, types.Double:
		s.floatSum += x.Float64
	default:
		return fmt.Errorf("expression: SUM window unsupported type %d", v.Type.ID)
	}
	return nil
}
func writeAccumulator(dst *vector.Vector, row int, t types.LogicalType, s *windowState) error {
	switch t.ID {
	case types.Double:
		dst.SetFloat64(row, s.floatSum)
		return nil
	case types.HugeInt:
		v, ok := types.Int128FromBig(&s.intSum)
		if !ok {
			return fmt.Errorf("expression: SUM window overflow")
		}
		dst.SetInt128(row, v)
		return nil
	case types.Decimal:
		v, ok := types.Int128FromBig(&s.intSum)
		if !ok {
			return fmt.Errorf("expression: SUM window decimal overflow")
		}
		if t.Decimal.Width <= 18 {
			b := int128Big(v)
			if !b.IsInt64() {
				return fmt.Errorf("expression: SUM window decimal overflow")
			}
			dst.SetInt64(row, b.Int64())
		} else {
			dst.SetInt128(row, v)
		}
		return nil
	default:
		return fmt.Errorf("expression: SUM window result type %d unsupported", t.ID)
	}
}

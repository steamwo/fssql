package expression

import (
	"fmt"
	"math"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

// executeManagedRepeat reserves variable-width output before constructing it.
func (e *Executor) executeManagedRepeat(args []*vector.Vector, count int) (*vector.Vector, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("repeat requires two arguments")
	}
	out, err := vector.NewFlat(e.Scope, e.Buffers, types.LogicalType{ID: types.Varchar}, count)
	if err != nil {
		return nil, err
	}
	for row := 0; row < count; row++ {
		src, valid := args[0].Bytes(row)
		n, countValid := args[1].Int64(row)
		if !valid || !countValid {
			out.SetNull(row)
			continue
		}
		if n < 0 {
			out.Release()
			return nil, fmt.Errorf("repeat count must be non-negative")
		}
		if len(src) == 0 || n == 0 {
			if err := out.SetBytes(row, nil); err != nil {
				out.Release()
				return nil, err
			}
			continue
		}
		// StringRef lengths are uint32, and allocations must also fit a native int.
		max := uint64(math.MaxUint32)
		if native := uint64(int(^uint(0) >> 1)); native < max {
			max = native
		}
		if uint64(n) > max/uint64(len(src)) {
			out.Release()
			return nil, fmt.Errorf("repeat output length overflow")
		}
		size := len(src) * int(n)
		buf, err := e.Buffers.Allocate(e.Scope, memory.TagStringHeap, size)
		if err != nil {
			out.Release()
			return nil, err
		}
		data := buf.Bytes()
		written := copy(data, src)
		for written < len(data) {
			written += copy(data[written:], data[:written])
		}
		err = out.SetBytes(row, data)
		buf.Release()
		if err != nil {
			out.Release()
			return nil, err
		}
	}
	return out, nil
}

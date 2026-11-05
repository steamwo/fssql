package expression

import (
	"encoding/binary"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/semantics"
)

// int64ArithmeticBatch chooses the operation and borrows managed buffers once.
// The flat, non-null path also avoids resolving representation and validity for
// every input value; constants and dictionaries retain the same SQL semantics.
func int64ArithmeticBatch(kind Kind, left, right, out *vector.Vector, count int) (bool, error) {
	if left.Type.ID != types.BigInt || right.Type.ID != types.BigInt || out.Type.ID != types.BigInt || left.Type.IsUnsignedInteger() || right.Type.IsUnsignedInteger() {
		return false, nil
	}
	var operation func(int64, int64) (int64, error)
	switch kind {
	case Add:
		operation = semantics.AddInt64
	case Subtract:
		operation = semantics.SubInt64
	case Multiply:
		operation = semantics.MulInt64
	default:
		return false, nil
	}
	l, _ := left.BorrowFixedWidth()
	r, _ := right.BorrowFixedWidth()
	dst, validity, _ := out.MutableFixedWidth()
	ld, lv, lf := l.FlatData()
	rd, rv, rf := r.FlatData()
	if lf && rf && lv.AllValid(count) && rv.AllValid(count) {
		for row := 0; row < count; row++ {
			a := int64(binary.LittleEndian.Uint64(ld[row*8:]))
			b := int64(binary.LittleEndian.Uint64(rd[row*8:]))
			value, err := operation(a, b)
			if err != nil {
				return true, err
			}
			binary.LittleEndian.PutUint64(dst[row*8:], uint64(value))
		}
		return true, nil
	}
	for row := 0; row < count; row++ {
		a, av := l.Value(row)
		b, bv := r.Value(row)
		if !av || !bv {
			validity.SetValid(row, false)
			continue
		}
		value, err := operation(int64(binary.LittleEndian.Uint64(a)), int64(binary.LittleEndian.Uint64(b)))
		if err != nil {
			return true, err
		}
		binary.LittleEndian.PutUint64(dst[row*8:], uint64(value))
	}
	return true, nil
}

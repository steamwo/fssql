package expression

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// executeStringBatch 每批借用一次只读输入；注册覆盖仍保留标量参数独立所有权。
func (e *Executor) executeStringBatch(expr *binder.BoundExpression, args []*vector.Vector, count int) (*vector.Vector, bool, error) {
	if _, overridden := e.Scalars[expr.FunctionID]; overridden {
		return nil, false, nil
	}
	switch expr.FunctionID {
	case function.LengthID:
		if len(args) != 1 || !expr.Type.Same(types.LogicalType{ID: types.BigInt}) {
			return nil, false, nil
		}
	case function.StrposID:
		if len(args) != 2 || !expr.Type.Same(types.LogicalType{ID: types.BigInt}) {
			return nil, false, nil
		}
	case function.ConcatID:
		if len(args) != 2 || expr.Type.ID != types.Varchar {
			return nil, false, nil
		}
	case function.LowerID:
		if len(args) != 1 || expr.Type.ID != types.Varchar {
			return nil, false, nil
		}
	default:
		return nil, false, nil
	}
	left, ok := args[0].BorrowBytes()
	if !ok {
		return nil, false, nil
	}
	var right vector.BytesView
	if len(args) == 2 {
		right, ok = args[1].BorrowBytes()
		if !ok {
			return nil, false, nil
		}
	}
	out, err := vector.NewFlat(e.Scope, e.Buffers, expr.Type, count)
	if err != nil {
		return nil, true, err
	}
	keep := false
	defer func() {
		if !keep {
			out.Release()
		}
	}()
	if count == 0 {
		keep = true
		return out, true, nil
	}
	switch expr.FunctionID {
	case function.LengthID:
		data, validity, _ := out.MutableFixedWidth()
		for row := 0; row < count; row++ {
			value, valid := left.Value(row)
			if !valid {
				validity.SetValid(row, false)
				continue
			}
			binary.LittleEndian.PutUint64(data[row*8:], uint64(countRunesBytes(value)))
		}
	case function.StrposID:
		data, validity, _ := out.MutableFixedWidth()
		for row := 0; row < count; row++ {
			text, textValid := left.Value(row)
			needle, needleValid := right.Value(row)
			if !textValid || !needleValid {
				validity.SetValid(row, false)
				continue
			}
			binary.LittleEndian.PutUint64(data[row*8:], uint64(strposBytes(text, needle)))
		}
	case function.ConcatID:
		// 仅累计外部堆值，避免 NULL 与内联短字符串导致过早限额。
		heapBytes := uint64(0)
		maxInt := int(^uint(0) >> 1)
		var inlineRef vector.StringRef
		for row := 0; row < count; row++ {
			a, av := left.Value(row)
			b, bv := right.Value(row)
			if !av || !bv {
				continue
			}
			size := uint64(len(a)) + uint64(len(b))
			if size > math.MaxUint32 {
				return nil, true, errors.New("string batch value exceeds uint32 length")
			}
			if size <= uint64(len(inlineRef.Inline)) {
				continue
			}
			if size > uint64(maxInt)-heapBytes {
				return nil, true, errors.New("string batch heap size overflow")
			}
			heapBytes += size
		}
		writer, err := out.NewStringBatchWriter()
		if err != nil {
			return nil, true, err
		}
		defer writer.Commit()
		if err := writer.Reserve(int(heapBytes)); err != nil {
			return nil, true, err
		}
		for row := 0; row < count; row++ {
			a, av := left.Value(row)
			b, bv := right.Value(row)
			if !av || !bv {
				out.SetNull(row)
				continue
			}
			if err := writer.AppendParts(row, a, b); err != nil {
				return nil, true, err
			}
		}
	case function.LowerID:
		writer, err := out.NewStringBatchWriter()
		if err != nil {
			return nil, true, err
		}
		defer writer.Commit()
		for row := 0; row < count; row++ {
			value, valid := left.Value(row)
			if !valid {
				out.SetNull(row)
				continue
			}
			lowered := strings.ToLower(string(value))
			if err := writer.Append(row, []byte(lowered)); err != nil {
				return nil, true, err
			}
		}
	}
	keep = true
	return out, true, nil
}

func strposBytes(text, needle []byte) int {
	if len(needle) == 0 {
		return 1
	}
	// The scalar implementation compares runes, including replacement runes
	// for invalid UTF-8; preserve that behavior on the exceptional path.
	if !utf8.Valid(text) || !utf8.Valid(needle) {
		return strposRunes(string(text), string(needle))
	}
	position := bytes.Index(text, needle)
	if position < 0 {
		return 0
	}
	return countRunesBytes(text[:position]) + 1
}

// Count directly from bytes: some supported Go versions implement RuneCount's
// non-ASCII path through string conversion, which can allocate for long values.
func countRunesBytes(data []byte) int {
	count := 0
	for position := 0; position < len(data); count++ {
		if data[position] < utf8.RuneSelf {
			position++
			continue
		}
		_, width := utf8.DecodeRune(data[position:])
		position += width
	}
	return count
}

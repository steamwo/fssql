package hash

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/steamwo/fssql/internal/core/vector"
)

// ChunkRowEncoder borrows one chunk's column views so encoding many rows does
// not re-acquire managed buffers or re-resolve vector kinds per row.
type ChunkRowEncoder struct {
	chunk    *vector.DataChunk
	layout   RowLayout
	fixed    []vector.FixedWidthView
	bytes    []vector.BytesView
	variable []bool
}

func NewChunkRowEncoder(chunk *vector.DataChunk, layout RowLayout) (*ChunkRowEncoder, error) {
	if chunk == nil {
		return nil, errors.New("nil encoder chunk")
	}
	if !layout.relocatable {
		return nil, errors.New("row encoder requires relocatable layout")
	}
	if len(chunk.Columns) != len(layout.Columns) {
		return nil, errors.New("row encoder column count mismatch")
	}
	e := &ChunkRowEncoder{
		chunk:    chunk,
		layout:   layout,
		fixed:    make([]vector.FixedWidthView, len(layout.Columns)),
		bytes:    make([]vector.BytesView, len(layout.Columns)),
		variable: make([]bool, len(layout.Columns)),
	}
	for i, col := range layout.Columns {
		if col.Variable {
			view, ok := chunk.Columns[i].BorrowBytes()
			if !ok {
				return nil, errors.New("row encoder requires varchar view")
			}
			e.bytes[i] = view
			e.variable[i] = true
			continue
		}
		view, ok := chunk.Columns[i].BorrowFixedWidth()
		if !ok {
			return nil, errors.New("row encoder requires fixed-width view")
		}
		e.fixed[i] = view
	}
	return e, nil
}

func (e *ChunkRowEncoder) RowSize(row int) (int, error) {
	if row < 0 || row >= e.chunk.Cardinality {
		return 0, errors.New("row out of range")
	}
	size := e.layout.FixedWidth
	for i, col := range e.layout.Columns {
		if !col.Variable {
			continue
		}
		data, valid := e.bytes[i].Value(row)
		if !valid {
			continue
		}
		if uint64(size)+uint64(len(data)) > uint64(math.MaxInt) {
			return 0, errors.New("row size overflow")
		}
		size += len(data)
	}
	return size, nil
}

func (e *ChunkRowEncoder) WriteRowSized(row int, dst []byte, size int) error {
	if size < e.layout.FixedWidth || len(dst) < size {
		return errors.New("row destination too small")
	}
	for i := 0; i < size; i++ {
		dst[i] = 0
	}
	varOff := e.layout.FixedWidth
	for i, col := range e.layout.Columns {
		if e.variable[i] {
			data, valid := e.bytes[i].Value(row)
			if !valid {
				setNullBit(dst, i)
				continue
			}
			binary.LittleEndian.PutUint32(dst[col.Offset:col.Offset+4], uint32(varOff))
			binary.LittleEndian.PutUint32(dst[col.Offset+4:col.Offset+8], uint32(len(data)))
			copy(dst[varOff:varOff+len(data)], data)
			varOff += len(data)
			continue
		}
		data, valid := e.fixed[i].Value(row)
		if !valid {
			setNullBit(dst, i)
			continue
		}
		copy(dst[col.Offset:col.Offset+col.Width], data)
	}
	return nil
}

func (e *ChunkRowEncoder) WriteRow(row int, dst []byte) (int, error) {
	size, err := e.RowSize(row)
	if err != nil {
		return 0, err
	}
	if err := e.WriteRowSized(row, dst, size); err != nil {
		return 0, err
	}
	return size, nil
}

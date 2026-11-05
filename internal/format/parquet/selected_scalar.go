package parquet

import (
	"context"
	"encoding/binary"
	"errors"
	"io"

	"github.com/steamwo/fssql/internal/core/vector"
)

// skipScalar advances physical values without converting or materializing them.
// Complete pages need only their bounded headers; partial pages use the decoder.
func (c *ColumnCursor) skipScalar(ctx context.Context, rows int64) error {
	if c.closed || c.leaf.MaxRep != 0 || rows < 0 || rows > c.meta.NumValues-c.consumed {
		return errors.New("parquet: invalid scalar skip")
	}
	for rows > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		if c.page == nil {
			skipped, err := c.loadPageSkipping(ctx, rows)
			if err != nil {
				return err
			}
			rows -= skipped
			if rows == 0 {
				break
			}
		}
		n := int(min(rows, int64(min(c.page.remaining, vector.DefaultVectorSize))))
		defined := 0
		if c.hasPending {
			n = 1
			if c.pending.HasValue {
				defined = 1
			}
		} else if c.page.def.definedPrefix(n) == n {
			c.page.def.consumeDefined(n)
			defined = n
		} else {
			var positions [vector.DefaultVectorSize]int
			var err error
			defined, err = c.page.def.readValidity(nil, 0, n, positions[:0])
			if err != nil {
				return err
			}
		}
		if n == 0 {
			return io.ErrUnexpectedEOF
		}
		if c.page.values != nil {
			if err := c.page.values.skip(defined); err != nil {
				return err
			}
		} else if c.page.dict != nil {
			var indices [vector.DefaultVectorSize]uint32
			if err := c.page.dict.Read(indices[:defined]); err != nil {
				return err
			}
			for _, index := range indices[:defined] {
				if uint64(index) >= uint64(c.dictionary.Count()) {
					return errors.New("parquet: dictionary index out of range")
				}
			}
		} else {
			return errors.New("parquet: page has no value decoder")
		}
		c.hasPending = false
		c.finishScalarBatch(n, 0)
		rows -= int64(n)
	}
	return nil
}

func (s *valueStream) skip(n int) error {
	width := 0
	switch s.physical {
	case TypeBoolean:
		if n > len(s.data)*8-s.bitPos {
			return io.ErrUnexpectedEOF
		}
		s.bitPos += n
		return nil
	case TypeInt32, TypeFloat:
		width = 4
	case TypeInt64, TypeDouble:
		width = 8
	case TypeInt96:
		width = 12
	case TypeFixedLenByteArray:
		if s.leaf == nil || s.leaf.Element.TypeLength == nil || *s.leaf.Element.TypeLength <= 0 {
			return errors.New("parquet: missing fixed value width")
		}
		width = int(*s.leaf.Element.TypeLength)
	case TypeByteArray:
		for i := 0; i < n; i++ {
			if s.off > len(s.data)-4 {
				return io.ErrUnexpectedEOF
			}
			length := uint64(binary.LittleEndian.Uint32(s.data[s.off:]))
			s.off += 4
			if length > uint64(len(s.data)-s.off) {
				return io.ErrUnexpectedEOF
			}
			s.off += int(length)
		}
		return nil
	default:
		return errors.New("parquet: unsupported physical skip")
	}
	if s.off < 0 || s.off > len(s.data) || n > (len(s.data)-s.off)/width {
		return io.ErrUnexpectedEOF
	}
	s.off += n * width
	return nil
}

// readSelected keeps the physical cursor aligned while decoding only selected
// runs. Selection rows are sorted and relative to the requested input batch.
func (c *ColumnCursor) readSelected(ctx context.Context, rows int, selection []int) (*vector.Vector, error) {
	if len(selection) == rows {
		return c.ReadScalar(ctx, rows)
	}
	out, err := vector.NewFlat(c.scope, c.bm, c.leaf.Logical, len(selection))
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			out.Release()
		}
	}()
	position := 0
	for i := 0; i < len(selection); {
		start := selection[i]
		end := i + 1
		for end < len(selection) && selection[end] == start+end-i {
			end++
		}
		if err := c.skipScalar(ctx, int64(start-position)); err != nil {
			return nil, err
		}
		if err := c.readScalarInto(ctx, out, i, end-i); err != nil {
			return nil, err
		}
		position = start + end - i
		i = end
	}
	if err := c.skipScalar(ctx, int64(rows-position)); err != nil {
		return nil, err
	}
	keep = true
	return out, nil
}

func (c *ColumnCursor) readScalarInto(ctx context.Context, out *vector.Vector, start, rows int) error {
	for i := start; i < start+rows; {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := c.readScalarBatch(ctx, out, i, start+rows-i)
		if err != nil {
			return err
		}
		if n > 0 {
			i += n
			continue
		}
		entry, err := c.NextInto(ctx, out, i)
		if err != nil {
			return err
		}
		if entry.Repetition != 0 {
			return ErrRepeatedColumnRequiresLevelReader
		}
		if !entry.HasValue {
			out.SetNull(i)
		}
		i++
	}
	return nil
}

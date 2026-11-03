package parquet

import (
	"context"
	"errors"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

// Only physical/logical pairs that need no conversion may copy native bytes.
// Decimal precision and timestamp unit conversion retain the scalar decoder.
func (s *valueStream) writeFixedBatch(dst *vector.Vector, start, n int) (bool, error) {
	width := 0
	switch s.physical {
	case TypeInt32:
		if dst.Type.ID == types.Integer || dst.Type.ID == types.DateType {
			width = 4
		}
	case TypeInt64:
		if dst.Type.ID == types.BigInt || dst.Type.ID == types.TimestampType {
			width = 8
		}
		if s.leaf != nil {
			e := s.leaf.Element
			if e.Logical != nil && e.Logical.Kind == LogicalTimestamp && e.Logical.Timestamp != nil && e.Logical.Timestamp.Unit != UnitMicros {
				width = 0
			}
			if e.Converted != nil && *e.Converted == ConvertedTimestampMillis {
				width = 0
			}
		}
	case TypeFloat:
		if dst.Type.ID == types.Float {
			width = 4
		}
	case TypeDouble:
		if dst.Type.ID == types.Double {
			width = 8
		}
	}
	if width == 0 {
		return false, nil
	}
	if s.off < 0 || s.off > len(s.data) || n < 0 || n > (len(s.data)-s.off)/width {
		return true, errors.New("parquet: truncated fixed-width batch")
	}
	end := s.off + n*width
	if err := dst.WriteFixedWidth(start, s.data[s.off:end]); err != nil {
		return true, err
	}
	s.off = end
	return true, nil
}

func (c *ColumnCursor) readScalarBatch(ctx context.Context, dst *vector.Vector, start, want int) (int, error) {
	if c.hasPending {
		return 0, nil
	}
	if c.page == nil || c.page.remaining == 0 {
		if err := c.loadPage(ctx); err != nil {
			return 0, err
		}
	}
	p := c.page
	if p == nil || p.remaining == 0 {
		return 0, nil
	}
	n := min(want, p.remaining)
	// A NULL RLE run consumes no value payload. The first level was validated
	// by NextInto before it became an active run.
	if !p.def.constant && p.def.hybrid != nil && p.def.hybrid.runRemain > 0 && int(p.def.hybrid.runValue) < c.leaf.MaxDef {
		n = min(n, p.def.hybrid.runRemain)
		for i := start; i < start+n; i++ {
			dst.SetNull(i)
		}
		p.def.hybrid.runRemain -= n
		c.finishScalarBatch(n, 0)
		return n, nil
	}
	n = p.def.definedPrefix(n)
	if n == 0 {
		return 0, nil
	}
	handled := false
	if p.values != nil {
		var err error
		handled, err = p.values.writeFixedBatch(dst, start, n)
		if err != nil {
			return 0, err
		}
	} else if p.dict != nil {
		if _, fixed := dst.Type.FixedSize(); fixed {
			var indices [vector.DefaultVectorSize]uint32
			if err := p.dict.Read(indices[:n]); err != nil {
				return 0, err
			}
			if err := dst.CopyFixedIndices(start, c.dictionary, indices[:n]); err != nil {
				return 0, err
			}
			handled = true
		}
	}
	if !handled {
		return 0, nil
	}
	p.def.consumeDefined(n)
	c.finishScalarBatch(n, n)
	return n, nil
}

func (c *ColumnCursor) finishScalarBatch(n, values int) {
	c.page.remaining -= n
	c.consumed += int64(n)
	c.metrics.DataValues += int64(values)
	if c.page.remaining == 0 {
		c.releasePage()
	}
}

// Inspect only the current run; structural/invalid levels remain on the
// checked scalar path. Both RLE and packed all-defined prefixes can batch.
func (s *levelStream) definedPrefix(want int) int {
	if s.constant {
		return want
	}
	data, bit, width := s.packed, s.bitPos, s.bitWidth
	if s.hybrid != nil {
		h := s.hybrid
		if h.runRemain > 0 {
			if int(h.runValue) == s.max {
				return min(want, h.runRemain)
			}
			return 0
		}
		want = min(want, h.packedLeft)
		data, bit, width = h.packed, h.packedBit, h.bitWidth
	} else if width > 0 {
		want = min(want, (len(data)*8-bit)/width)
	}
	n := 0
	for n < want && int(packedWord(data, bit+n*width, width)) == s.max {
		n++
	}
	return n
}

func (s *levelStream) consumeDefined(n int) {
	if s.constant {
		return
	}
	if s.hybrid != nil {
		if s.hybrid.runRemain > 0 {
			s.hybrid.runRemain -= n
		} else {
			s.hybrid.packedBit += n * s.hybrid.bitWidth
			s.hybrid.packedLeft -= n
		}
	} else {
		s.bitPos += n * s.bitWidth
	}
}

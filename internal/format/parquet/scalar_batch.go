package parquet

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

// Only physical/logical pairs that need no conversion may copy native bytes.
// Decimal precision and timestamp unit conversion retain the scalar decoder.
func (s *valueStream) fixedBatchWidth(dst *vector.Vector) int {
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
	return width
}

func (s *valueStream) writeFixedBatch(dst *vector.Vector, start, n int) (bool, error) {
	width := s.fixedBatchWidth(dst)
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

// writeFixedBatchSelected scatters n native values into the supplied output
// rows, keeping null slots untouched. It is the mixed-definition-level path.
func (s *valueStream) writeFixedBatchSelected(dst *vector.Vector, positions []int) (bool, error) {
	width := s.fixedBatchWidth(dst)
	if width == 0 {
		return false, nil
	}
	n := len(positions)
	if s.off < 0 || s.off > len(s.data) || n > (len(s.data)-s.off)/width {
		return true, errors.New("parquet: truncated fixed-width batch")
	}
	end := s.off + n*width
	if err := dst.WriteFixedWidthSelected(s.data[s.off:end], positions); err != nil {
		return true, err
	}
	s.off = end
	return true, nil
}

// writeStringBatch decodes PLAIN BYTE_ARRAY length-prefixed values in one pass
// through a StringBatchWriter, which acquires the heap once per batch.
func (s *valueStream) writeStringBatch(dst *vector.Vector, dstStart int, rows []int, n int) (bool, error) {
	if s.physical != TypeByteArray || dst.Type.ID != types.Varchar {
		return false, nil
	}
	if rows != nil {
		n = len(rows)
	}
	if n == 0 {
		return true, nil
	}
	writer, err := dst.NewStringBatchWriter()
	if err != nil {
		return true, err
	}
	off := s.off
	for i := 0; i < n; i++ {
		if off+4 > len(s.data) {
			return true, errors.New("parquet: byte-array length exhausted")
		}
		length := int(binary.LittleEndian.Uint32(s.data[off : off+4]))
		off += 4
		if length < 0 || off+length > len(s.data) {
			return true, errors.New("parquet: byte-array value exhausted")
		}
		row := dstStart + i
		if rows != nil {
			row = rows[i]
		}
		if err := writer.Append(row, s.data[off:off+length]); err != nil {
			return true, err
		}
		off += length
	}
	writer.Commit()
	s.off = off
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
	if p.def.definedPrefix(n) == n {
		handled, err := c.readContiguousValues(dst, start, n)
		if err != nil {
			return 0, err
		}
		if !handled {
			return 0, nil
		}
		p.def.consumeDefined(n)
		c.finishScalarBatch(n, n)
		return n, nil
	}
	var positions [vector.DefaultVectorSize]int
	defined, err := p.def.readValidity(dst, start, n, positions[:0])
	if err != nil {
		return 0, err
	}
	if defined > 0 {
		if err := c.readDefinedValues(dst, positions[:defined]); err != nil {
			return 0, err
		}
	}
	c.finishScalarBatch(n, defined)
	return n, nil
}

func (c *ColumnCursor) readContiguousValues(dst *vector.Vector, start, n int) (bool, error) {
	p := c.page
	if p.values != nil {
		handled, err := p.values.writeFixedBatch(dst, start, n)
		if err != nil || handled {
			return handled, err
		}
		return p.values.writeStringBatch(dst, start, nil, n)
	}
	if p.dict != nil {
		var indices [vector.DefaultVectorSize]uint32
		if n > len(indices) {
			return true, errors.New("parquet: dictionary batch exceeds vector size")
		}
		if err := p.dict.Read(indices[:n]); err != nil {
			return true, err
		}
		if err := vector.CopyDictionaryIndices(dst, start, c.dictionary, indices[:n]); err != nil {
			return true, err
		}
		return true, nil
	}
	return false, nil
}

// readDefinedValues is called after definition levels have already been
// consumed. It gathers dictionary indices in one stream read, or scatters
// fixed-width plain values into their defined positions.
func (c *ColumnCursor) readDefinedValues(dst *vector.Vector, positions []int) error {
	n := len(positions)
	if n == 0 {
		return nil
	}
	if n > vector.DefaultVectorSize {
		return errors.New("parquet: defined batch exceeds vector size")
	}
	contiguous := true
	base := positions[0]
	for i := 1; i < n; i++ {
		if positions[i] != base+i {
			contiguous = false
			break
		}
	}
	p := c.page
	if p.values != nil {
		if contiguous {
			handled, err := p.values.writeFixedBatch(dst, base, n)
			if err != nil {
				return err
			}
			if handled {
				return nil
			}
			handled, err = p.values.writeStringBatch(dst, base, nil, n)
			if err != nil {
				return err
			}
			if handled {
				return nil
			}
		} else {
			handled, err := p.values.writeFixedBatchSelected(dst, positions)
			if err != nil {
				return err
			}
			if handled {
				return nil
			}
			handled, err = p.values.writeStringBatch(dst, 0, positions, n)
			if err != nil {
				return err
			}
			if handled {
				return nil
			}
		}
		for _, row := range positions {
			if err := p.values.NextInto(dst, row); err != nil {
				return err
			}
		}
		return nil
	}
	if p.dict != nil {
		var indices [vector.DefaultVectorSize]uint32
		if err := p.dict.Read(indices[:n]); err != nil {
			return err
		}
		if contiguous {
			return vector.CopyDictionaryIndices(dst, base, c.dictionary, indices[:n])
		}
		return vector.CopyDictionaryIndicesSelected(dst, c.dictionary, indices[:n], positions)
	}
	return errors.New("parquet: page has no value decoder")
}

// readValidity decodes n definition levels into destination validity and
// appends the defined output rows. It replays RLE runs and packed groups in
// batches instead of advancing one level at a time.
func (s *levelStream) readValidity(dst *vector.Vector, start, n int, positions []int) (int, error) {
	if s == nil || s.constant {
		return n, nil
	}
	defined := 0
	written := 0
	for written < n {
		if s.hybrid != nil {
			h := s.hybrid
			if h.runRemain == 0 && h.packedLeft == 0 {
				if err := h.nextRun(); err != nil {
					return 0, err
				}
			}
			if h.runRemain > 0 {
				take := min(n-written, h.runRemain)
				value := int(h.runValue)
				if value > s.max {
					return 0, fmt.Errorf("parquet: level %d exceeds max %d", value, s.max)
				}
				if value == s.max {
					for i := 0; i < take; i++ {
						positions = append(positions, start+written+i)
					}
					defined += take
				} else {
					for i := 0; i < take; i++ {
						if dst != nil { dst.SetNull(start + written + i) }
					}
				}
				h.runRemain -= take
				written += take
				continue
			}
			if h.packedLeft > 0 {
				take := min(n-written, h.packedLeft)
				for i := 0; i < take; i++ {
					value := int(packedWord(h.packed, h.packedBit+i*h.bitWidth, h.bitWidth))
					if value > s.max {
						return 0, fmt.Errorf("parquet: level %d exceeds max %d", value, s.max)
					}
					if value == s.max {
						positions = append(positions, start+written+i)
						defined++
					} else {
						if dst != nil { dst.SetNull(start + written + i) }
					}
				}
				h.packedBit += take * h.bitWidth
				h.packedLeft -= take
				written += take
				continue
			}
			return 0, errors.New("parquet: level stream made no progress")
		}
		if s.bitWidth <= 0 {
			return 0, errors.New("parquet: invalid packed level width")
		}
		available := (len(s.packed)*8 - s.bitPos) / s.bitWidth
		take := min(n-written, available)
		if take <= 0 {
			return 0, errors.New("parquet: bit-packed level stream exhausted")
		}
		for i := 0; i < take; i++ {
			value := int(packedWord(s.packed, s.bitPos+i*s.bitWidth, s.bitWidth))
			if value > s.max {
				return 0, fmt.Errorf("parquet: level %d exceeds max %d", value, s.max)
			}
			if value == s.max {
				positions = append(positions, start+written+i)
				defined++
			} else {
				if dst != nil { dst.SetNull(start + written + i) }
			}
		}
		s.bitPos += take * s.bitWidth
		written += take
	}
	return defined, nil
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

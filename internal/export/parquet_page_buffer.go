package export

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/steamwo/fssql/internal/core/memory"
	coretypes "github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

// parquetPageBuffer 是单页生命周期的可增长 Managed Buffer；增长始终先经 Reservation.Resize。
type parquetPageBuffer struct {
	// buffer 是 page payload 的 Managed Buffer。
	buffer *memory.Buffer
	// used 是已经写入的 payload 字节数。
	used int
}

// newParquetPageBuffer 创建 page payload buffer。
func newParquetPageBuffer(scope *memory.Scope, bm *memory.BufferManager, initial int) (*parquetPageBuffer, error) {
	if initial < 1 {
		initial = defaultParquetPageBuffer
	}
	buf, err := bm.Allocate(scope, memory.TagExportBuffer, initial)
	if err != nil {
		return nil, err
	}
	return &parquetPageBuffer{buffer: buf}, nil
}

// Len 返回当前有效 payload 长度。
func (b *parquetPageBuffer) Len() int { return b.used }

// Bytes 返回当前有效 payload 视图；只在 buffer 未释放时使用。
func (b *parquetPageBuffer) Bytes() []byte { return b.buffer.Bytes()[:b.used] }

// Reserve 先扩 Managed reservation/backing buffer，随后调用者才写新数据。
func (b *parquetPageBuffer) Reserve(extra int) error {
	if extra < 0 || extra > math.MaxInt-b.used {
		return errors.New("export: parquet page buffer size overflow")
	}
	need := b.used + extra
	if need <= b.buffer.Len() {
		return nil
	}
	cap := b.buffer.Len() * 2
	if cap < need {
		cap = need
	}
	return b.buffer.Resize(cap, nil)
}

// Append 追加字节到受管 page buffer。
func (b *parquetPageBuffer) Append(p []byte) error {
	if err := b.Reserve(len(p)); err != nil {
		return err
	}
	copy(b.buffer.Bytes()[b.used:b.used+len(p)], p)
	b.used += len(p)
	return nil
}

// AppendByte 追加一个字节。
func (b *parquetPageBuffer) AppendByte(v byte) error {
	if err := b.Reserve(1); err != nil {
		return err
	}
	b.buffer.Bytes()[b.used] = v
	b.used++
	return nil
}

// AppendUvarint 追加 unsigned varint，供 definition-level RLE run 使用。
func (b *parquetPageBuffer) AppendUvarint(v uint64) error {
	var scratch [10]byte
	n := binary.PutUvarint(scratch[:], v)
	return b.Append(scratch[:n])
}

// Release 释放当前 page payload ownership。
func (b *parquetPageBuffer) Release() {
	if b != nil && b.buffer != nil {
		b.buffer.Release()
		b.buffer = nil
		b.used = 0
	}
}

// encodeDefinitionLevels 写 DataPage v1 的 4-byte length + RLE hybrid definition levels。
func encodeDefinitionLevels(out *parquetPageBuffer, vec *vector.Vector, start, count int) error {
	prefixPos := out.used
	if err := out.Append([]byte{0, 0, 0, 0}); err != nil {
		return err
	}
	streamStart := out.used
	for row := start; row < start+count; {
		value := byte(0)
		if vec.IsValid(row) {
			value = 1
		}
		run := 1
		for row+run < start+count {
			next := byte(0)
			if vec.IsValid(row + run) {
				next = 1
			}
			if next != value {
				break
			}
			run++
		}
		if err := out.AppendUvarint(uint64(run << 1)); err != nil {
			return err
		}
		if err := out.AppendByte(value); err != nil {
			return err
		}
		row += run
	}
	binary.LittleEndian.PutUint32(out.buffer.Bytes()[prefixPos:prefixPos+4], uint32(out.used-streamStart))
	return nil
}

// encodeParquetPlain 按 parquet-format PLAIN 规则写当前 page 的非 NULL values。
func encodeParquetPlain(out *parquetPageBuffer, vec *vector.Vector, typ coretypes.LogicalType, start, count int) error {
	if typ.ID == coretypes.Boolean {
		var packed byte
		bit := uint(0)
		for row := start; row < start+count; row++ {
			if !vec.IsValid(row) {
				continue
			}
			v, _ := vec.Bool(row)
			if v {
				packed |= 1 << bit
			}
			bit++
			if bit == 8 {
				if err := out.AppendByte(packed); err != nil {
					return err
				}
				packed, bit = 0, 0
			}
		}
		if bit != 0 {
			return out.AppendByte(packed)
		}
		return nil
	}
	var scratch [16]byte
	for row := start; row < start+count; row++ {
		if !vec.IsValid(row) {
			continue
		}
		switch typ.ID {
		case coretypes.TinyInt:
			v, _ := vec.Int8(row)
			binary.LittleEndian.PutUint32(scratch[:4], uint32(int32(v)))
			if err := out.Append(scratch[:4]); err != nil {
				return err
			}
		case coretypes.SmallInt:
			v, _ := vec.Int16(row)
			binary.LittleEndian.PutUint32(scratch[:4], uint32(int32(v)))
			if err := out.Append(scratch[:4]); err != nil {
				return err
			}
		case coretypes.Integer, coretypes.DateType:
			v, _ := vec.Int32(row)
			binary.LittleEndian.PutUint32(scratch[:4], uint32(v))
			if err := out.Append(scratch[:4]); err != nil {
				return err
			}
		case coretypes.BigInt, coretypes.TimestampType:
			v, _ := vec.Int64(row)
			binary.LittleEndian.PutUint64(scratch[:8], uint64(v))
			if err := out.Append(scratch[:8]); err != nil {
				return err
			}
		case coretypes.Float:
			v, _ := vec.Float32(row)
			binary.LittleEndian.PutUint32(scratch[:4], math.Float32bits(v))
			if err := out.Append(scratch[:4]); err != nil {
				return err
			}
		case coretypes.Double:
			v, _ := vec.Float64(row)
			binary.LittleEndian.PutUint64(scratch[:8], math.Float64bits(v))
			if err := out.Append(scratch[:8]); err != nil {
				return err
			}
		case coretypes.Decimal:
			if typ.Decimal.Width <= 18 {
				v, _ := vec.Int64(row)
				binary.LittleEndian.PutUint64(scratch[:8], uint64(v))
				if err := out.Append(scratch[:8]); err != nil {
					return err
				}
			} else {
				v, _ := vec.Int128(row)
				binary.BigEndian.PutUint64(scratch[:8], uint64(v.Hi))
				binary.BigEndian.PutUint64(scratch[8:16], v.Lo)
				if err := out.Append(scratch[:16]); err != nil {
					return err
				}
			}
		case coretypes.Varchar:
			v, _ := vec.String(row)
			if uint64(len(v)) > math.MaxUint32 {
				return errors.New("export: parquet string exceeds uint32")
			}
			binary.LittleEndian.PutUint32(scratch[:4], uint32(len(v)))
			if err := out.Append(scratch[:4]); err != nil {
				return err
			}
			if err := out.Append([]byte(v)); err != nil {
				return err
			}
		default:
			return fmt.Errorf("export: unsupported parquet plain type %d", typ.ID)
		}
	}
	return nil
}

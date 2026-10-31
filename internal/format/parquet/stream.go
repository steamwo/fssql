package parquet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	coretypes "github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

type hybridStream struct {
	data       []byte
	bitWidth   int
	off        int
	runRemain  int
	runValue   uint32
	packed     []byte
	packedBit  int
	packedLeft int
}

func newHybridStream(data []byte, bitWidth int) (*hybridStream, error) {
	if bitWidth < 0 || bitWidth > 32 {
		return nil, fmt.Errorf("parquet: invalid hybrid bit width %d", bitWidth)
	}
	return &hybridStream{data: data, bitWidth: bitWidth}, nil
}

func (s *hybridStream) Next() (uint32, error) {
	if s.runRemain > 0 {
		s.runRemain--
		return s.runValue, nil
	}
	if s.packedLeft > 0 {
		var v uint32
		for b := 0; b < s.bitWidth; b++ {
			byteIdx := s.packedBit >> 3
			bitIdx := s.packedBit & 7
			if s.packed[byteIdx]&(1<<bitIdx) != 0 {
				v |= 1 << b
			}
			s.packedBit++
		}
		s.packedLeft--
		return v, nil
	}
	if s.off >= len(s.data) {
		return 0, errors.New("parquet: hybrid stream exhausted")
	}
	header, n := binary.Uvarint(s.data[s.off:])
	if n <= 0 {
		return 0, errors.New("parquet: invalid hybrid run header")
	}
	s.off += n
	if header&1 == 0 {
		run := int(header >> 1)
		if run <= 0 {
			return 0, errors.New("parquet: zero RLE run")
		}
		bw := (s.bitWidth + 7) / 8
		if s.off+bw > len(s.data) {
			return 0, errors.New("parquet: truncated RLE run")
		}
		var v uint32
		for i := 0; i < bw; i++ {
			v |= uint32(s.data[s.off+i]) << (8 * i)
		}
		s.off += bw
		s.runValue = v
		s.runRemain = run - 1
		return v, nil
	}
	groups := int(header >> 1)
	if groups <= 0 {
		return 0, errors.New("parquet: zero bit-packed run")
	}
	bytesNeeded := groups * s.bitWidth
	if s.off+bytesNeeded > len(s.data) {
		return 0, errors.New("parquet: truncated bit-packed run")
	}
	s.packed = s.data[s.off : s.off+bytesNeeded]
	s.off += bytesNeeded
	s.packedBit = 0
	s.packedLeft = groups * 8
	return s.Next()
}

type levelStream struct {
	max      int
	constant bool
	hybrid   *hybridStream
	packed   []byte
	bitWidth int
	bitPos   int
}

func newLevelsV1(data []byte, maxLevel, count int, enc Encoding) (*levelStream, int, error) {
	if maxLevel == 0 {
		return &levelStream{max: 0, constant: true}, 0, nil
	}
	bw := levelBitWidth(maxLevel)
	switch enc {
	case EncodingRLE:
		if len(data) < 4 {
			return nil, 0, errors.New("parquet: truncated level length")
		}
		n := int(binary.LittleEndian.Uint32(data[:4]))
		if n < 0 || n > len(data)-4 {
			return nil, 0, errors.New("parquet: invalid level stream length")
		}
		h, err := newHybridStream(data[4:4+n], bw)
		if err != nil {
			return nil, 0, err
		}
		return &levelStream{max: maxLevel, hybrid: h}, 4 + n, nil
	case EncodingBitPacked:
		used := (count*bw + 7) / 8
		if used > len(data) {
			return nil, 0, errors.New("parquet: truncated bit-packed level stream")
		}
		return &levelStream{max: maxLevel, packed: data[:used], bitWidth: bw}, used, nil
	default:
		return nil, 0, fmt.Errorf("parquet: unsupported level encoding %d", enc)
	}
}

func newLevelsV2(data []byte, maxLevel int) (*levelStream, error) {
	if maxLevel == 0 {
		return &levelStream{max: 0, constant: true}, nil
	}
	h, err := newHybridStream(data, levelBitWidth(maxLevel))
	if err != nil {
		return nil, err
	}
	return &levelStream{max: maxLevel, hybrid: h}, nil
}

func (s *levelStream) Next() (int, error) {
	if s == nil || s.constant {
		return s.max, nil
	}
	if s.hybrid != nil {
		v, err := s.hybrid.Next()
		if err != nil {
			return 0, err
		}
		if int(v) > s.max {
			return 0, fmt.Errorf("parquet: level %d exceeds max %d", v, s.max)
		}
		return int(v), nil
	}
	if s.bitPos+s.bitWidth > len(s.packed)*8 {
		return 0, errors.New("parquet: bit-packed level stream exhausted")
	}
	var v int
	for b := 0; b < s.bitWidth; b++ {
		byteIdx := s.bitPos >> 3
		bitIdx := s.bitPos & 7
		if s.packed[byteIdx]&(1<<bitIdx) != 0 {
			v |= 1 << b
		}
		s.bitPos++
	}
	if v > s.max {
		return 0, fmt.Errorf("parquet: level %d exceeds max %d", v, s.max)
	}
	return v, nil
}

type valueStream struct {
	data     []byte
	off      int
	bitPos   int
	physical PhysicalType
	leaf     *SchemaNode
}

func newValueStream(data []byte, physical PhysicalType, leaf *SchemaNode) *valueStream {
	return &valueStream{data: data, physical: physical, leaf: leaf}
}

func (s *valueStream) NextInto(dst *vector.Vector, row int) error {
	switch s.physical {
	case TypeBoolean:
		if s.bitPos >= len(s.data)*8 {
			return errors.New("parquet: boolean stream exhausted")
		}
		v := s.data[s.bitPos>>3]&(1<<uint(s.bitPos&7)) != 0
		s.bitPos++
		dst.SetBool(row, v)
		return nil
	case TypeInt32:
		if s.off+4 > len(s.data) {
			return errors.New("parquet: int32 stream exhausted")
		}
		v := int32(binary.LittleEndian.Uint32(s.data[s.off : s.off+4]))
		s.off += 4
		if dst.Type.ID == coretypes.Decimal {
			dst.SetInt64(row, int64(v))
		} else {
			dst.SetInt32(row, v)
		}
		return nil
	case TypeInt64:
		if s.off+8 > len(s.data) {
			return errors.New("parquet: int64 stream exhausted")
		}
		v := int64(binary.LittleEndian.Uint64(s.data[s.off : s.off+8]))
		s.off += 8
		if s.leaf != nil {
			e := s.leaf.Element
			if e.Logical != nil && e.Logical.Kind == LogicalTimestamp && e.Logical.Timestamp != nil {
				switch e.Logical.Timestamp.Unit {
				case UnitMillis:
					v *= 1000
				case UnitNanos:
					v /= 1000
				}
			} else if e.Converted != nil && *e.Converted == ConvertedTimestampMillis {
				v *= 1000
			}
		}
		dst.SetInt64(row, v)
		return nil
	case TypeInt96:
		if s.off+12 > len(s.data) {
			return errors.New("parquet: int96 stream exhausted")
		}
		nanos := int64(binary.LittleEndian.Uint64(s.data[s.off : s.off+8]))
		julian := int64(binary.LittleEndian.Uint32(s.data[s.off+8 : s.off+12]))
		s.off += 12
		const unixJulian = int64(2440588)
		micros := (julian-unixJulian)*86400*1_000_000 + nanos/1000
		dst.SetInt64(row, micros)
		return nil
	case TypeFloat:
		if s.off+4 > len(s.data) {
			return errors.New("parquet: float stream exhausted")
		}
		dst.SetFloat32(row, math.Float32frombits(binary.LittleEndian.Uint32(s.data[s.off:s.off+4])))
		s.off += 4
		return nil
	case TypeDouble:
		if s.off+8 > len(s.data) {
			return errors.New("parquet: double stream exhausted")
		}
		dst.SetFloat64(row, math.Float64frombits(binary.LittleEndian.Uint64(s.data[s.off:s.off+8])))
		s.off += 8
		return nil
	case TypeByteArray:
		if s.off+4 > len(s.data) {
			return errors.New("parquet: byte-array length exhausted")
		}
		n := int(binary.LittleEndian.Uint32(s.data[s.off : s.off+4]))
		s.off += 4
		if n < 0 || s.off+n > len(s.data) {
			return errors.New("parquet: byte-array value exhausted")
		}
		value := s.data[s.off : s.off+n]
		s.off += n
		if dst.Type.ID == coretypes.Decimal {
			v, err := decodeDecimal64BigEndian(value, dst.Type)
			if err != nil {
				return err
			}
			dst.SetInt64(row, v)
			return nil
		}
		return dst.SetStringBytes(row, value)
	case TypeFixedLenByteArray:
		if s.leaf == nil || s.leaf.Element.TypeLength == nil || *s.leaf.Element.TypeLength <= 0 {
			return errors.New("parquet: fixed-len byte array missing type length")
		}
		n := int(*s.leaf.Element.TypeLength)
		if s.off+n > len(s.data) {
			return errors.New("parquet: fixed-len byte-array stream exhausted")
		}
		value := s.data[s.off : s.off+n]
		s.off += n
		if dst.Type.ID != coretypes.Decimal {
			return errors.New("parquet: FIXED_LEN_BYTE_ARRAY requires supported logical type")
		}
		v, err := decodeDecimal64BigEndian(value, dst.Type)
		if err != nil {
			return err
		}
		dst.SetInt64(row, v)
		return nil
	default:
		return fmt.Errorf("parquet: physical value stream %d not implemented", s.physical)
	}
}

func decodeDecimal64BigEndian(src []byte, typ coretypes.LogicalType) (int64, error) {
	if typ.ID != coretypes.Decimal || typ.Decimal.Width == 0 || typ.Decimal.Width > 18 {
		return 0, fmt.Errorf("parquet: DECIMAL(%d,%d) requires Decimal128 representation", typ.Decimal.Width, typ.Decimal.Scale)
	}
	if len(src) == 0 {
		return 0, errors.New("parquet: empty decimal byte representation")
	}
	negative := src[0]&0x80 != 0
	for len(src) > 8 {
		expected := byte(0x00)
		if negative {
			expected = 0xff
		}
		if src[0] != expected || (src[1]&0x80 != 0) != negative {
			return 0, errors.New("parquet: decimal value overflows Decimal64")
		}
		src = src[1:]
	}
	var bits uint64
	for _, b := range src {
		bits = bits<<8 | uint64(b)
	}
	if negative && len(src) < 8 {
		bits |= ^uint64(0) << uint(len(src)*8)
	}
	return int64(bits), nil
}

type dictionaryIndexStream struct{ h *hybridStream }

func newDictionaryIndexStream(data []byte) (*dictionaryIndexStream, error) {
	if len(data) == 0 {
		return nil, errors.New("parquet: missing dictionary index bit width")
	}
	h, err := newHybridStream(data[1:], int(data[0]))
	if err != nil {
		return nil, err
	}
	return &dictionaryIndexStream{h: h}, nil
}

func (s *dictionaryIndexStream) Next() (uint32, error) { return s.h.Next() }

package parquet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"

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
		v := packedWord(s.packed, s.packedBit, s.bitWidth)
		s.packedBit += s.bitWidth
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
	if groups > int(^uint(0)>>1)/8 || (s.bitWidth > 0 && groups > (len(s.data)-s.off)/s.bitWidth) {
		return 0, errors.New("parquet: invalid bit-packed run length")
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
	v := int(packedWord(s.packed, s.bitPos, s.bitWidth))
	s.bitPos += s.bitWidth
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
		raw := binary.LittleEndian.Uint32(s.data[s.off : s.off+4])
		s.off += 4
		if dst.Type.ID == coretypes.Decimal {
			return setDecimalFromInt64(dst, row, int64(int32(raw)), 9)
		}
		return setInt32Logical(dst, row, raw)
	case TypeInt64:
		if s.off+8 > len(s.data) {
			return errors.New("parquet: int64 stream exhausted")
		}
		raw := binary.LittleEndian.Uint64(s.data[s.off : s.off+8])
		s.off += 8
		if dst.Type.ID == coretypes.Decimal {
			return setDecimalFromInt64(dst, row, int64(raw), 18)
		}
		if dst.Type.IsUnsignedInteger() {
			dst.SetUint64(row, raw)
			return nil
		}
		v := int64(raw)
		if dst.Type.ID == coretypes.TimestampType && s.leaf != nil {
			e := s.leaf.Element
			if e.Logical != nil && e.Logical.Kind == LogicalTimestamp && e.Logical.Timestamp != nil {
				sourceUnit := e.Logical.Timestamp.Unit
				switch {
				case sourceUnit == UnitMillis && dst.Type.Timestamp.Unit == coretypes.TimestampMicros:
					var err error
					v, err = checkedMul1000(v)
					if err != nil {
						return err
					}
				case sourceUnit == UnitNanos && dst.Type.Timestamp.Unit == coretypes.TimestampNanos:
					// Preserve nanoseconds exactly; no implicit /1000 conversion.
				case sourceUnit == UnitMicros && dst.Type.Timestamp.Unit == coretypes.TimestampMicros:
				default:
					return errors.New("parquet: timestamp source/destination unit mismatch")
				}
			} else if e.Converted != nil && *e.Converted == ConvertedTimestampMillis {
				var err error
				v, err = checkedMul1000(v)
				if err != nil {
					return err
				}
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
			return setDecimalBigEndian(dst, row, value)
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
		return setDecimalBigEndian(dst, row, value)
	default:
		return fmt.Errorf("parquet: physical value stream %d not implemented", s.physical)
	}
}

func setInt32Logical(dst *vector.Vector, row int, raw uint32) error {
	if dst.Type.IsUnsignedInteger() {
		switch dst.Type.ID {
		case coretypes.TinyInt:
			if raw > math.MaxUint8 {
				return errors.New("parquet: UTINYINT value out of range")
			}
			dst.SetUint8(row, uint8(raw))
		case coretypes.SmallInt:
			if raw > math.MaxUint16 {
				return errors.New("parquet: USMALLINT value out of range")
			}
			dst.SetUint16(row, uint16(raw))
		case coretypes.Integer:
			dst.SetUint32(row, raw)
		default:
			return errors.New("parquet: unsigned INT32 logical type mismatch")
		}
		return nil
	}
	v := int32(raw)
	switch dst.Type.ID {
	case coretypes.TinyInt:
		if v < math.MinInt8 || v > math.MaxInt8 {
			return errors.New("parquet: TINYINT value out of range")
		}
		dst.SetInt8(row, int8(v))
	case coretypes.SmallInt:
		if v < math.MinInt16 || v > math.MaxInt16 {
			return errors.New("parquet: SMALLINT value out of range")
		}
		dst.SetInt16(row, int16(v))
	case coretypes.Integer, coretypes.DateType:
		dst.SetInt32(row, v)
	default:
		return errors.New("parquet: INT32 logical type mismatch")
	}
	return nil
}

type uint128Magnitude struct {
	hi uint64
	lo uint64
}

var decimalPow10 = buildDecimalPow10()

func buildDecimalPow10() [39]uint128Magnitude {
	var result [39]uint128Magnitude
	result[0] = uint128Magnitude{lo: 1}
	for i := 1; i < len(result); i++ {
		loCarry, lo := bits.Mul64(result[i-1].lo, 10)
		hiCarry, hiLow := bits.Mul64(result[i-1].hi, 10)
		hi, carry := bits.Add64(loCarry, hiLow, 0)
		if hiCarry != 0 || carry != 0 {
			panic("parquet: internal decimal power table overflow")
		}
		result[i] = uint128Magnitude{hi: hi, lo: lo}
	}
	return result
}

func int128Magnitude(value coretypes.Int128) uint128Magnitude {
	if value.Hi >= 0 {
		return uint128Magnitude{hi: uint64(value.Hi), lo: value.Lo}
	}
	lo := ^value.Lo + 1
	hi := ^uint64(value.Hi)
	if lo == 0 {
		hi++
	}
	return uint128Magnitude{hi: hi, lo: lo}
}

func decimalFitsPrecision(value coretypes.Int128, precision uint8) bool {
	if precision == 0 || precision > 38 {
		return false
	}
	magnitude := int128Magnitude(value)
	limit := decimalPow10[precision]
	return magnitude.hi < limit.hi || magnitude.hi == limit.hi && magnitude.lo < limit.lo
}

func setDecimalFromInt64(dst *vector.Vector, row int, value int64, maxPhysicalPrecision uint8) error {
	if dst.Type.ID != coretypes.Decimal || dst.Type.Decimal.Width == 0 || dst.Type.Decimal.Width > maxPhysicalPrecision {
		return fmt.Errorf("parquet: INT decimal physical type cannot represent DECIMAL(%d,%d)", dst.Type.Decimal.Width, dst.Type.Decimal.Scale)
	}
	if !decimalFitsPrecision(coretypes.Int128FromInt64(value), dst.Type.Decimal.Width) {
		return fmt.Errorf("parquet: decimal value exceeds declared precision %d", dst.Type.Decimal.Width)
	}
	dst.SetInt64(row, value)
	return nil
}

func setDecimalBigEndian(dst *vector.Vector, row int, src []byte) error {
	if dst.Type.ID != coretypes.Decimal || dst.Type.Decimal.Width == 0 || dst.Type.Decimal.Width > 38 {
		return fmt.Errorf("parquet: invalid DECIMAL(%d,%d) destination", dst.Type.Decimal.Width, dst.Type.Decimal.Scale)
	}
	if dst.Type.Physical() == coretypes.PhysicalInt128 {
		value, err := coretypes.Int128FromBigEndianTwosComplement(src)
		if err != nil {
			return fmt.Errorf("parquet: decimal128 decode: %w", err)
		}
		if !decimalFitsPrecision(value, dst.Type.Decimal.Width) {
			return fmt.Errorf("parquet: decimal value exceeds declared precision %d", dst.Type.Decimal.Width)
		}
		dst.SetInt128(row, value)
		return nil
	}
	value, err := decodeDecimal64BigEndian(src, dst.Type)
	if err != nil {
		return err
	}
	dst.SetInt64(row, value)
	return nil
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
	var raw uint64
	for _, b := range src {
		raw = raw<<8 | uint64(b)
	}
	if negative && len(src) < 8 {
		raw |= ^uint64(0) << uint(len(src)*8)
	}
	value := int64(raw)
	if !decimalFitsPrecision(coretypes.Int128FromInt64(value), typ.Decimal.Width) {
		return 0, fmt.Errorf("parquet: decimal value exceeds declared precision %d", typ.Decimal.Width)
	}
	return value, nil
}

func checkedMul1000(value int64) (int64, error) {
	const maxInt64 = int64(^uint64(0) >> 1)
	const minInt64 = -maxInt64 - 1
	if value > maxInt64/1000 || value < minInt64/1000 {
		return 0, errors.New("parquet: timestamp conversion overflows microseconds")
	}
	return value * 1000, nil
}

type dictionaryIndexStream struct {
	h   *hybridStream
	err error
}

func newDictionaryIndexStream(data []byte) *dictionaryIndexStream {
	s := &dictionaryIndexStream{}
	if len(data) == 0 {
		s.err = errors.New("parquet: missing dictionary index bit width")
		return s
	}
	s.h, s.err = newHybridStream(data[1:], int(data[0]))
	return s
}

func (s *dictionaryIndexStream) Next() (uint32, error) {
	if s == nil {
		return 0, errors.New("parquet: nil dictionary index stream")
	}
	if s.err != nil {
		return 0, s.err
	}
	if s.h == nil {
		return 0, errors.New("parquet: dictionary index stream is not initialized")
	}
	return s.h.Next()
}

// Read fills an output batch a run at a time, retaining the same stream state
// as repeated Next calls, including packed padding across batch boundaries.
func (s *dictionaryIndexStream) Read(dst []uint32) error {
	if s == nil {
		return errors.New("parquet: nil dictionary index stream")
	}
	if s.err != nil {
		return s.err
	}
	if s.h == nil {
		return errors.New("parquet: dictionary index stream is not initialized")
	}
	h := s.h
	for len(dst) > 0 {
		if h.runRemain > 0 {
			n := min(len(dst), h.runRemain)
			for i := 0; i < n; i++ {
				dst[i] = h.runValue
			}
			h.runRemain -= n
			dst = dst[n:]
		} else if h.packedLeft > 0 {
			n := min(len(dst), h.packedLeft)
			for i := 0; i < n; i++ {
				dst[i] = packedWord(h.packed, h.packedBit+i*h.bitWidth, h.bitWidth)
			}
			h.packedBit += n * h.bitWidth
			h.packedLeft -= n
			dst = dst[n:]
		} else {
			value, err := h.Next()
			if err != nil {
				return err
			}
			dst[0] = value
			dst = dst[1:]
		}
	}
	return nil
}

// Read up to 32 packed bits using byte/word loads instead of a per-bit loop.
func packedWord(data []byte, bitPosition, width int) uint32 {
	if width == 0 {
		return 0
	}
	start, shift := bitPosition>>3, uint(bitPosition&7)
	n := (width + int(shift) + 7) / 8
	var word uint64
	if len(data)-start >= 8 {
		word = binary.LittleEndian.Uint64(data[start : start+8])
	} else {
		for i := 0; i < n; i++ {
			word |= uint64(data[start+i]) << uint(i*8)
		}
	}
	return uint32((word >> shift) & ((uint64(1) << uint(width)) - 1))
}

package parquet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
)

func levelBitWidth(maxLevel int) int {
	if maxLevel <= 0 {
		return 0
	}
	return bits.Len(uint(maxLevel))
}

func DecodeLevelsV1(data []byte, maxLevel, count int, enc Encoding) ([]int16, int, error) {
	if count < 0 {
		return nil, 0, errors.New("parquet: negative level count")
	}
	if maxLevel == 0 {
		return make([]int16, count), 0, nil
	}
	switch enc {
	case EncodingRLE:
		if len(data) < 4 {
			return nil, 0, errors.New("parquet: truncated level length")
		}
		n := int(binary.LittleEndian.Uint32(data[:4]))
		if n < 0 || n > len(data)-4 {
			return nil, 0, errors.New("parquet: invalid level stream length")
		}
		vals, _, err := decodeHybrid(data[4:4+n], levelBitWidth(maxLevel), count)
		if err != nil {
			return nil, 0, err
		}
		out := make([]int16, len(vals))
		for i, v := range vals {
			out[i] = int16(v)
		}
		return out, 4 + n, nil
	case EncodingBitPacked:
		vals, used, err := decodeBitPacked(data, levelBitWidth(maxLevel), count)
		if err != nil {
			return nil, 0, err
		}
		out := make([]int16, len(vals))
		for i, v := range vals {
			out[i] = int16(v)
		}
		return out, used, nil
	default:
		return nil, 0, fmt.Errorf("parquet: unsupported level encoding %d", enc)
	}
}

func DecodeLevelsV2(data []byte, maxLevel, count int) ([]int16, error) {
	if maxLevel == 0 {
		return make([]int16, count), nil
	}
	vals, _, err := decodeHybrid(data, levelBitWidth(maxLevel), count)
	if err != nil {
		return nil, err
	}
	out := make([]int16, len(vals))
	for i, v := range vals {
		out[i] = int16(v)
	}
	return out, nil
}

func DecodeDictionaryIndexes(data []byte, count int) ([]uint32, error) {
	if count == 0 {
		return nil, nil
	}
	if len(data) == 0 {
		return nil, errors.New("parquet: missing dictionary index bit width")
	}
	width := int(data[0])
	if width > 32 {
		return nil, fmt.Errorf("parquet: invalid dictionary bit width %d", width)
	}
	vals, _, err := decodeHybrid(data[1:], width, count)
	return vals, err
}

func decodeHybrid(data []byte, bitWidth, count int) ([]uint32, int, error) {
	if bitWidth < 0 || bitWidth > 32 {
		return nil, 0, fmt.Errorf("parquet: invalid bit width %d", bitWidth)
	}
	out := make([]uint32, 0, count)
	off := 0
	for len(out) < count {
		if off >= len(data) {
			return nil, 0, errors.New("parquet: truncated hybrid stream")
		}
		header, n := binary.Uvarint(data[off:])
		if n <= 0 {
			return nil, 0, errors.New("parquet: truncated hybrid header")
		}
		off += n
		if header&1 == 0 {
			run := int(header >> 1)
			if run <= 0 {
				return nil, 0, errors.New("parquet: invalid zero RLE run")
			}
			byteWidth := (bitWidth + 7) / 8
			if off+byteWidth > len(data) {
				return nil, 0, errors.New("parquet: truncated RLE value")
			}
			var v uint32
			for i := 0; i < byteWidth; i++ {
				v |= uint32(data[off+i]) << (8 * i)
			}
			off += byteWidth
			for i := 0; i < run && len(out) < count; i++ {
				out = append(out, v)
			}
		} else {
			groups := int(header >> 1)
			if groups <= 0 {
				return nil, 0, errors.New("parquet: invalid zero bit-packed run")
			}
			values := groups * 8
			bytesNeeded := groups * bitWidth
			if off+bytesNeeded > len(data) {
				return nil, 0, errors.New("parquet: truncated bit-packed run")
			}
			vals, _, err := decodeBitPacked(data[off:off+bytesNeeded], bitWidth, values)
			if err != nil {
				return nil, 0, err
			}
			off += bytesNeeded
			for _, v := range vals {
				if len(out) < count {
					out = append(out, v)
				}
			}
		}
	}
	return out, off, nil
}

func decodeBitPacked(data []byte, bitWidth, count int) ([]uint32, int, error) {
	if bitWidth == 0 {
		return make([]uint32, count), 0, nil
	}
	bitsNeeded := bitWidth * count
	bytesNeeded := (bitsNeeded + 7) / 8
	if bytesNeeded > len(data) {
		return nil, 0, errors.New("parquet: truncated bit-packed values")
	}
	out := make([]uint32, count)
	bitPos := 0
	for i := 0; i < count; i++ {
		var v uint32
		for b := 0; b < bitWidth; b++ {
			byteIdx := bitPos >> 3
			bitIdx := bitPos & 7
			if data[byteIdx]&(1<<bitIdx) != 0 {
				v |= 1 << b
			}
			bitPos++
		}
		out[i] = v
	}
	return out, bytesNeeded, nil
}

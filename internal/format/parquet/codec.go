package parquet

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
)

func Decompress(codec CompressionCodec, src []byte, expected int) ([]byte, error) {
	switch codec {
	case CodecUncompressed:
		if expected >= 0 && len(src) != expected {
			return nil, fmt.Errorf("parquet: uncompressed size %d want %d", len(src), expected)
		}
		return append([]byte(nil), src...), nil
	case CodecGzip:
		zr, err := gzip.NewReader(bytes.NewReader(src))
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		limit := int64(expected) + 1
		if expected < 0 {
			limit = 64 << 20
		}
		out, err := io.ReadAll(io.LimitReader(zr, limit))
		if err != nil {
			return nil, err
		}
		if expected >= 0 && len(out) != expected {
			return nil, fmt.Errorf("parquet: gzip size %d want %d", len(out), expected)
		}
		return out, nil
	case CodecSnappy:
		return decodeSnappyBlock(src, expected)
	default:
		return nil, fmt.Errorf("parquet: unsupported compression codec %d", codec)
	}
}

func decodeSnappyBlock(src []byte, expected int) ([]byte, error) {
	decodedLen, n := readSnappyUvarint(src)
	if n <= 0 {
		return nil, errors.New("parquet: invalid snappy length")
	}
	if expected >= 0 && decodedLen != expected {
		return nil, fmt.Errorf("parquet: snappy declared size %d want %d", decodedLen, expected)
	}
	out := make([]byte, 0, decodedLen)
	i := n
	for i < len(src) && len(out) < decodedLen {
		tag := src[i]
		i++
		switch tag & 3 {
		case 0:
			l := int(tag >> 2)
			if l < 60 {
				l++
			} else {
				nb := l - 59
				if nb < 1 || nb > 4 || i+nb > len(src) {
					return nil, errors.New("parquet: invalid snappy literal length")
				}
				l = 1
				for j := 0; j < nb; j++ {
					l += int(src[i+j]) << (8 * j)
				}
				i += nb
			}
			if l < 0 || i+l > len(src) || len(out)+l > decodedLen {
				return nil, errors.New("parquet: truncated snappy literal")
			}
			out = append(out, src[i:i+l]...)
			i += l
		case 1:
			if i >= len(src) {
				return nil, errors.New("parquet: truncated snappy copy1")
			}
			l := 4 + int((tag>>2)&7)
			off := int(tag&0xe0)<<3 | int(src[i])
			i++
			var err error
			out, err = snappyCopy(out, off, l, decodedLen)
			if err != nil {
				return nil, err
			}
		case 2:
			if i+2 > len(src) {
				return nil, errors.New("parquet: truncated snappy copy2")
			}
			l := 1 + int(tag>>2)
			off := int(src[i]) | int(src[i+1])<<8
			i += 2
			var err error
			out, err = snappyCopy(out, off, l, decodedLen)
			if err != nil {
				return nil, err
			}
		case 3:
			if i+4 > len(src) {
				return nil, errors.New("parquet: truncated snappy copy4")
			}
			l := 1 + int(tag>>2)
			off := int(src[i]) | int(src[i+1])<<8 | int(src[i+2])<<16 | int(src[i+3])<<24
			i += 4
			var err error
			out, err = snappyCopy(out, off, l, decodedLen)
			if err != nil {
				return nil, err
			}
		}
	}
	if len(out) != decodedLen {
		return nil, fmt.Errorf("parquet: snappy output size %d want %d", len(out), decodedLen)
	}
	return out, nil
}

func readSnappyUvarint(src []byte) (int, int) {
	var x uint64
	for i, b := range src {
		if i >= 5 {
			return 0, -1
		}
		if b < 0x80 {
			x |= uint64(b) << (7 * i)
			if x > uint64(^uint(0)>>1) {
				return 0, -1
			}
			return int(x), i + 1
		}
		x |= uint64(b&0x7f) << (7 * i)
	}
	return 0, 0
}

func snappyCopy(out []byte, off, l, limit int) ([]byte, error) {
	if off <= 0 || off > len(out) || l < 0 || l > limit-len(out) {
		return nil, errors.New("parquet: invalid snappy copy offset")
	}
	start := len(out)
	if l > cap(out)-start {
		out = append(out, make([]byte, l)...)
	} else {
		out = out[:start+l]
	}
	copySnappyMatch(out, start, off, l)
	return out, nil
}

// A Snappy match may overlap its own output (offset 1 is a repeated byte).
// Seed from initialized bytes, then double the available pattern on each copy.
// Each copy reads only bytes already produced by this or an earlier match.
func copySnappyMatch(dst []byte, start, offset, length int) {
	if length == 0 {
		return
	}
	filled := copy(dst[start:start+min(offset, length)], dst[start-offset:start])
	for filled < length {
		filled += copy(dst[start+filled:start+length], dst[start:start+filled])
	}
}

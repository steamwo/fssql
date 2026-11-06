package parquet

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/format/compression"
)

func DecompressManaged(scope *memory.Scope, bm *memory.BufferManager, codec CompressionCodec, src []byte, expected int) (*memory.Buffer, error) {
	if scope == nil || bm == nil {
		return nil, errors.New("parquet: managed decompression requires scope and buffer manager")
	}
	if expected < 0 {
		return nil, errors.New("parquet: negative expected decompressed size")
	}
	out, err := bm.Allocate(scope, memory.TagParquetDecode, expected)
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			out.Release()
		}
	}()
	switch codec {
	case CodecUncompressed:
		if len(src) != expected {
			return nil, fmt.Errorf("parquet: uncompressed size %d want %d", len(src), expected)
		}
		copy(out.Bytes(), src)
	case CodecGzip:
		zr, err := gzip.NewReader(bytes.NewReader(src))
		if err != nil {
			return nil, err
		}
		n, readErr := io.ReadFull(zr, out.Bytes())
		if readErr != nil {
			_ = zr.Close()
			return nil, fmt.Errorf("parquet: gzip short decode %d/%d: %w", n, expected, readErr)
		}
		var extra [1]byte
		extraN, extraErr := zr.Read(extra[:])
		closeErr := zr.Close()
		if extraN != 0 || (extraErr != nil && extraErr != io.EOF) {
			return nil, errors.New("parquet: gzip decoded more than expected")
		}
		if closeErr != nil {
			return nil, closeErr
		}
	case CodecSnappy:
		if err := decodeSnappyInto(out.Bytes(), src); err != nil {
			return nil, err
		}
	case CodecZSTD:
		if err := readExactDecoded("zstd", compression.NewZstdReader(bytes.NewReader(src)), out.Bytes()); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("parquet: unsupported compression codec %d", codec)
	}
	keep = true
	return out, nil
}

func readExactDecoded(name string, r io.Reader, dst []byte) error {
	n, err := io.ReadFull(r, dst)
	if err != nil {
		return fmt.Errorf("parquet: %s short decode %d/%d: %w", name, n, len(dst), err)
	}
	var extra [1]byte
	extraN, extraErr := r.Read(extra[:])
	if extraN != 0 || (extraErr != nil && extraErr != io.EOF) {
		return fmt.Errorf("parquet: %s decoded more than expected", name)
	}
	return nil
}

func decodeSnappyInto(dst, src []byte) error {
	decodedLen, n := readSnappyUvarint(src)
	if n <= 0 || decodedLen != len(dst) {
		return fmt.Errorf("parquet: snappy decoded size %d want %d", decodedLen, len(dst))
	}
	i, out := n, 0
	copyBack := func(off, l int) error {
		if off <= 0 || off > out || l < 0 || l > len(dst)-out {
			return errors.New("parquet: invalid snappy copy offset")
		}
		copySnappyMatch(dst, out, off, l)
		out += l
		return nil
	}
	for i < len(src) && out < len(dst) {
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
					return errors.New("parquet: invalid snappy literal length")
				}
				l = 1
				for j := 0; j < nb; j++ {
					l += int(src[i+j]) << (8 * j)
				}
				i += nb
			}
			if i+l > len(src) || out+l > len(dst) {
				return errors.New("parquet: truncated snappy literal")
			}
			copy(dst[out:out+l], src[i:i+l])
			i += l
			out += l
		case 1:
			if i >= len(src) {
				return errors.New("parquet: truncated snappy copy1")
			}
			l := 4 + int((tag>>2)&7)
			off := int(tag&0xe0)<<3 | int(src[i])
			i++
			if err := copyBack(off, l); err != nil {
				return err
			}
		case 2:
			if i+2 > len(src) {
				return errors.New("parquet: truncated snappy copy2")
			}
			l := 1 + int(tag>>2)
			off := int(src[i]) | int(src[i+1])<<8
			i += 2
			if err := copyBack(off, l); err != nil {
				return err
			}
		case 3:
			if i+4 > len(src) {
				return errors.New("parquet: truncated snappy copy4")
			}
			l := 1 + int(tag>>2)
			off := int(src[i]) | int(src[i+1])<<8 | int(src[i+2])<<16 | int(src[i+3])<<24
			i += 4
			if err := copyBack(off, l); err != nil {
				return err
			}
		}
	}
	if out != len(dst) {
		return fmt.Errorf("parquet: snappy output %d want %d", out, len(dst))
	}
	return nil
}

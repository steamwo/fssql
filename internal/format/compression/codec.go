package compression

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/format/compression/internalzstd"
	"github.com/steamwo/fssql/internal/format/scan"
)

const DefaultMaxOutputBytes int64 = 512 << 20

type Codec interface {
	Name() string
	Capability() scan.CompressionCapability
	Decompress(context.Context, []byte, int64, *memory.Scope, *memory.BufferManager) (*memory.Buffer, error)
}

type identityCodec struct{}
type gzipCodec struct{}
type zstdCodec struct{}

func Identity() Codec                                        { return identityCodec{} }
func Gzip() Codec                                            { return gzipCodec{} }
func Zstd() Codec                                            { return zstdCodec{} }
func (identityCodec) Name() string                           { return "identity" }
func (identityCodec) Capability() scan.CompressionCapability { return scan.CompressionIdentity }
func (gzipCodec) Name() string                               { return "gzip" }
func (gzipCodec) Capability() scan.CompressionCapability     { return scan.CompressionSequential }
func (zstdCodec) Name() string                               { return "zstd" }
func (zstdCodec) Capability() scan.CompressionCapability     { return scan.CompressionSequential }

func (identityCodec) Decompress(ctx context.Context, src []byte, max int64, scope *memory.Scope, bm *memory.BufferManager) (*memory.Buffer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if scope == nil || bm == nil {
		return nil, errors.New("compression: managed memory required")
	}
	if max <= 0 {
		max = DefaultMaxOutputBytes
	}
	if int64(len(src)) > max {
		return nil, fmt.Errorf("compression: output limit exceeded: %d > %d", len(src), max)
	}
	out, err := bm.Allocate(scope, memory.TagCompression, len(src))
	if err != nil {
		return nil, err
	}
	copy(out.Bytes(), src)
	return out, nil
}
func (gzipCodec) Decompress(ctx context.Context, src []byte, max int64, scope *memory.Scope, bm *memory.BufferManager) (*memory.Buffer, error) {
	if scope == nil || bm == nil {
		return nil, errors.New("compression: managed memory required")
	}
	work, err := scope.Reserve(memory.TagCompression, 1<<20)
	if err != nil {
		return nil, err
	}
	defer work.Release()
	zr, err := gzip.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	return readManaged(ctx, zr, max, scope, bm)
}
func (zstdCodec) Decompress(ctx context.Context, src []byte, max int64, scope *memory.Scope, bm *memory.BufferManager) (*memory.Buffer, error) {
	if scope == nil || bm == nil {
		return nil, errors.New("compression: managed memory required")
	}
	work, err := scope.Reserve(memory.TagCompression, 9<<20)
	if err != nil {
		return nil, err
	}
	defer work.Release()
	zr := internalzstd.NewReader(bytes.NewReader(src))
	return readManaged(ctx, zr, max, scope, bm)
}

func readManaged(ctx context.Context, r io.Reader, max int64, scope *memory.Scope, bm *memory.BufferManager) (*memory.Buffer, error) {
	if scope == nil || bm == nil {
		return nil, errors.New("compression: managed memory required")
	}
	if max <= 0 {
		max = DefaultMaxOutputBytes
	}
	initial := 64 << 10
	if int64(initial) > max {
		initial = int(max)
	}
	if initial < 1 {
		initial = 1
	}
	out, err := bm.Allocate(scope, memory.TagCompression, initial)
	if err != nil {
		return nil, err
	}
	used := 0
	fail := func(e error) (*memory.Buffer, error) { out.Release(); return nil, e }
	var scratch [32 << 10]byte
	for {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		n, readErr := r.Read(scratch[:])
		if n > 0 {
			if int64(used+n) > max {
				return fail(fmt.Errorf("compression: output limit exceeded: max=%d", max))
			}
			need := used + n
			if need > out.Len() {
				next := out.Len() * 2
				if next < need {
					next = need
				}
				if int64(next) > max {
					next = int(max)
				}
				if err := out.Resize(next, nil); err != nil {
					return fail(err)
				}
			}
			copy(out.Bytes()[used:need], scratch[:n])
			used = need
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fail(readErr)
		}
		if n == 0 {
			return fail(io.ErrNoProgress)
		}
	}
	if err := out.Resize(used, nil); err != nil {
		return fail(err)
	}
	return out, nil
}

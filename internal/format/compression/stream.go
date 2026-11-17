package compression

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/format/compression/internalzstd"
)

// NewManagedReader reuses the existing streaming codecs with a workspace
// reservation acquired before decoder allocation. Close releases only decoder
// state; input ownership remains with the transform pipeline.
func NewManagedReader(ctx context.Context, kind string, input io.Reader, scope *memory.Scope) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	work, err := scope.Reserve(memory.TagCompression, 1<<20)
	if err != nil {
		return nil, err
	}
	d := &managedReader{work: work, ctx: ctx}
	switch kind {
	case "gzip":
		gz, err := gzip.NewReader(input)
		if err != nil {
			work.Release()
			return nil, err
		}
		d.reader, d.closeDecoder = gz, gz.Close
	case "zstd":
		zr := internalzstd.NewReader(input)
		zr.WindowResize = func(old, size int) (func(), error) {
			capacity := max(old, size)
			peak := capacity
			if size > old {
				peak += old
			}
			if err := work.Resize((1 << 20) + int64(peak)); err != nil {
				return nil, err
			}
			return func() { _ = work.Resize((1 << 20) + int64(capacity)) }, nil
		}
		d.reader = zr
	default:
		work.Release()
		return nil, errors.New("compression: unsupported streaming decoder")
	}
	return d, nil
}

type managedReader struct {
	ctx          context.Context
	reader       io.Reader
	closeDecoder func() error
	work         *memory.Reservation
	once         sync.Once
	err          error
}

func (d *managedReader) Read(p []byte) (int, error) {
	if err := d.ctx.Err(); err != nil {
		return 0, err
	}
	return d.reader.Read(p)
}
func (d *managedReader) Close() error {
	d.once.Do(func() {
		if d.closeDecoder != nil {
			d.err = d.closeDecoder()
		}
		d.reader = nil
		d.work.Release()
	})
	return d.err
}

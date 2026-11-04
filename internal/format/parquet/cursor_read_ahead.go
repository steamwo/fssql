package parquet

import (
	"context"
	"errors"

	"github.com/steamwo/fssql/internal/core/memory"
)

func (c *ColumnCursor) releaseHeaderTail() {
	if c.headerTail != nil {
		c.headerTail.Release()
		c.headerTail = nil
	}
	c.headerSize = 0
}

// Piggyback a bounded next-header probe on the current page-body request.
// A separate managed copy keeps the probe alive after the current page closes.
// Reads remain inside this column chunk and never prefetch a complete file.
func (c *ColumnCursor) readPageBody(ctx context.Context, offset, length int64) (*memory.Buffer, error) {
	next := offset + length // Bounds were checked by loadPage before this call.
	extra := min(pageHeaderPiggyback, c.end-next)
	if extra <= 0 {
		c.releaseHeaderTail()
		return c.readRange(ctx, offset, length, memory.TagParquetCompressed)
	}
	if c.headerTail == nil {
		buffer, err := c.bm.Allocate(c.scope, memory.TagParquetRange, int(pageHeaderPiggyback))
		if err != nil {
			if errors.Is(err, memory.ErrLimitExceeded) {
				return c.readRange(ctx, offset, length, memory.TagParquetCompressed)
			}
			return nil, err
		}
		c.headerTail = buffer
	}
	c.headerSize = 0
	body, err := c.readRange(ctx, offset, length+extra, memory.TagParquetCompressed)
	if err != nil {
		c.releaseHeaderTail()
		if errors.Is(err, memory.ErrLimitExceeded) {
			// Optional lookahead must not make a page fail when the original
			// exact-body request still fits. Real I/O errors are never retried.
			return c.readRange(ctx, offset, length, memory.TagParquetCompressed)
		}
		return nil, err
	}
	copy(c.headerTail.Bytes(), body.Bytes()[int(length):])
	c.headerAt, c.headerSize = next, int(extra)
	return body, nil
}

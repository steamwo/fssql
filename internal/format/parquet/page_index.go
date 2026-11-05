package parquet

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/format/scan"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

const maxPageIndexBytes = 4 << 20
const maxIndexedPages = 65536

type pageIndexRef struct {
	offset int64
	length int32
}
type indexedPage struct {
	offset, firstRow   int64
	size               int32
	nullPage, hasNulls bool
	nulls              int64
	min, max           []byte
}

type cursorPageIndex struct {
	offsetRef, columnRef                  pageIndexRef
	offsetTried, columnTried, columnValid bool
	pages                                 []indexedPage
	buffers                               []*memory.Buffer
	reservation                           *memory.Reservation
}

func (i *cursorPageIndex) close() {
	for _, buffer := range i.buffers {
		buffer.Release()
	}
	if i.reservation != nil {
		i.reservation.Release()
	}
	i.buffers, i.pages, i.reservation = nil, nil, nil
}

func (c *ColumnCursor) configurePageIndex(chunk ColumnChunk) {
	if c.leaf.MaxRep != 0 || chunk.OffsetIndexOffset == nil || chunk.OffsetIndexLength == nil {
		return
	}
	c.index = &cursorPageIndex{offsetRef: pageIndexRef{*chunk.OffsetIndexOffset, *chunk.OffsetIndexLength}}
	if chunk.ColumnIndexOffset != nil && chunk.ColumnIndexLength != nil {
		c.index.columnRef = pageIndexRef{*chunk.ColumnIndexOffset, *chunk.ColumnIndexLength}
	}
}

func (c *ColumnCursor) validIndexRef(ref pageIndexRef) bool {
	if !c.identity.Stable() || c.identity.Size <= 0 || c.end > c.identity.Size || ref.offset < 0 || ref.length <= 0 || ref.length > maxPageIndexBytes || int64(ref.length) > c.identity.Size || ref.offset > c.identity.Size-int64(ref.length) {
		return false
	}
	start := c.meta.DataPageOffset
	if c.meta.DictionaryPageOffset != nil {
		start = min(start, *c.meta.DictionaryPageOffset)
	}
	return ref.offset >= c.end || ref.offset+int64(ref.length) <= start
}

func (c *ColumnCursor) readIndex(ctx context.Context, ref pageIndexRef) (*memory.Buffer, error) {
	identity := c.identity
	result, err := c.driver.ReadRange(ctx, vfs.RangeRequest{URI: c.uri, Offset: ref.offset, Length: int64(ref.length), Expected: &identity, Tag: memory.TagMetadata}, c.scope, c.bm)
	c.metrics.Requests++
	c.metrics.PlannedBytes += int64(ref.length)
	if result.Buffer != nil {
		c.metrics.TransferredBytes += int64(result.Buffer.Len())
	}
	if err != nil {
		if result.Buffer != nil {
			result.Buffer.Release()
		}
		return nil, err
	}
	if result.Buffer == nil || result.Buffer.Len() != int(ref.length) {
		if result.Buffer != nil {
			result.Buffer.Release()
		}
		return nil, vfs.ErrShortRead
	}
	c.metrics.UsefulBytes += int64(ref.length)
	return result.Buffer, nil
}

// Optional metadata is transactional: no index is used before its shape, row
// coverage and file/chunk bounds have been validated. I/O errors stay fatal.
func (c *ColumnCursor) ensurePageIndex(ctx context.Context, column bool) error {
	i := c.index
	if i == nil {
		return nil
	}
	if !i.offsetTried {
		i.offsetTried = true
		if !c.validIndexRef(i.offsetRef) {
			return nil
		}
		buffer, err := c.readIndex(ctx, i.offsetRef)
		if errors.Is(err, memory.ErrLimitExceeded) {
			return nil
		}
		if err != nil {
			return err
		}
		pages, reservation, err := decodeOffsetIndex(buffer.Bytes(), c)
		if err != nil {
			buffer.Release()
			if reservation != nil {
				reservation.Release()
			}
			return nil
		}
		i.pages, i.reservation = pages, reservation
		i.buffers = append(i.buffers, buffer)
	}
	if !column || len(i.pages) == 0 || i.columnTried {
		return nil
	}
	i.columnTried = true
	if !c.validIndexRef(i.columnRef) {
		return nil
	}
	if i.columnRef.offset < i.offsetRef.offset+int64(i.offsetRef.length) && i.offsetRef.offset < i.columnRef.offset+int64(i.columnRef.length) {
		return nil
	}
	buffer, err := c.readIndex(ctx, i.columnRef)
	if errors.Is(err, memory.ErrLimitExceeded) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := decodeColumnIndex(buffer.Bytes(), i.pages, c.meta.NumValues); err != nil {
		for position := range i.pages {
			i.pages[position].min = nil
			i.pages[position].max = nil
			i.pages[position].nullPage = false
			i.pages[position].hasNulls = false
		}
		buffer.Release()
		return nil
	}
	i.buffers = append(i.buffers, buffer)
	i.columnValid = true
	return nil
}

func (c *ColumnCursor) indexedPageAtRow() (int, int64, bool) {
	if c.index == nil || len(c.index.pages) == 0 || c.page != nil || c.hasPending {
		return 0, 0, false
	}
	pages := c.index.pages
	position := sort.Search(len(pages), func(i int) bool { return pages[i].firstRow >= c.consumed })
	if position == len(pages) || pages[position].firstRow != c.consumed {
		return 0, 0, false
	}
	end := c.meta.NumValues
	if position+1 < len(pages) {
		end = pages[position+1].firstRow
	}
	return position, end - c.consumed, true
}

func (c *ColumnCursor) skipIndexedPage(position int, rows int64) {
	page := c.index.pages[position]
	c.offset = page.offset + int64(page.size)
	c.consumed += rows
	c.metrics.PagesSkipped++
	c.cachedHeader, c.cachedHeaderLen = PageHeader{}, 0
	c.releaseHeaderTail()
}

func (c *ColumnCursor) skipIndexedRows(ctx context.Context, rows int64) (int64, error) {
	if err := c.ensurePageIndex(ctx, false); err != nil {
		return 0, err
	}
	skipped := int64(0)
	for rows > 0 {
		if err := ctx.Err(); err != nil {
			return skipped, err
		}
		position, count, ok := c.indexedPageAtRow()
		if !ok || count > rows {
			break
		}
		c.skipIndexedPage(position, count)
		rows -= count
		skipped += count
	}
	return skipped, nil
}

func (c *ColumnCursor) pruneIndexedPage(ctx context.Context, filter scan.Filter) (int64, int64, bool, error) {
	if err := c.ensurePageIndex(ctx, true); err != nil {
		return 0, 0, false, err
	}
	position, count, ok := c.indexedPageAtRow()
	if !ok {
		return 0, 0, false, nil
	}
	if !c.index.columnValid {
		return 0, 0, false, nil
	}
	page := c.index.pages[position]
	stats := Statistics{MinValue: page.min, MaxValue: page.max}
	if page.hasNulls {
		nulls := page.nulls
		stats.NullCount = &nulls
	}
	if page.nullPage {
		nulls := count
		stats.NullCount = &nulls
	}
	if pageStatisticsTruth(c.leaf, &stats, count, filter) != scan.AlwaysFalse {
		return 0, count, true, nil
	}
	c.skipIndexedPage(position, count)
	return count, 0, true, nil
}

// A skipped prefix may bypass the dictionary page too. Load it only when a
// retained data page actually needs decoding, without prefetching rejected pages.
func (c *ColumnCursor) prepareIndexedRead(ctx context.Context) error {
	position, _, ok := c.indexedPageAtRow()
	if !ok {
		return nil
	}
	target := c.index.pages[position].offset
	if c.dictionary == nil && c.meta.DictionaryPageOffset != nil && *c.meta.DictionaryPageOffset < c.meta.DataPageOffset {
		c.offset = *c.meta.DictionaryPageOffset
		header, length, err := c.readPageHeader(ctx)
		if err != nil {
			return err
		}
		if header.Type != PageDictionary {
			return errors.New("parquet: indexed dictionary offset is not a dictionary page")
		}
		bodyOffset := c.offset + int64(length)
		if bodyOffset < c.offset || bodyOffset > c.meta.DataPageOffset || int64(header.CompressedPageSize) > c.meta.DataPageOffset-bodyOffset {
			return errors.New("parquet: indexed dictionary exceeds data page offset")
		}
		body, err := c.readPageBody(ctx, bodyOffset, int64(header.CompressedPageSize))
		if err != nil {
			return err
		}
		err = c.loadDictionary(header, body)
		body.Release()
		if err != nil {
			return err
		}
		c.metrics.PagesRead++
		c.metrics.UsefulBytes += int64(length) + int64(header.CompressedPageSize)
	}
	c.offset = target
	return nil
}

func (c *ColumnCursor) validateIndexedHeader(header PageHeader, length int, rows int64) error {
	position, count, ok := c.indexedPageAtRow()
	if !ok {
		return nil
	}
	page := c.index.pages[position]
	if c.offset != page.offset || count != rows || int64(length)+int64(header.CompressedPageSize) != int64(page.size) {
		return fmt.Errorf("parquet: data page disagrees with offset index at row %d", c.consumed)
	}
	return nil
}

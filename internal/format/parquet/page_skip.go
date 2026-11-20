package parquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/format/scan"
)

// Compact binary fields borrow the probe buffer. Page statistics outlive that
// buffer when a header is cached or the next-header lookahead is overwritten.
func ownPageStatistics(header PageHeader) PageHeader {
	var stats *Statistics
	if header.Data != nil {
		stats = header.Data.Statistics
	}
	if header.DataV2 != nil {
		stats = header.DataV2.Statistics
	}
	if stats != nil {
		stats.Min, stats.Max = bytes.Clone(stats.Min), bytes.Clone(stats.Max)
		stats.MinValue, stats.MaxValue = bytes.Clone(stats.MinValue), bytes.Clone(stats.MaxValue)
	}
	return header
}

func (c *ColumnCursor) cachePageHeader(header PageHeader, length int) (PageHeader, int, error) {
	return c.cacheOwnedPageHeader(ownPageStatistics(header), length)
}

func (c *ColumnCursor) cacheOwnedPageHeader(header PageHeader, length int) (PageHeader, int, error) {
	c.cachedHeader, c.cachedHeaderAt, c.cachedHeaderLen = header, c.offset, length
	return header, length, nil
}

// nextDataPageHeader leaves the cursor at the data page header. Dictionaries
// must remain available for later selected pages, even when earlier pages skip.
func (c *ColumnCursor) nextDataPageHeader(ctx context.Context) (PageHeader, int, error) {
	for c.offset < c.end {
		if err := ctx.Err(); err != nil {
			return PageHeader{}, 0, err
		}
		header, length, err := c.readPageHeader(ctx)
		if err != nil {
			return PageHeader{}, 0, err
		}
		bodyOffset, bodyLen := c.offset+int64(length), int64(header.CompressedPageSize)
		if bodyOffset < c.offset || bodyOffset > c.end || bodyLen < 0 || bodyLen > c.end-bodyOffset {
			return PageHeader{}, 0, errors.New("parquet: page body exceeds column chunk bounds")
		}
		switch header.Type {
		case PageData, PageDataV2:
			return header, length, nil
		case PageDictionary:
			body, err := c.readDecodedPageBody(ctx, bodyOffset, header)
			if err != nil {
				return PageHeader{}, 0, err
			}
			err = c.loadDictionary(header, body)
			body.Release()
			if err != nil {
				return PageHeader{}, 0, err
			}
			c.metrics.PagesRead++
			c.metrics.UsefulBytes += int64(length) + bodyLen
		case PageIndex:
			// Legacy index pages are metadata, not data pages to decode.
			c.metrics.UsefulBytes += int64(length)
		default:
			return PageHeader{}, 0, fmt.Errorf("parquet: unsupported page type %d", header.Type)
		}
		c.offset = bodyOffset + bodyLen
	}
	return PageHeader{}, 0, io.ErrUnexpectedEOF
}

func (c *ColumnCursor) scalarPageInfo(header PageHeader) (int64, *Statistics, error) {
	var count int64
	var stats *Statistics
	var encoding Encoding
	switch header.Type {
	case PageData:
		if header.Data == nil {
			return 0, nil, errors.New("parquet: missing data page header")
		}
		count, stats, encoding = int64(header.Data.NumValues), header.Data.Statistics, header.Data.Encoding
	case PageDataV2:
		if header.DataV2 == nil {
			return 0, nil, errors.New("parquet: missing data v2 page header")
		}
		h := header.DataV2
		count, stats, encoding = int64(h.NumValues), h.Statistics, h.Encoding
		levels := int64(h.DefinitionLevelsByteLength) + int64(h.RepetitionLevelsByteLength)
		if h.NumNulls < 0 || h.NumNulls > h.NumValues || h.NumRows < 0 || h.NumRows > h.NumValues || h.DefinitionLevelsByteLength < 0 || h.RepetitionLevelsByteLength < 0 || levels > int64(c.pagePayloadSize(header)) || levels > int64(header.UncompressedPageSize) || (c.leaf.MaxRep == 0 && h.NumRows != h.NumValues) {
			return 0, nil, errors.New("parquet: invalid data v2 page counts or levels")
		}
	default:
		return 0, nil, errors.New("parquet: expected data page")
	}
	if count < 0 || count > c.meta.NumValues-c.consumed {
		return 0, nil, errors.New("parquet: data page exceeds remaining column values")
	}
	switch encoding {
	case EncodingPlain:
	case EncodingPlainDictionary, EncodingRLEDictionary:
		if c.dictionary == nil {
			return 0, nil, errors.New("parquet: dictionary data page without dictionary page")
		}
	default:
		return 0, nil, fmt.Errorf("parquet: unsupported data page encoding %d", encoding)
	}
	return count, stats, nil
}

func (c *ColumnCursor) advanceSkippedPage(headerLen int, bodyLen, rows int64) {
	c.offset += int64(headerLen) + bodyLen
	c.pageOrdinal++
	c.consumed += rows
	c.metrics.PagesSkipped++
	c.metrics.UsefulBytes += int64(headerLen)
}

// pruneFilterPage is checked only at a scalar page boundary. It never invents
// values for rejected rows: the scanner advances its physical row position too.
func (c *ColumnCursor) pruneFilterPage(ctx context.Context, filter scan.Filter) (int64, int64, error) {
	if c.page != nil {
		return 0, int64(c.page.remaining), nil
	}
	if skipped, available, handled, err := c.pruneIndexedPage(ctx, filter); handled || err != nil {
		return skipped, available, err
	}
	if err := c.prepareIndexedRead(ctx); err != nil {
		return 0, 0, err
	}
	header, headerLen, err := c.nextDataPageHeader(ctx)
	if err != nil {
		return 0, 0, err
	}
	count, stats, err := c.scalarPageInfo(header)
	if err != nil {
		return 0, 0, err
	}
	if pageStatisticsTruth(c.leaf, stats, count, filter) != scan.AlwaysFalse {
		return 0, count, nil
	}
	c.advanceSkippedPage(headerLen, int64(header.CompressedPageSize), count)
	return count, 0, nil
}

func pageStatisticsTruth(leaf *SchemaNode, stats *Statistics, count int64, filter scan.Filter) scan.Truth {
	if count == 0 || stats == nil || leaf.MaxRep != 0 {
		return scan.MaybeTrue
	}
	if stats.NullCount != nil && (*stats.NullCount < 0 || *stats.NullCount > count) {
		return scan.MaybeTrue
	}
	if filter.Op != scan.IsNull && filter.Op != scan.IsNotNull {
		if stats.NullCount != nil && *stats.NullCount == count && !filter.Value.Null {
			return scan.AlwaysFalse
		}
		// Floating min/max may omit NaNs; timestamps may require conversion.
		switch leaf.Logical.ID {
		case types.Boolean, types.Integer, types.BigInt, types.Varchar:
		default:
			return scan.MaybeTrue
		}
		if (stats.IsMinValueExact != nil && !*stats.IsMinValueExact) || (stats.IsMaxValueExact != nil && !*stats.IsMaxValueExact) {
			return scan.MaybeTrue
		}
		minBytes, maxBytes := statsBounds(stats)
		lower, lowerOK := decodeStatValue(leaf, minBytes)
		upper, upperOK := decodeStatValue(leaf, maxBytes)
		maximum := scan.Literal{Type: leaf.Logical, Int64: upper.i, Bool: upper.b, String: upper.s}
		if order, ok := compareLiteral(lower, maximum); !lowerOK || !upperOK || !ok || order > 0 {
			return scan.MaybeTrue
		}
	}
	meta := ColumnMetaData{PathInSchema: leaf.Path, Statistics: stats}
	return EvaluateRowGroup(&SchemaTree{Leaves: []*SchemaNode{leaf}}, RowGroup{NumRows: count, Columns: []ColumnChunk{{MetaData: &meta}}}, []scan.Filter{filter}).Truth
}

package parquet

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

const (
	initialPageHeaderProbe = int64(128)
	// Page headers carrying min/max statistics for wide strings routinely
	// exceed 128 bytes. Piggybacking a larger bounded probe avoids repeated
	// header-only range reads without ever prefetching a whole chunk.
	pageHeaderPiggyback = int64(2048)
	maxPageHeaderProbe  = int64(64 << 10)
)

var ErrRepeatedColumnRequiresLevelReader = errors.New("parquet: repeated column requires level-aware reader")

type ColumnCursorMetrics struct {
	Requests         int64
	PlannedBytes     int64
	TransferredBytes int64
	PagesRead        int64
	PagesSkipped     int64
	UsefulBytes      int64
	DataValues       int64
}

type ColumnEntry struct {
	Definition int
	Repetition int
	HasValue   bool
}

type columnPageState struct {
	header     PageHeader
	compressed *memory.Buffer
	decoded    *memory.Buffer
	def        *levelStream
	rep        *levelStream
	values     *valueStream
	dict       *dictionaryIndexStream
	remaining  int
}

func (p *columnPageState) release() {
	if p == nil {
		return
	}
	if p.compressed != nil {
		p.compressed.Release()
		p.compressed = nil
	}
	if p.decoded != nil {
		p.decoded.Release()
		p.decoded = nil
	}
}

// ColumnCursor 按列持有页面、字典与可选读取句柄，所有借用在 Close 时结束。
type ColumnCursor struct {
	driver   vfs.Driver
	reader   vfs.RangeReader // reader 惰性复用支持该能力的驱动句柄，由游标关闭。
	uri      vfs.URI
	identity vfs.ObjectIdentity
	meta     *ColumnMetaData
	leaf     *SchemaNode
	scope    *memory.Scope
	bm       *memory.BufferManager

	offset          int64
	end             int64
	consumed        int64
	pageOrdinal     int
	page            *columnPageState
	pending         ColumnEntry
	hasPending      bool
	dictionary      *vector.Vector
	headerTail      *memory.Buffer
	headerAt        int64
	headerSize      int
	cachedHeader    PageHeader
	cachedHeaderAt  int64
	cachedHeaderLen int
	metrics         ColumnCursorMetrics
	closed          bool
	index           *cursorPageIndex
}

func NewColumnCursor(driver vfs.Driver, uri vfs.URI, identity vfs.ObjectIdentity, meta *ColumnMetaData, leaf *SchemaNode, scope *memory.Scope, bm *memory.BufferManager) (*ColumnCursor, error) {
	if driver == nil || meta == nil || leaf == nil || scope == nil || bm == nil {
		return nil, errors.New("parquet: column cursor requires driver, metadata, schema leaf, scope, and buffer manager")
	}
	if leaf.Element.Type == nil {
		return nil, errors.New("parquet: column cursor requires primitive schema leaf")
	}
	if *leaf.Element.Type != meta.Type {
		return nil, fmt.Errorf("parquet: column physical type mismatch metadata=%d schema=%d", meta.Type, *leaf.Element.Type)
	}
	if meta.NumValues < 0 || meta.TotalCompressedSize <= 0 || meta.DataPageOffset < 0 {
		return nil, errors.New("parquet: invalid column metadata bounds")
	}
	start := meta.DataPageOffset
	if meta.DictionaryPageOffset != nil {
		if *meta.DictionaryPageOffset < 0 {
			return nil, errors.New("parquet: negative dictionary page offset")
		}
		if *meta.DictionaryPageOffset < start {
			start = *meta.DictionaryPageOffset
		}
	}
	end := start + meta.TotalCompressedSize
	if end < start {
		return nil, errors.New("parquet: column chunk offset overflow")
	}
	return &ColumnCursor{driver: driver, uri: uri, identity: identity, meta: meta, leaf: leaf, scope: scope, bm: bm, offset: start, end: end}, nil
}

func (c *ColumnCursor) Metrics() ColumnCursorMetrics { return c.metrics }
func (c *ColumnCursor) Consumed() int64              { return c.consumed }

// Close 释放页面、字典与当前列持有的可选范围读取句柄。
func (c *ColumnCursor) Close() {
	if c == nil || c.closed {
		return
	}
	c.closed = true
	if c.reader != nil {
		_ = c.reader.Close()
		c.reader = nil
	}
	c.releasePage()
	c.releaseHeaderTail()
	c.cachedHeader, c.cachedHeaderLen = PageHeader{}, 0
	if c.index != nil {
		c.index.close()
		c.index = nil
	}
	if c.dictionary != nil {
		c.dictionary.Release()
		c.dictionary = nil
	}
}

func (c *ColumnCursor) releasePage() {
	c.hasPending = false
	if c.page != nil {
		c.page.release()
		c.page = nil
	}
}

// readRange 使用精确受管范围读取；支持可选会话的驱动只在首个实际读请求打开句柄。
func (c *ColumnCursor) readRange(ctx context.Context, offset, length int64, tag memory.Tag) (*memory.Buffer, error) {
	if length < 0 || offset < 0 || offset > c.end || length > c.end-offset {
		return nil, errors.New("parquet: column range exceeds chunk bounds")
	}
	if length == 0 {
		return c.bm.Allocate(c.scope, tag, 0)
	}
	var expected *vfs.ObjectIdentity
	if c.identity.Stable() {
		exp := c.identity
		expected = &exp
	}
	if c.reader == nil {
		if opener, ok := c.driver.(vfs.RangeReaderOpener); ok {
			reader, err := opener.OpenRangeReader(ctx, c.uri, expected)
			if err != nil {
				return nil, err
			}
			c.reader = reader
		}
	}
	request := vfs.RangeRequest{URI: c.uri, Offset: offset, Length: length, Expected: expected, Tag: tag}
	read := c.driver.ReadRange
	if c.reader != nil {
		read = c.reader.ReadRange
	}
	result, err := read(ctx, request, c.scope, c.bm)
	c.metrics.Requests++
	c.metrics.PlannedBytes += length
	if err != nil {
		return nil, err
	}
	if result.Buffer == nil {
		return nil, errors.New("parquet: range read returned nil buffer")
	}
	c.metrics.TransferredBytes += int64(result.Buffer.Len())
	if int64(result.Buffer.Len()) != length {
		result.Buffer.Release()
		return nil, vfs.ErrShortRead
	}
	return result.Buffer, nil
}

func (c *ColumnCursor) readPageHeader(ctx context.Context) (PageHeader, int, error) {
	if c.cachedHeaderLen > 0 && c.cachedHeaderAt == c.offset {
		return c.cachedHeader, c.cachedHeaderLen, nil
	}
	c.cachedHeader = PageHeader{}
	c.cachedHeaderLen = 0
	remaining := c.end - c.offset
	if remaining <= 0 {
		return PageHeader{}, 0, io.EOF
	}
	if c.meta.crypto != nil {
		return c.readEncryptedPageHeader(ctx, remaining)
	}
	if c.headerSize > 0 && c.headerAt == c.offset {
		h, used, err := DecodePageHeader(c.headerTail.Bytes()[:c.headerSize])
		c.headerSize = 0
		if err == nil && used > 0 && used <= int(remaining) {
			return c.cachePageHeader(h, used)
		}
	}
	probe := initialPageHeaderProbe
	if probe > remaining {
		// Keep small tail pages on the original narrow discovery path;
		// never turn a larger probe into an eager whole-chunk request.
		probe = min(16, remaining)
	}
	var lastErr error
	for {
		b, err := c.readRange(ctx, c.offset, probe, memory.TagParquetRange)
		if err != nil {
			return PageHeader{}, 0, err
		}
		h, used, decodeErr := DecodePageHeader(b.Bytes())
		if decodeErr == nil {
			h = ownPageStatistics(h)
		}
		b.Release()
		if decodeErr == nil {
			if used <= 0 || int64(used) > probe {
				return PageHeader{}, 0, errors.New("parquet: invalid decoded page header length")
			}
			return c.cacheOwnedPageHeader(h, used)
		}
		lastErr = decodeErr
		if probe >= remaining || probe >= maxPageHeaderProbe {
			return PageHeader{}, 0, fmt.Errorf("parquet: decode page header: %w", lastErr)
		}
		next := probe * 2
		if next > remaining {
			next = remaining
		}
		if next > maxPageHeaderProbe {
			next = maxPageHeaderProbe
		}
		if next <= probe {
			return PageHeader{}, 0, fmt.Errorf("parquet: decode page header: %w", lastErr)
		}
		probe = next
	}
}

func (c *ColumnCursor) loadPage(ctx context.Context) error {
	_, err := c.loadPageSkipping(ctx, 0)
	return err
}

// loadPageSkipping can advance complete scalar data pages from their headers.
// The first partially needed page retains the normal decoder and validation.
func (c *ColumnCursor) loadPageSkipping(ctx context.Context, rows int64) (int64, error) {
	c.releasePage()
	skipped := int64(0)
	if rows > 0 {
		var err error
		skipped, err = c.skipIndexedRows(ctx, rows)
		if err != nil {
			return skipped, err
		}
		rows -= skipped
		if rows == 0 {
			return skipped, nil
		}
	}
	for c.consumed < c.meta.NumValues {
		if err := c.prepareIndexedRead(ctx); err != nil {
			return skipped, err
		}
		header, headerLen, err := c.nextDataPageHeader(ctx)
		if err != nil {
			return skipped, err
		}
		count, _, err := c.scalarPageInfo(header)
		if err != nil {
			return skipped, err
		}
		if err := c.validateIndexedHeader(header, headerLen, count); err != nil {
			return skipped, err
		}
		bodyOffset := c.offset + int64(headerLen)
		bodyLen := int64(header.CompressedPageSize)
		if c.leaf.MaxRep == 0 && count > 0 && count <= rows {
			c.advanceSkippedPage(headerLen, bodyLen, count)
			rows -= count
			skipped += count
			if rows == 0 {
				return skipped, nil
			}
			continue
		}
		body, err := c.readDecodedPageBody(ctx, bodyOffset, header)
		if err != nil {
			return skipped, err
		}
		c.offset = bodyOffset + bodyLen
		c.pageOrdinal++
		c.metrics.PagesRead++
		c.metrics.UsefulBytes += int64(headerLen) + bodyLen

		switch header.Type {
		case PageData:
			state, err := c.loadDataV1(header, body)
			body.Release()
			if err != nil {
				return skipped, err
			}
			c.page = state
			return skipped, nil
		case PageDataV2:
			state, err := c.loadDataV2(header, body)
			if err != nil {
				body.Release()
				return skipped, err
			}
			c.page = state
			return skipped, nil
		default:
			body.Release()
			return skipped, fmt.Errorf("parquet: unsupported page type %d", header.Type)
		}
	}
	return skipped, io.EOF
}

func (c *ColumnCursor) loadDictionary(header PageHeader, body *memory.Buffer) error {
	if header.Dictionary == nil || header.Dictionary.NumValues < 0 {
		return errors.New("parquet: invalid dictionary page")
	}
	if header.Dictionary.Encoding != EncodingPlain {
		return fmt.Errorf("parquet: unsupported dictionary encoding %d", header.Dictionary.Encoding)
	}
	decoded, err := DecompressManaged(c.scope, c.bm, c.meta.Codec, c.pagePayload(body, header), int(header.UncompressedPageSize))
	if err != nil {
		return err
	}
	defer decoded.Release()
	dict, err := vector.NewFlat(c.scope, c.bm, c.leaf.Logical, int(header.Dictionary.NumValues))
	if err != nil {
		return err
	}
	values := newValueStream(decoded.Bytes(), c.meta.Type, c.leaf)
	bulk, err := values.writeFixedBatch(dict, 0, dict.Count())
	if err != nil {
		dict.Release()
		return err
	}
	for i := 0; !bulk && i < dict.Count(); i++ {
		if err := values.NextInto(dict, i); err != nil {
			dict.Release()
			return err
		}
	}
	if c.dictionary != nil {
		c.dictionary.Release()
	}
	c.dictionary = dict
	return nil
}

func (c *ColumnCursor) loadDataV1(header PageHeader, body *memory.Buffer) (*columnPageState, error) {
	h := header.Data
	if h == nil || h.NumValues < 0 {
		return nil, errors.New("parquet: invalid data page v1")
	}
	decoded, err := DecompressManaged(c.scope, c.bm, c.meta.Codec, c.pagePayload(body, header), int(header.UncompressedPageSize))
	if err != nil {
		return nil, err
	}
	state := &columnPageState{header: header, decoded: decoded, remaining: int(h.NumValues)}
	keep := false
	defer func() {
		if !keep {
			state.release()
		}
	}()
	payload := decoded.Bytes()
	rep, used, err := newLevelsV1(payload, c.leaf.MaxRep, int(h.NumValues), h.RepetitionLevelEncoding)
	if err != nil {
		return nil, err
	}
	payload = payload[used:]
	def, used, err := newLevelsV1(payload, c.leaf.MaxDef, int(h.NumValues), h.DefinitionLevelEncoding)
	if err != nil {
		return nil, err
	}
	payload = payload[used:]
	state.rep, state.def = rep, def
	if err := c.configureValues(state, h.Encoding, payload); err != nil {
		return nil, err
	}
	keep = true
	return state, nil
}

func (c *ColumnCursor) loadDataV2(header PageHeader, body *memory.Buffer) (*columnPageState, error) {
	h := header.DataV2
	if h == nil || h.NumValues < 0 || h.RepetitionLevelsByteLength < 0 || h.DefinitionLevelsByteLength < 0 {
		return nil, errors.New("parquet: invalid data page v2")
	}
	levelLen64 := int64(h.RepetitionLevelsByteLength) + int64(h.DefinitionLevelsByteLength)
	if levelLen64 < 0 || levelLen64 > int64(c.pagePayloadSize(header)) || levelLen64 > int64(header.UncompressedPageSize) {
		return nil, errors.New("parquet: invalid data page v2 level lengths")
	}
	levelLen := int(levelLen64)
	repEnd := int(h.RepetitionLevelsByteLength)
	defEnd := levelLen
	raw := c.pagePayload(body, header)
	rep, err := newLevelsV2(raw[:repEnd], c.leaf.MaxRep)
	if err != nil {
		return nil, err
	}
	def, err := newLevelsV2(raw[repEnd:defEnd], c.leaf.MaxDef)
	if err != nil {
		return nil, err
	}

	expectedValues := int(header.UncompressedPageSize) - levelLen
	if expectedValues < 0 {
		return nil, errors.New("parquet: negative v2 value size")
	}
	codec := c.meta.Codec
	if !h.IsCompressed {
		codec = CodecUncompressed
	}
	decoded, err := DecompressManaged(c.scope, c.bm, codec, raw[levelLen:], expectedValues)
	if err != nil {
		return nil, err
	}
	state := &columnPageState{header: header, compressed: body, decoded: decoded, rep: rep, def: def, remaining: int(h.NumValues)}
	if err := c.configureValues(state, h.Encoding, decoded.Bytes()); err != nil {
		state.compressed = nil // caller still owns body on error
		state.release()
		return nil, err
	}
	return state, nil
}

func (c *ColumnCursor) configureValues(state *columnPageState, encoding Encoding, payload []byte) error {
	switch encoding {
	case EncodingPlain:
		state.values = newValueStream(payload, c.meta.Type, c.leaf)
	case EncodingPlainDictionary, EncodingRLEDictionary:
		if c.dictionary == nil {
			return errors.New("parquet: dictionary data page without dictionary page")
		}
		state.dict = newDictionaryIndexStream(payload)
	default:
		return fmt.Errorf("parquet: unsupported data page encoding %d", encoding)
	}
	return nil
}

func (c *ColumnCursor) PeekEntry(ctx context.Context) (ColumnEntry, error) {
	if c == nil || c.closed {
		return ColumnEntry{}, errors.New("parquet: column cursor is closed")
	}
	if c.consumed >= c.meta.NumValues {
		return ColumnEntry{}, io.EOF
	}
	if c.hasPending {
		return c.pending, nil
	}
	if c.page == nil || c.page.remaining == 0 {
		if err := c.loadPage(ctx); err != nil {
			return ColumnEntry{}, err
		}
	}
	rep, err := c.page.rep.Next()
	if err != nil {
		return ColumnEntry{}, err
	}
	def, err := c.page.def.Next()
	if err != nil {
		return ColumnEntry{}, err
	}
	entry := ColumnEntry{Definition: def, Repetition: rep, HasValue: def == c.leaf.MaxDef}
	c.pending = entry
	c.hasPending = true
	return entry, nil
}

func (c *ColumnCursor) NextInto(ctx context.Context, dst *vector.Vector, row int) (ColumnEntry, error) {
	if c == nil || c.closed {
		return ColumnEntry{}, errors.New("parquet: column cursor is closed")
	}
	if dst == nil || row < 0 || row >= dst.Count() {
		return ColumnEntry{}, errors.New("parquet: invalid destination row")
	}
	entry, err := c.PeekEntry(ctx)
	if err != nil {
		return ColumnEntry{}, err
	}
	p := c.page
	if entry.HasValue {
		switch {
		case p.values != nil:
			if err := p.values.NextInto(dst, row); err != nil {
				return ColumnEntry{}, err
			}
		case p.dict != nil:
			idx, err := p.dict.Next()
			if err != nil {
				return ColumnEntry{}, err
			}
			if int(idx) >= c.dictionary.Count() {
				return ColumnEntry{}, fmt.Errorf("parquet: dictionary index %d out of range %d", idx, c.dictionary.Count())
			}
			if err := copyDictionaryValue(dst, row, c.dictionary, int(idx)); err != nil {
				return ColumnEntry{}, err
			}
		default:
			return ColumnEntry{}, errors.New("parquet: page has no value decoder")
		}
		c.metrics.DataValues++
	}
	c.hasPending = false
	p.remaining--
	c.consumed++
	if p.remaining == 0 {
		c.releasePage()
	}
	return entry, nil
}

func copyDictionaryValue(dst *vector.Vector, row int, dict *vector.Vector, idx int) error {
	return vector.CopyValue(dict, idx, dst, row)
}

func (c *ColumnCursor) ReadScalar(ctx context.Context, rows int) (*vector.Vector, error) {
	if c == nil || c.closed {
		return nil, errors.New("parquet: column cursor is closed")
	}
	if c.leaf.MaxRep != 0 {
		return nil, ErrRepeatedColumnRequiresLevelReader
	}
	if rows <= 0 || rows > vector.DefaultVectorSize {
		return nil, fmt.Errorf("parquet: invalid scalar batch size %d", rows)
	}
	remaining := c.meta.NumValues - c.consumed
	if remaining <= 0 {
		return nil, io.EOF
	}
	if int64(rows) > remaining {
		rows = int(remaining)
	}
	if out, handled, err := c.readOwnedDictionaryBatch(ctx, rows); handled || err != nil {
		return out, err
	}
	out, err := vector.NewFlat(c.scope, c.bm, c.leaf.Logical, rows)
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			out.Release()
		}
	}()
	if err := c.readScalarInto(ctx, out, 0, rows); err != nil {
		return nil, err
	}
	keep = true
	return out, nil
}

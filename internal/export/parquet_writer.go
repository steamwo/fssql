package export

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/format/parquet"
)

const (
	// DefaultParquetRowGroupRows preserves the default export layout.
	DefaultParquetRowGroupRows = 2048
	// DefaultParquetPageRows is clamped to smaller row groups when omitted.
	DefaultParquetPageRows   = 512
	defaultParquetPageBuffer = 64 << 10
)

var (
	parquetMagic          = []byte{'P', 'A', 'R', '1'}
	encryptedParquetMagic = []byte{'P', 'A', 'R', 'E'}
)

// ParquetWriteOptions controls row-group/page boundaries, internal compression and native encryption.
type ParquetWriteOptions struct {
	RowGroupRows   int
	PageRows       int
	PageBufferSize int
	// Compression is an internal page codec. It defaults to snappy; identity disables compression.
	Compression string
	// Encryption enables encrypted-footer Parquet Modular Encryption when non-nil.
	Encryption *ParquetEncryptionConfig
}

// ParquetWriter is the memory-governed Controlled Parquet writer.
type ParquetWriter struct {
	schema []Column
	opts   ParquetWriteOptions
	scope  *memory.Scope
	bm     *memory.BufferManager
	sink   ByteSink

	rows      []*vector.Vector
	rowCount  int
	totalRows int64
	offset    int64
	rowGroups []parquet.RowGroup

	metadataReservation *memory.Reservation
	statisticsBytes     int64
	fileAAD             [parquetUniqueFileIDBytes]byte
	started             bool
	terminal            bool
}

// NewParquetWriter validates all format options and allocates the first managed row group.
func NewParquetWriter(schema []Column, opts ParquetWriteOptions, scope *memory.Scope, bm *memory.BufferManager, sink ByteSink) (*ParquetWriter, error) {
	if len(schema) == 0 {
		return nil, errors.New("export: Parquet schema is empty")
	}
	if scope == nil || bm == nil || sink == nil {
		return nil, errors.New("export: Parquet writer requires managed memory and sink")
	}
	var err error
	opts, err = NormalizeParquetLayout(opts)
	if err != nil {
		return nil, err
	}
	opts.Compression = strings.ToLower(strings.TrimSpace(opts.Compression))
	if opts.Compression == "" {
		opts.Compression = "snappy"
	} else if opts.Compression == "none" {
		opts.Compression = "identity"
	}
	if opts.Compression != "identity" && opts.Compression != "snappy" {
		return nil, fmt.Errorf("export: unsupported Parquet page compression %q", opts.Compression)
	}
	if opts.Encryption != nil {
		enc := *opts.Encryption
		enc.Key = append([]byte(nil), opts.Encryption.Key...)
		opts.Encryption = &enc
		if err := ValidateParquetEncryption(opts.Encryption); err != nil {
			zeroBytes(enc.Key)
			return nil, err
		}
	}

	copySchema := append([]Column(nil), schema...)
	for i := range copySchema {
		if _, err := parquetPhysicalType(copySchema[i].Type); err != nil {
			if opts.Encryption != nil {
				zeroBytes(opts.Encryption.Key)
			}
			return nil, fmt.Errorf("export: parquet column %d %q: %w", i, copySchema[i].Name, err)
		}
	}
	metaRes, err := scope.Reserve(memory.TagMetadata, 0)
	if err != nil {
		if opts.Encryption != nil {
			zeroBytes(opts.Encryption.Key)
		}
		return nil, err
	}
	w := &ParquetWriter{schema: copySchema, opts: opts, scope: scope, bm: bm, sink: sink, metadataReservation: metaRes}
	if opts.Encryption != nil {
		if _, err := rand.Read(w.fileAAD[:]); err != nil {
			metaRes.Release()
			zeroBytes(opts.Encryption.Key)
			return nil, fmt.Errorf("export: generate Parquet file AAD: %w", err)
		}
	}
	if err := w.allocateRowGroup(); err != nil {
		metaRes.Release()
		if opts.Encryption != nil {
			zeroBytes(opts.Encryption.Key)
		}
		return nil, err
	}
	return w, nil
}

func (w *ParquetWriter) WriteChunk(ctx context.Context, chunk *vector.DataChunk) error {
	if w == nil || w.terminal {
		return ErrWriterClosed
	}
	if err := validateChunkSchema(w.schema, chunk); err != nil {
		return err
	}
	if err := w.ensureStarted(ctx); err != nil {
		return err
	}
	var selection [vector.DefaultVectorSize]int
	for srcRow := 0; srcRow < chunk.Cardinality; {
		if err := ctx.Err(); err != nil {
			return err
		}
		space := w.opts.RowGroupRows - w.rowCount
		take := chunk.Cardinality - srcRow
		if take > space {
			take = space
		}
		for n := 0; n < take; n++ {
			selection[n] = srcRow + n
		}
		for col := range w.schema {
			if err := vector.CopySelectionInto(w.rows[col], w.rowCount, chunk.Columns[col], selection[:take]); err != nil {
				return fmt.Errorf("export: parquet copy batch at row %d column %d: %w", srcRow, col, err)
			}
		}
		w.rowCount += take
		w.totalRows += int64(take)
		srcRow += take
		if w.rowCount == w.opts.RowGroupRows {
			if err := w.flushRowGroup(ctx, true); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *ParquetWriter) Flush(ctx context.Context) error {
	if w == nil || w.terminal {
		return ErrWriterClosed
	}
	if err := w.ensureStarted(ctx); err != nil {
		return err
	}
	if w.rowCount > 0 {
		if err := w.flushRowGroup(ctx, true); err != nil {
			return err
		}
	}
	return w.sink.Flush(ctx)
}

// Close writes the standard footer or PARE encrypted-footer envelope and atomically closes the sink.
func (w *ParquetWriter) Close(ctx context.Context) (WriteResult, error) {
	if w == nil || w.terminal {
		return WriteResult{}, ErrWriterClosed
	}
	w.terminal = true
	if err := w.ensureStarted(ctx); err != nil {
		return WriteResult{}, w.fail(err)
	}
	if w.rowCount > 0 {
		if err := w.flushRowGroup(ctx, false); err != nil {
			return WriteResult{}, w.fail(err)
		}
	}
	meta, err := w.fileMetadata()
	if err != nil {
		return WriteResult{}, w.fail(err)
	}

	footerStart := w.offset
	if w.opts.Encryption != nil {
		cryptoMeta, err := parquet.EncodeFileCryptoMetaData(w.fileAAD[:], []byte(w.opts.Encryption.KeyMetadata))
		if err != nil {
			return WriteResult{}, w.fail(err)
		}
		plainFooter, err := parquet.EncodeEncryptedFileMetaData(meta)
		if err != nil {
			return WriteResult{}, w.fail(err)
		}
		if err := w.writeBytes(ctx, cryptoMeta); err != nil {
			return WriteResult{}, w.fail(err)
		}
		frame, buffer, err := w.encryptManaged(plainFooter, parquetModuleFooter, -1, -1, -1)
		if err != nil {
			return WriteResult{}, w.fail(err)
		}
		writeErr := w.writeBytes(ctx, frame)
		buffer.Release()
		if writeErr != nil {
			return WriteResult{}, w.fail(writeErr)
		}
	} else {
		footer, err := parquet.EncodeFileMetaData(meta)
		if err != nil {
			return WriteResult{}, w.fail(err)
		}
		if err := w.writeBytes(ctx, footer); err != nil {
			return WriteResult{}, w.fail(err)
		}
	}
	footerSize := w.offset - footerStart
	if footerSize < 0 || footerSize > math.MaxUint32 {
		return WriteResult{}, w.fail(errors.New("export: parquet footer exceeds uint32"))
	}
	var footerLen [4]byte
	binary.LittleEndian.PutUint32(footerLen[:], uint32(footerSize))
	if err := w.writeBytes(ctx, footerLen[:]); err != nil {
		return WriteResult{}, w.fail(err)
	}
	magic := parquetMagic
	if w.opts.Encryption != nil {
		magic = encryptedParquetMagic
	}
	if err := w.writeBytes(ctx, magic); err != nil {
		return WriteResult{}, w.fail(err)
	}
	result, err := w.sink.Close(ctx)
	w.releaseResources()
	if err != nil {
		abortErr := w.sink.Abort(context.Background())
		return WriteResult{}, errors.Join(err, abortErr)
	}
	return result, nil
}

func (w *ParquetWriter) Abort(ctx context.Context) error {
	if w == nil || w.terminal {
		return nil
	}
	w.terminal = true
	err := w.sink.Abort(ctx)
	w.releaseResources()
	return err
}

func (w *ParquetWriter) ensureStarted(ctx context.Context) error {
	if w.started {
		return nil
	}
	magic := parquetMagic
	if w.opts.Encryption != nil {
		magic = encryptedParquetMagic
	}
	if err := w.writeBytes(ctx, magic); err != nil {
		return err
	}
	w.started = true
	return nil
}

func (w *ParquetWriter) allocateRowGroup() error {
	if err := checkParquetRowGroupBudget(w.schema, w.opts.RowGroupRows, w.scope); err != nil {
		return err
	}
	rows := make([]*vector.Vector, len(w.schema))
	for i := range w.schema {
		v, err := vector.NewFlat(w.scope, w.bm, w.schema[i].Type, w.opts.RowGroupRows)
		if err != nil {
			for j := 0; j < i; j++ {
				rows[j].Release()
			}
			return err
		}
		rows[i] = v
	}
	w.rows = rows
	w.rowCount = 0
	return nil
}

// parquetRowGroupOrdinal 仅为加密模块 AAD 保留必需的 int16 row-group ordinal。
func parquetRowGroupOrdinal(encrypted bool, rowGroupCount int) (*int16, error) {
	if !encrypted {
		return nil, nil
	}
	if rowGroupCount > math.MaxInt16 {
		return nil, errors.New("export: Parquet row-group ordinal exceeds int16")
	}
	ordinal := int16(rowGroupCount)
	return &ordinal, nil
}

func (w *ParquetWriter) flushRowGroup(ctx context.Context, allocateNext bool) error {
	if w.rowCount == 0 {
		return nil
	}
	ordinal, err := parquetRowGroupOrdinal(w.opts.Encryption != nil, len(w.rowGroups))
	if err != nil {
		return err
	}
	group := parquet.RowGroup{Columns: make([]parquet.ColumnChunk, len(w.schema)), NumRows: int64(w.rowCount), Ordinal: ordinal}
	groupStart := w.offset
	rowGroupOrdinal := int16(0)
	if ordinal != nil {
		rowGroupOrdinal = *ordinal
	}
	var totalUncompressed, totalCompressed int64
	pages := &parquetPageWorkspace{scope: w.scope, bm: w.bm, initial: w.opts.PageBufferSize}
	defer pages.release()
	for col := range w.schema {
		if col > math.MaxInt16 {
			return errors.New("export: Parquet column ordinal exceeds int16")
		}
		chunk, uncompressed, compressed, err := w.writeColumnChunk(ctx, col, rowGroupOrdinal, pages)
		if err != nil {
			return err
		}
		group.Columns[col] = chunk
		totalUncompressed += uncompressed
		totalCompressed += compressed
	}
	group.TotalByteSize = totalUncompressed
	group.TotalCompressedSize = &totalCompressed
	group.FileOffset = &groupStart

	pages.release()
	estimate := int64((len(w.rowGroups)+1)*(256+len(w.schema)*320)) + w.statisticsBytes
	if err := w.metadataReservation.Resize(estimate); err != nil {
		return err
	}
	w.rowGroups = append(w.rowGroups, group)
	w.releaseRowGroup()
	if allocateNext {
		return w.allocateRowGroup()
	}
	return nil
}

func (w *ParquetWriter) writeColumnChunk(ctx context.Context, col int, rowGroupOrdinal int16, pages *parquetPageWorkspace) (parquet.ColumnChunk, int64, int64, error) {
	defer pages.releaseEncoded()
	physical, err := parquetPhysicalType(w.schema[col].Type)
	if err != nil {
		return parquet.ColumnChunk{}, 0, 0, err
	}
	// Column statistics merge page statistics, avoiding a second value scan.
	extraMetadata := int64(256)
	err = w.metadataReservation.Resize(w.metadataReservation.Size() + extraMetadata)
	if errors.Is(err, memory.ErrLimitExceeded) {
		pages.release()
		err = w.metadataReservation.Resize(w.metadataReservation.Size() + extraMetadata)
	}
	if err != nil {
		return parquet.ColumnChunk{}, 0, 0, err
	}
	w.statisticsBytes += extraMetadata
	nulls, nans, exact := int64(0), int64(0), true
	columnStats := &parquet.Statistics{NullCount: &nulls}
	if physical == parquet.TypeFloat || physical == parquet.TypeDouble {
		columnStats.NaNCount = &nans
	}
	compareBounds := parquetStatisticsEncodedComparator(w.schema[col].Type)
	omitBounds := false
	start := w.offset
	var totalUncompressed, totalCompressed int64
	pageOrdinal := int16(0)
	dictionary, err := planParquetDictionary(ctx, w.rows[col], w.rowCount, w.opts.PageRows, w.scope, w.bm)
	if err != nil {
		return parquet.ColumnChunk{}, 0, 0, err
	}
	defer func() { dictionary.Release() }()
	var dictionaryOffset *int64
	dataOffset := start
	encodings := []parquet.Encoding{parquet.EncodingPlain, parquet.EncodingRLE}
	if dictionary != nil {
		var body *parquetPageBuffer
		body, err = pages.page()
		if err == nil {
			var committed bool
			committed, totalUncompressed, totalCompressed, err = w.writeDictionaryPage(ctx, body, dictionary, rowGroupOrdinal, int16(col))
			if committed {
				dictionaryOffset = &start
				dataOffset = w.offset
				encodings = append(encodings, parquet.EncodingRLEDictionary)
			}
		}
		if err != nil {
			if dictionaryOffset != nil || !errors.Is(err, memory.ErrLimitExceeded) {
				return parquet.ColumnChunk{}, totalUncompressed, totalCompressed, err
			}
			// No dictionary bytes reached the sink. Restore the original page
			// budget and reuse the immutable row group through the PLAIN path.
			dictionary.Release()
			dictionary = nil
			pages.release()
			totalUncompressed, totalCompressed = 0, 0
		}
	}
	for pageStart := 0; pageStart < w.rowCount; pageStart += w.opts.PageRows {
		count := w.rowCount - pageStart
		if count > w.opts.PageRows {
			count = w.opts.PageRows
		}
		body, err := pages.page()
		if err != nil {
			return parquet.ColumnChunk{}, totalUncompressed, totalCompressed, err
		}
		encoding := parquet.EncodingPlain
		if dictionary != nil {
			encoding = parquet.EncodingRLEDictionary
		}
		for attempt := 0; attempt < 2; attempt++ {
			body.used = 0
			err = encodeDefinitionLevels(body, w.rows[col], pageStart, count)
			if err == nil {
				if dictionary != nil {
					err = dictionary.encodeIndices(body, pageStart, count)
				} else {
					err = encodeParquetPlain(body, w.rows[col], w.schema[col].Type, pageStart, count)
				}
			}
			if !errors.Is(err, memory.ErrLimitExceeded) || attempt != 0 {
				break
			}
			// Encoding has not reached the sink. Reclaim the preceding page's
			// idle compression/encryption buffers and rebuild this page.
			pages.releaseEncoded()
		}
		if err != nil {
			return parquet.ColumnChunk{}, totalUncompressed, totalCompressed, err
		}
		if body.Len() > math.MaxInt32 {
			return parquet.ColumnChunk{}, totalUncompressed, totalCompressed, errors.New("export: parquet page exceeds int32 size")
		}

		payload := body.Bytes()
		if w.opts.Compression == "snappy" {
			payload, err = pages.compress(payload)
			if err != nil {
				return parquet.ColumnChunk{}, totalUncompressed, totalCompressed, err
			}
		}
		compressedPageSize := len(payload)
		if w.opts.Encryption != nil {
			frameSize, err := parquetEncryptedFrameSize(len(payload))
			if err != nil {
				return parquet.ColumnChunk{}, totalUncompressed, totalCompressed, err
			}
			compressedPageSize = frameSize
		}
		if compressedPageSize > math.MaxInt32 {
			return parquet.ColumnChunk{}, totalUncompressed, totalCompressed, errors.New("export: parquet compressed page exceeds int32 size")
		}
		pageStats := collectParquetStatistics(w.rows[col], pageStart, count)
		nulls += *pageStats.NullCount
		if pageStats.NaNCount != nil {
			nans += *pageStats.NaNCount
		}
		if *pageStats.NullCount < int64(count) {
			if pageStats.MinValue == nil || pageStats.MaxValue == nil {
				omitBounds = true
			} else if !omitBounds {
				if columnStats.MinValue == nil || compareBounds(pageStats.MinValue, columnStats.MinValue) < 0 {
					columnStats.MinValue = pageStats.MinValue
				}
				if columnStats.MaxValue == nil || compareBounds(pageStats.MaxValue, columnStats.MaxValue) > 0 {
					columnStats.MaxValue = pageStats.MaxValue
				}
			}
		}
		header, err := parquet.EncodePageHeader(parquet.PageHeader{
			Type:                 parquet.PageData,
			UncompressedPageSize: int32(body.Len()),
			CompressedPageSize:   int32(compressedPageSize),
			Data: &parquet.DataPageHeader{
				NumValues:               int32(count),
				Encoding:                encoding,
				DefinitionLevelEncoding: parquet.EncodingRLE,
				RepetitionLevelEncoding: parquet.EncodingRLE,
				Statistics:              pageStats,
			},
		})
		if err != nil {
			return parquet.ColumnChunk{}, totalUncompressed, totalCompressed, err
		}

		headerOnDisk := len(header)
		if w.opts.Encryption != nil {
			headerFrame, err := pages.encrypt(w, header, parquetModuleDataPageHeader, rowGroupOrdinal, int16(col), pageOrdinal)
			if err != nil {
				return parquet.ColumnChunk{}, totalUncompressed, totalCompressed, err
			}
			headerOnDisk = len(headerFrame)
			writeErr := w.writeBytes(ctx, headerFrame)
			if writeErr != nil {
				return parquet.ColumnChunk{}, totalUncompressed, totalCompressed, writeErr
			}
			dataFrame, err := pages.encrypt(w, payload, parquetModuleDataPage, rowGroupOrdinal, int16(col), pageOrdinal)
			if err != nil {
				return parquet.ColumnChunk{}, totalUncompressed, totalCompressed, err
			}
			writeErr = w.writeBytes(ctx, dataFrame)
			if writeErr != nil {
				return parquet.ColumnChunk{}, totalUncompressed, totalCompressed, writeErr
			}
		} else {
			if err := w.writeBytes(ctx, header); err != nil {
				return parquet.ColumnChunk{}, totalUncompressed, totalCompressed, err
			}
			if err := w.writeBytes(ctx, payload); err != nil {
				return parquet.ColumnChunk{}, totalUncompressed, totalCompressed, err
			}
		}
		totalUncompressed += int64(headerOnDisk + body.Len())
		totalCompressed += int64(headerOnDisk + compressedPageSize)
		pages.trim()
		if pageOrdinal == math.MaxInt16 && pageStart+count < w.rowCount {
			return parquet.ColumnChunk{}, totalUncompressed, totalCompressed, errors.New("export: Parquet page ordinal exceeds int16")
		}
		pageOrdinal++
	}
	pages.releaseEncoded()
	if omitBounds {
		columnStats.MinValue, columnStats.MaxValue = nil, nil
	} else if columnStats.MinValue != nil && columnStats.NaNCount == nil {
		columnStats.IsMinValueExact, columnStats.IsMaxValueExact = &exact, &exact
	}
	// Bounds must outlive row-group vectors and temporary page metadata.
	boundBytes := int64(len(columnStats.MinValue) + len(columnStats.MaxValue))
	if err := w.metadataReservation.Resize(w.metadataReservation.Size() + boundBytes); err != nil {
		return parquet.ColumnChunk{}, totalUncompressed, totalCompressed, err
	}
	w.statisticsBytes += boundBytes
	columnStats.MinValue = bytes.Clone(columnStats.MinValue)
	columnStats.MaxValue = bytes.Clone(columnStats.MaxValue)
	meta := &parquet.ColumnMetaData{
		Type:                  physical,
		Encodings:             encodings,
		PathInSchema:          []string{w.schema[col].Name},
		Codec:                 parquet.CodecUncompressed,
		NumValues:             int64(w.rowCount),
		TotalUncompressedSize: totalUncompressed,
		TotalCompressedSize:   totalCompressed,
		DataPageOffset:        dataOffset,
		DictionaryPageOffset:  dictionaryOffset,
		Statistics:            columnStats,
	}
	if w.opts.Compression == "snappy" {
		meta.Codec = parquet.CodecSnappy
	}
	return parquet.ColumnChunk{FileOffset: start, MetaData: meta}, totalUncompressed, totalCompressed, nil
}

func (w *ParquetWriter) encryptManaged(plain []byte, module int8, rowGroup, column, page int16) ([]byte, *memory.Buffer, error) {
	size, err := parquetEncryptedFrameSize(len(plain))
	if err != nil {
		return nil, nil, err
	}
	buf, err := w.bm.Allocate(w.scope, memory.TagExportBuffer, size)
	if err != nil {
		return nil, nil, err
	}
	sealed, err := sealParquetModule(buf.Bytes(), w.opts.Encryption, w.fileAAD[:], module, rowGroup, column, page, plain)
	if err != nil {
		buf.Release()
		return nil, nil, err
	}
	return sealed, buf, nil
}

func (w *ParquetWriter) fileMetadata() (parquet.FileMetaData, error) {
	rootChildren := int32(len(w.schema))
	flat := make([]parquet.SchemaElement, 0, len(w.schema)+1)
	flat = append(flat, parquet.SchemaElement{Name: "schema", NumChildren: &rootChildren})
	for i := range w.schema {
		e, err := parquetSchemaElement(w.schema[i])
		if err != nil {
			return parquet.FileMetaData{}, err
		}
		flat = append(flat, e)
	}
	orders := make([]parquet.ColumnOrder, len(w.schema))
	for i := range orders {
		orders[i] = parquet.TypeDefinedOrder
	}
	return parquet.FileMetaData{Version: 1, Schema: flat, NumRows: w.totalRows, RowGroups: append([]parquet.RowGroup(nil), w.rowGroups...), CreatedBy: "fssql controlled parquet writer", ColumnOrders: orders}, nil
}

func (w *ParquetWriter) writeBytes(ctx context.Context, p []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n, err := w.sink.Write(ctx, p)
	if err != nil {
		return err
	}
	if n != len(p) {
		return io.ErrShortWrite
	}
	w.offset += int64(n)
	return nil
}

func (w *ParquetWriter) fail(cause error) error {
	abortErr := w.sink.Abort(context.Background())
	w.releaseResources()
	return errors.Join(cause, abortErr)
}

func (w *ParquetWriter) releaseRowGroup() {
	for _, v := range w.rows {
		if v != nil {
			v.Release()
		}
	}
	w.rows = nil
	w.rowCount = 0
}

func (w *ParquetWriter) releaseResources() {
	w.releaseRowGroup()
	if w.metadataReservation != nil {
		w.metadataReservation.Release()
		w.metadataReservation = nil
	}
	w.rowGroups = nil
	w.statisticsBytes = 0
	if w.opts.Encryption != nil {
		zeroBytes(w.opts.Encryption.Key)
		w.opts.Encryption.Key = nil
	}
}

func zeroBytes(value []byte) {
	for i := range value {
		value[i] = 0
	}
}

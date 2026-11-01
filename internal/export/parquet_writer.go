package export

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/format/parquet"
)

const (
	defaultParquetRowGroupRows = 2048
	defaultParquetPageRows     = 512
	defaultParquetPageBuffer   = 64 << 10
)

var parquetMagic = []byte{'P', 'A', 'R', '1'}

// ParquetWriteOptions 控制 Parquet 格式内部 row-group/page 边界；multipart 边界仍完全属于 VFS 层。
type ParquetWriteOptions struct {
	// RowGroupRows 是一个 row group 的目标行数，与 DataChunk 和 multipart 大小独立。
	RowGroupRows int
	// PageRows 是单个 data page 的最大逻辑行数，与 row-group 大小独立。
	PageRows int
	// PageBufferSize 是单页 plain 编码的初始 Managed Buffer 大小。
	PageBufferSize int
}

// ParquetWriter 是 Phase 4 memory-governed Parquet writer 的 baseline。
// 它直接消费自研 Vector/DataChunk，并写标准 PLAIN + uncompressed DataPage v1。
type ParquetWriter struct {
	// schema 是绑定后固定的查询输出 schema。
	schema []Column
	// opts 是规范化后的格式内部边界参数。
	opts ParquetWriteOptions
	// scope 管理 row-group vector、page payload 与 metadata reservation。
	scope *memory.Scope
	// bm 分配所有大块 writer 缓冲。
	bm *memory.BufferManager
	// sink 是最终字节流；标准 Parquet 不允许再被 whole-file compression 改写内部 offset。
	sink ByteSink
	// rows 是当前 row group 的 managed scalar vectors。
	rows []*vector.Vector
	// rowCount 是当前 row group 已缓存的行数。
	rowCount int
	// totalRows 是整个文件已接受的行数。
	totalRows int64
	// offset 是已成功写入 Parquet 文件字节流的绝对偏移。
	offset int64
	// rowGroups 是 footer 所需的小型 metadata；增长前先扩 metadataReservation。
	rowGroups []parquet.RowGroup
	// metadataReservation 记账 rowGroups/footer 的 Go heap metadata。
	metadataReservation *memory.Reservation
	// started 表示文件头 magic 已成功写出。
	started bool
	// terminal 表示 Close/Abort 后不可继续使用。
	terminal bool
}

// NewParquetWriter 验证 schema 后预分配第一个受管 row-group buffer；不会绕过 MemoryManager。
func NewParquetWriter(schema []Column, opts ParquetWriteOptions, scope *memory.Scope, bm *memory.BufferManager, sink ByteSink) (*ParquetWriter, error) {
	if len(schema) == 0 {
		return nil, errors.New("export: Parquet schema is empty")
	}
	if scope == nil || bm == nil || sink == nil {
		return nil, errors.New("export: Parquet writer requires managed memory and sink")
	}
	if opts.RowGroupRows <= 0 {
		opts.RowGroupRows = defaultParquetRowGroupRows
	}
	if opts.PageRows <= 0 {
		opts.PageRows = defaultParquetPageRows
		if opts.PageRows > opts.RowGroupRows {
			opts.PageRows = opts.RowGroupRows
		}
	}
	if opts.PageBufferSize <= 0 {
		opts.PageBufferSize = defaultParquetPageBuffer
	}
	if opts.PageRows > opts.RowGroupRows {
		return nil, fmt.Errorf("export: parquet page rows %d exceed row-group rows %d", opts.PageRows, opts.RowGroupRows)
	}
	copySchema := append([]Column(nil), schema...)
	for i := range copySchema {
		if _, err := parquetPhysicalType(copySchema[i].Type); err != nil {
			return nil, fmt.Errorf("export: parquet column %d %q: %w", i, copySchema[i].Name, err)
		}
	}
	metaRes, err := scope.Reserve(memory.TagMetadata, 0)
	if err != nil {
		return nil, err
	}
	w := &ParquetWriter{schema: copySchema, opts: opts, scope: scope, bm: bm, sink: sink, metadataReservation: metaRes}
	if err := w.allocateRowGroup(); err != nil {
		metaRes.Release()
		return nil, err
	}
	return w, nil
}

// WriteChunk 把输入按 RowGroupRows 切分并复制进受管 row-group vectors；边界不依赖输入 chunk 大小。
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
			dstRow := w.rowCount + n
			for col := range w.schema {
				if err := copyParquetScalar(w.rows[col], dstRow, chunk.Columns[col], srcRow+n, w.schema[col].Type); err != nil {
					return fmt.Errorf("export: parquet copy row %d column %d: %w", srcRow+n, col, err)
				}
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

// Flush 完成当前 partial row group 并推进下游；显式 Flush 可以提前结束 row group，但不会改变 page/multipart 规则。
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

// Close 写完最后 row group、footer length 与尾部 magic，然后关闭文件 sink。
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
	footer, err := parquet.EncodeFileMetaData(meta)
	if err != nil {
		return WriteResult{}, w.fail(err)
	}
	if len(footer) > math.MaxUint32 {
		return WriteResult{}, w.fail(errors.New("export: parquet footer exceeds uint32"))
	}
	if err := w.writeBytes(ctx, footer); err != nil {
		return WriteResult{}, w.fail(err)
	}
	var footerLen [4]byte
	binary.LittleEndian.PutUint32(footerLen[:], uint32(len(footer)))
	if err := w.writeBytes(ctx, footerLen[:]); err != nil {
		return WriteResult{}, w.fail(err)
	}
	if err := w.writeBytes(ctx, parquetMagic); err != nil {
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

// Abort 丢弃当前 incomplete Parquet file，并释放 row-group/page metadata ownership。
func (w *ParquetWriter) Abort(ctx context.Context) error {
	if w == nil || w.terminal {
		return nil
	}
	w.terminal = true
	err := w.sink.Abort(ctx)
	w.releaseResources()
	return err
}

// ensureStarted 写文件头 magic；任何 short write 都会作为完整文件失败处理。
func (w *ParquetWriter) ensureStarted(ctx context.Context) error {
	if w.started {
		return nil
	}
	if err := w.writeBytes(ctx, parquetMagic); err != nil {
		return err
	}
	w.started = true
	return nil
}

// allocateRowGroup 为每个 scalar 列预分配固定行数的 managed Vector。
func (w *ParquetWriter) allocateRowGroup() error {
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

// flushRowGroup 按列连续写出当前 row group，并在 footer metadata 增长前 Reserve Metadata。
func (w *ParquetWriter) flushRowGroup(ctx context.Context, allocateNext bool) error {
	if w.rowCount == 0 {
		return nil
	}
	group := parquet.RowGroup{Columns: make([]parquet.ColumnChunk, len(w.schema)), NumRows: int64(w.rowCount)}
	groupStart := w.offset
	var total int64
	for col := range w.schema {
		chunk, bytesWritten, err := w.writeColumnChunk(ctx, col)
		if err != nil {
			return err
		}
		group.Columns[col] = chunk
		total += bytesWritten
	}
	group.TotalByteSize = total
	compressed := total
	group.TotalCompressedSize = &compressed
	group.FileOffset = &groupStart
	ordinal := int16(len(w.rowGroups))
	group.Ordinal = &ordinal

	// row-group metadata 是可增长 control structure；先扩大 reservation 再 append。
	estimate := int64((len(w.rowGroups) + 1) * (256 + len(w.schema)*320))
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

// writeColumnChunk 把一个 scalar column 按 PageRows 切成独立 DataPage v1，返回 footer ColumnChunk metadata。
func (w *ParquetWriter) writeColumnChunk(ctx context.Context, col int) (parquet.ColumnChunk, int64, error) {
	physical, err := parquetPhysicalType(w.schema[col].Type)
	if err != nil {
		return parquet.ColumnChunk{}, 0, err
	}
	start := w.offset
	var total int64
	for pageStart := 0; pageStart < w.rowCount; pageStart += w.opts.PageRows {
		count := w.rowCount - pageStart
		if count > w.opts.PageRows {
			count = w.opts.PageRows
		}
		body, err := newParquetPageBuffer(w.scope, w.bm, w.opts.PageBufferSize)
		if err != nil {
			return parquet.ColumnChunk{}, total, err
		}
		if err := encodeDefinitionLevels(body, w.rows[col], pageStart, count); err != nil {
			body.Release()
			return parquet.ColumnChunk{}, total, err
		}
		if err := encodeParquetPlain(body, w.rows[col], w.schema[col].Type, pageStart, count); err != nil {
			body.Release()
			return parquet.ColumnChunk{}, total, err
		}
		if body.Len() > math.MaxInt32 {
			body.Release()
			return parquet.ColumnChunk{}, total, errors.New("export: parquet page exceeds int32 size")
		}
		header, err := parquet.EncodePageHeader(parquet.PageHeader{
			Type:                 parquet.PageData,
			UncompressedPageSize: int32(body.Len()),
			CompressedPageSize:   int32(body.Len()),
			Data: &parquet.DataPageHeader{
				NumValues:               int32(count),
				Encoding:                parquet.EncodingPlain,
				DefinitionLevelEncoding: parquet.EncodingRLE,
				RepetitionLevelEncoding: parquet.EncodingRLE,
			},
		})
		if err != nil {
			body.Release()
			return parquet.ColumnChunk{}, total, err
		}
		if err := w.writeBytes(ctx, header); err != nil {
			body.Release()
			return parquet.ColumnChunk{}, total, err
		}
		if err := w.writeBytes(ctx, body.Bytes()); err != nil {
			body.Release()
			return parquet.ColumnChunk{}, total, err
		}
		total += int64(len(header) + body.Len())
		body.Release()
	}
	meta := &parquet.ColumnMetaData{
		Type:                  physical,
		Encodings:             []parquet.Encoding{parquet.EncodingPlain, parquet.EncodingRLE},
		PathInSchema:          []string{w.schema[col].Name},
		Codec:                 parquet.CodecUncompressed,
		NumValues:             int64(w.rowCount),
		TotalUncompressedSize: total,
		TotalCompressedSize:   total,
		DataPageOffset:        start,
	}
	return parquet.ColumnChunk{FileOffset: start, MetaData: meta}, total, nil
}

// fileMetadata 根据固定 export schema 与已写 row groups 构造 footer metadata。
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
	return parquet.FileMetaData{Version: 1, Schema: flat, NumRows: w.totalRows, RowGroups: append([]parquet.RowGroup(nil), w.rowGroups...), CreatedBy: "fssql phase4 controlled parquet writer"}, nil
}

// writeBytes 完整写入一段字节并更新文件 offset；short write 绝不静默接受。
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

// fail 统一 Abort incomplete file，并释放所有 managed ownership。
func (w *ParquetWriter) fail(cause error) error {
	abortErr := w.sink.Abort(context.Background())
	w.releaseResources()
	return errors.Join(cause, abortErr)
}

// releaseRowGroup 释放当前 row-group vectors；STRING heap 等子资源由 Vector 递归释放。
func (w *ParquetWriter) releaseRowGroup() {
	for _, v := range w.rows {
		if v != nil {
			v.Release()
		}
	}
	w.rows = nil
	w.rowCount = 0
}

// releaseResources 是 writer 的唯一终态资源释放点。
func (w *ParquetWriter) releaseResources() {
	w.releaseRowGroup()
	if w.metadataReservation != nil {
		w.metadataReservation.Release()
		w.metadataReservation = nil
	}
	w.rowGroups = nil
}

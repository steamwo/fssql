package parquet

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// compactEncoder 是 Parquet footer/page header 使用的 Thrift Compact Protocol 最小受控编码器。
// 它只编码本引擎 writer 需要的 parquet-format 字段，不把第三方 Thrift 类型引入核心 ABI。
type compactEncoder struct {
	// buf 保存当前小型 metadata/header；大页 payload 不经过这里。
	buf []byte
}

// EncodeFileMetaData 按 parquet-format 的 TCompactProtocol 编码文件 footer metadata。
func EncodeFileMetaData(meta FileMetaData) ([]byte, error) {
	if len(meta.Schema) == 0 {
		return nil, errors.New("parquet: metadata missing schema")
	}
	if meta.NumRows < 0 {
		return nil, errors.New("parquet: negative row count")
	}
	w := &compactEncoder{}
	if err := w.fileMeta(meta); err != nil {
		return nil, err
	}
	return append([]byte(nil), w.buf...), nil
}

// EncodePageHeader 按 TCompactProtocol 编码一个 Parquet page header。
func EncodePageHeader(header PageHeader) ([]byte, error) {
	if header.UncompressedPageSize < 0 || header.CompressedPageSize < 0 {
		return nil, errors.New("parquet: negative page size")
	}
	switch header.Type {
	case PageData:
		if header.Data == nil {
			return nil, errors.New("parquet: data page missing header")
		}
	case PageDictionary:
		if header.Dictionary == nil {
			return nil, errors.New("parquet: dictionary page missing header")
		}
	case PageDataV2:
		if header.DataV2 == nil {
			return nil, errors.New("parquet: data v2 page missing header")
		}
	}
	w := &compactEncoder{}
	if err := w.pageHeader(header); err != nil {
		return nil, err
	}
	return append([]byte(nil), w.buf...), nil
}

// fieldHeader 写 Compact Protocol 字段头；优先使用 4-bit field-id delta 紧凑形式。
func (w *compactEncoder) fieldHeader(last, id int16, typ byte) int16 {
	delta := id - last
	if delta > 0 && delta <= 15 {
		w.buf = append(w.buf, byte(delta<<4)|typ)
		return id
	}
	w.buf = append(w.buf, typ)
	w.i16(id)
	return id
}

// stop 结束一个 Compact struct。
func (w *compactEncoder) stop() { w.buf = append(w.buf, ctStop) }

// uvarint 写 Compact Protocol 无符号 varint。
func (w *compactEncoder) uvarint(v uint64) {
	var scratch [10]byte
	n := binary.PutUvarint(scratch[:], v)
	w.buf = append(w.buf, scratch[:n]...)
}

// zigzag 将有符号整数映射到 Compact Protocol zigzag 编码。
func zigzag(v int64) uint64 { return uint64(v<<1) ^ uint64(v>>63) }

// i16 写 Compact i16。
func (w *compactEncoder) i16(v int16) { w.uvarint(zigzag(int64(v))) }

// i32 写 Compact i32。
func (w *compactEncoder) i32(v int32) { w.uvarint(zigzag(int64(v))) }

// i64 写 Compact i64。
func (w *compactEncoder) i64(v int64) { w.uvarint(zigzag(v)) }

// binary 写 length-prefixed binary/string。
func (w *compactEncoder) binary(v []byte) {
	w.uvarint(uint64(len(v)))
	w.buf = append(w.buf, v...)
}

// listHeader 写 Compact list header，并限制集合长度避免 metadata 无界膨胀。
func (w *compactEncoder) listHeader(elemType byte, n int) error {
	if n < 0 || n > maxCompactCollection {
		return fmt.Errorf("parquet: compact collection too large %d", n)
	}
	if n < 15 {
		w.buf = append(w.buf, byte(n<<4)|elemType)
		return nil
	}
	w.buf = append(w.buf, 0xf0|elemType)
	w.uvarint(uint64(n))
	return nil
}

// boolField 把 bool 值编码在字段头类型中，不额外写 payload。
func (w *compactEncoder) boolField(last, id int16, value bool) int16 {
	typ := byte(ctBoolFalse)
	if value {
		typ = ctBoolTrue
	}
	return w.fieldHeader(last, id, typ)
}

// fileMeta 编码 FileMetaData 的 Phase 4 writer 所需字段。
func (w *compactEncoder) fileMeta(meta FileMetaData) error {
	last := int16(0)
	last = w.fieldHeader(last, 1, ctI32)
	w.i32(meta.Version)
	last = w.fieldHeader(last, 2, ctList)
	if err := w.listHeader(ctStruct, len(meta.Schema)); err != nil {
		return err
	}
	for i := range meta.Schema {
		if err := w.schemaElement(meta.Schema[i]); err != nil {
			return err
		}
	}
	last = w.fieldHeader(last, 3, ctI64)
	w.i64(meta.NumRows)
	if len(meta.RowGroups) > 0 {
		last = w.fieldHeader(last, 4, ctList)
		if err := w.listHeader(ctStruct, len(meta.RowGroups)); err != nil {
			return err
		}
		for i := range meta.RowGroups {
			if err := w.rowGroup(meta.RowGroups[i]); err != nil {
				return err
			}
		}
	}
	if len(meta.KeyValueMetadata) > 0 {
		last = w.fieldHeader(last, 5, ctList)
		if err := w.listHeader(ctStruct, len(meta.KeyValueMetadata)); err != nil {
			return err
		}
		for i := range meta.KeyValueMetadata {
			w.keyValue(meta.KeyValueMetadata[i])
		}
	}
	if meta.CreatedBy != "" {
		last = w.fieldHeader(last, 6, ctBinary)
		w.binary([]byte(meta.CreatedBy))
	}
	if len(meta.ColumnOrders) > 0 {
		last = w.fieldHeader(last, 7, ctList)
		if err := w.columnOrders(meta.ColumnOrders); err != nil {
			return err
		}
	}
	_ = last
	w.stop()
	return nil
}

func (w *compactEncoder) columnOrders(orders []ColumnOrder) error {
	if err := w.listHeader(ctStruct, len(orders)); err != nil {
		return err
	}
	for _, order := range orders {
		if order != TypeDefinedOrder {
			return fmt.Errorf("parquet: unsupported column order %d", order)
		}
		w.fieldHeader(0, 1, ctStruct) // ColumnOrder.TYPE_ORDER
		w.stop()                      // empty TypeDefinedOrder
		w.stop()                      // ColumnOrder union
	}
	return nil
}

// schemaElement 编码一个扁平化 SchemaElement。
func (w *compactEncoder) schemaElement(s SchemaElement) error {
	if s.Name == "" {
		return errors.New("parquet: schema element missing name")
	}
	last := int16(0)
	if s.Type != nil {
		last = w.fieldHeader(last, 1, ctI32)
		w.i32(int32(*s.Type))
	}
	if s.TypeLength != nil {
		last = w.fieldHeader(last, 2, ctI32)
		w.i32(*s.TypeLength)
	}
	if s.Repetition != nil {
		last = w.fieldHeader(last, 3, ctI32)
		w.i32(int32(*s.Repetition))
	}
	last = w.fieldHeader(last, 4, ctBinary)
	w.binary([]byte(s.Name))
	if s.NumChildren != nil {
		last = w.fieldHeader(last, 5, ctI32)
		w.i32(*s.NumChildren)
	}
	if s.Converted != nil {
		last = w.fieldHeader(last, 6, ctI32)
		w.i32(int32(*s.Converted))
	}
	if s.Scale != nil {
		last = w.fieldHeader(last, 7, ctI32)
		w.i32(*s.Scale)
	}
	if s.Precision != nil {
		last = w.fieldHeader(last, 8, ctI32)
		w.i32(*s.Precision)
	}
	if s.FieldID != nil {
		last = w.fieldHeader(last, 9, ctI32)
		w.i32(*s.FieldID)
	}
	if s.Logical != nil {
		last = w.fieldHeader(last, 10, ctStruct)
		if err := w.logicalType(*s.Logical); err != nil {
			return err
		}
	}
	_ = last
	w.stop()
	return nil
}

// logicalType 编码 parquet-format LogicalType union 中 v1 已支持的分支。
func (w *compactEncoder) logicalType(l LogicalType) error {
	last := int16(0)
	switch l.Kind {
	case LogicalString:
		last = w.fieldHeader(last, 1, ctStruct)
		w.stop()
	case LogicalMap:
		last = w.fieldHeader(last, 2, ctStruct)
		w.stop()
	case LogicalList:
		last = w.fieldHeader(last, 3, ctStruct)
		w.stop()
	case LogicalEnum:
		last = w.fieldHeader(last, 4, ctStruct)
		w.stop()
	case LogicalDecimal:
		if l.Decimal == nil {
			return errors.New("parquet: decimal logical type missing detail")
		}
		last = w.fieldHeader(last, 5, ctStruct)
		w.decimalLogical(*l.Decimal)
	case LogicalDate:
		last = w.fieldHeader(last, 6, ctStruct)
		w.stop()
	case LogicalTime:
		if l.Time == nil {
			return errors.New("parquet: time logical type missing detail")
		}
		last = w.fieldHeader(last, 7, ctStruct)
		w.timeLogical(*l.Time)
	case LogicalTimestamp:
		if l.Timestamp == nil {
			return errors.New("parquet: timestamp logical type missing detail")
		}
		last = w.fieldHeader(last, 8, ctStruct)
		w.timestampLogical(*l.Timestamp)
	case LogicalInteger:
		if l.Integer == nil {
			return errors.New("parquet: integer logical type missing detail")
		}
		last = w.fieldHeader(last, 10, ctStruct)
		w.integerLogical(*l.Integer)
	default:
		return fmt.Errorf("parquet: unsupported logical type %d", l.Kind)
	}
	_ = last
	w.stop()
	return nil
}

// decimalLogical 编码 DecimalType(scale, precision)。
func (w *compactEncoder) decimalLogical(d DecimalLogical) {
	last := int16(0)
	last = w.fieldHeader(last, 1, ctI32)
	w.i32(d.Scale)
	last = w.fieldHeader(last, 2, ctI32)
	w.i32(d.Precision)
	_ = last
	w.stop()
}

// timestampLogical 编码 TimestampType。
func (w *compactEncoder) timestampLogical(t TimestampLogical) {
	last := int16(0)
	last = w.boolField(last, 1, t.IsAdjustedToUTC)
	last = w.fieldHeader(last, 2, ctStruct)
	w.timeUnit(t.Unit)
	_ = last
	w.stop()
}

// timeLogical 编码 TimeType。
func (w *compactEncoder) timeLogical(t TimeLogical) {
	last := int16(0)
	last = w.boolField(last, 1, t.IsAdjustedToUTC)
	last = w.fieldHeader(last, 2, ctStruct)
	w.timeUnit(t.Unit)
	_ = last
	w.stop()
}

// integerLogical 编码 IntType(bitWidth, signed)。
func (w *compactEncoder) integerLogical(x IntegerLogical) {
	last := int16(0)
	last = w.fieldHeader(last, 1, ctByte)
	w.buf = append(w.buf, byte(x.BitWidth))
	last = w.boolField(last, 2, x.IsSigned)
	_ = last
	w.stop()
}

// timeUnit 编码 TimeUnit union。
func (w *compactEncoder) timeUnit(unit TimeUnit) {
	id := int16(unit) + 1
	w.fieldHeader(0, id, ctStruct)
	w.stop()
	w.stop()
}

// rowGroup 编码 RowGroup，列块 metadata 保持在 footer 中而不是外部 sidecar。
func (w *compactEncoder) rowGroup(group RowGroup) error {
	if group.NumRows < 0 || group.TotalByteSize < 0 {
		return errors.New("parquet: invalid row group sizes")
	}
	last := int16(0)
	last = w.fieldHeader(last, 1, ctList)
	if err := w.listHeader(ctStruct, len(group.Columns)); err != nil {
		return err
	}
	for i := range group.Columns {
		if err := w.columnChunk(group.Columns[i]); err != nil {
			return err
		}
	}
	last = w.fieldHeader(last, 2, ctI64)
	w.i64(group.TotalByteSize)
	last = w.fieldHeader(last, 3, ctI64)
	w.i64(group.NumRows)
	if group.FileOffset != nil {
		last = w.fieldHeader(last, 5, ctI64)
		w.i64(*group.FileOffset)
	}
	if group.TotalCompressedSize != nil {
		last = w.fieldHeader(last, 6, ctI64)
		w.i64(*group.TotalCompressedSize)
	}
	if group.Ordinal != nil {
		last = w.fieldHeader(last, 7, ctI16)
		w.i16(*group.Ordinal)
	}
	_ = last
	w.stop()
	return nil
}

// columnChunk 编码 ColumnChunk；Phase 4 不写 page/column index，因此对应 optional 字段保持省略。
func (w *compactEncoder) columnChunk(chunk ColumnChunk) error {
	if chunk.MetaData == nil {
		return errors.New("parquet: column chunk missing metadata")
	}
	last := int16(0)
	if chunk.FilePath != "" {
		last = w.fieldHeader(last, 1, ctBinary)
		w.binary([]byte(chunk.FilePath))
	}
	last = w.fieldHeader(last, 2, ctI64)
	w.i64(chunk.FileOffset)
	last = w.fieldHeader(last, 3, ctStruct)
	if err := w.columnMeta(*chunk.MetaData); err != nil {
		return err
	}
	if chunk.OffsetIndexOffset != nil {
		last = w.fieldHeader(last, 4, ctI64)
		w.i64(*chunk.OffsetIndexOffset)
	}
	if chunk.OffsetIndexLength != nil {
		last = w.fieldHeader(last, 5, ctI32)
		w.i32(*chunk.OffsetIndexLength)
	}
	if chunk.ColumnIndexOffset != nil {
		last = w.fieldHeader(last, 6, ctI64)
		w.i64(*chunk.ColumnIndexOffset)
	}
	if chunk.ColumnIndexLength != nil {
		last = w.fieldHeader(last, 7, ctI32)
		w.i32(*chunk.ColumnIndexLength)
	}
	_ = last
	w.stop()
	return nil
}

// columnMeta 编码 ColumnMetaData required fields 与 writer 已产生的可选统计。
func (w *compactEncoder) columnMeta(meta ColumnMetaData) error {
	if meta.NumValues < 0 || meta.TotalUncompressedSize < 0 || meta.TotalCompressedSize < 0 || meta.DataPageOffset < 0 {
		return errors.New("parquet: invalid column metadata sizes")
	}
	if len(meta.PathInSchema) == 0 || len(meta.Encodings) == 0 {
		return errors.New("parquet: column metadata missing path or encoding")
	}
	last := int16(0)
	last = w.fieldHeader(last, 1, ctI32)
	w.i32(int32(meta.Type))
	last = w.fieldHeader(last, 2, ctList)
	if err := w.listHeader(ctI32, len(meta.Encodings)); err != nil {
		return err
	}
	for _, enc := range meta.Encodings {
		w.i32(int32(enc))
	}
	last = w.fieldHeader(last, 3, ctList)
	if err := w.listHeader(ctBinary, len(meta.PathInSchema)); err != nil {
		return err
	}
	for _, p := range meta.PathInSchema {
		w.binary([]byte(p))
	}
	last = w.fieldHeader(last, 4, ctI32)
	w.i32(int32(meta.Codec))
	last = w.fieldHeader(last, 5, ctI64)
	w.i64(meta.NumValues)
	last = w.fieldHeader(last, 6, ctI64)
	w.i64(meta.TotalUncompressedSize)
	last = w.fieldHeader(last, 7, ctI64)
	w.i64(meta.TotalCompressedSize)
	last = w.fieldHeader(last, 9, ctI64)
	w.i64(meta.DataPageOffset)
	if meta.IndexPageOffset != nil {
		last = w.fieldHeader(last, 10, ctI64)
		w.i64(*meta.IndexPageOffset)
	}
	if meta.DictionaryPageOffset != nil {
		last = w.fieldHeader(last, 11, ctI64)
		w.i64(*meta.DictionaryPageOffset)
	}
	if meta.Statistics != nil {
		last = w.fieldHeader(last, 12, ctStruct)
		w.statistics(*meta.Statistics)
	}
	_ = last
	w.stop()
	return nil
}

// statistics 编码 footer/page statistics 中当前模型支持的字段。
func (w *compactEncoder) statistics(s Statistics) {
	last := int16(0)
	if s.Max != nil {
		last = w.fieldHeader(last, 1, ctBinary)
		w.binary(s.Max)
	}
	if s.Min != nil {
		last = w.fieldHeader(last, 2, ctBinary)
		w.binary(s.Min)
	}
	if s.NullCount != nil {
		last = w.fieldHeader(last, 3, ctI64)
		w.i64(*s.NullCount)
	}
	if s.DistinctCount != nil {
		last = w.fieldHeader(last, 4, ctI64)
		w.i64(*s.DistinctCount)
	}
	if s.MaxValue != nil {
		last = w.fieldHeader(last, 5, ctBinary)
		w.binary(s.MaxValue)
	}
	if s.MinValue != nil {
		last = w.fieldHeader(last, 6, ctBinary)
		w.binary(s.MinValue)
	}
	if s.IsMaxValueExact != nil {
		last = w.boolField(last, 7, *s.IsMaxValueExact)
	}
	if s.IsMinValueExact != nil {
		last = w.boolField(last, 8, *s.IsMinValueExact)
	}
	if s.NaNCount != nil {
		last = w.fieldHeader(last, 9, ctI64)
		w.i64(*s.NaNCount)
	}
	_ = last
	w.stop()
}

// keyValue 编码 footer key/value metadata。
func (w *compactEncoder) keyValue(kv KeyValue) {
	last := int16(0)
	last = w.fieldHeader(last, 1, ctBinary)
	w.binary([]byte(kv.Key))
	if kv.Value != nil {
		last = w.fieldHeader(last, 2, ctBinary)
		w.binary([]byte(*kv.Value))
	}
	_ = last
	w.stop()
}

// pageHeader 编码 PageHeader 以及对应的 DataPage/Dictionary/DataPageV2 子结构。
func (w *compactEncoder) pageHeader(h PageHeader) error {
	last := int16(0)
	last = w.fieldHeader(last, 1, ctI32)
	w.i32(int32(h.Type))
	last = w.fieldHeader(last, 2, ctI32)
	w.i32(h.UncompressedPageSize)
	last = w.fieldHeader(last, 3, ctI32)
	w.i32(h.CompressedPageSize)
	if h.CRC != nil {
		last = w.fieldHeader(last, 4, ctI32)
		w.i32(*h.CRC)
	}
	switch h.Type {
	case PageData:
		last = w.fieldHeader(last, 5, ctStruct)
		w.dataPage(*h.Data)
	case PageDictionary:
		last = w.fieldHeader(last, 7, ctStruct)
		w.dictionaryPage(*h.Dictionary)
	case PageDataV2:
		last = w.fieldHeader(last, 8, ctStruct)
		w.dataPageV2(*h.DataV2)
	}
	_ = last
	w.stop()
	return nil
}

// dataPage 编码 DataPageHeader v1。
func (w *compactEncoder) dataPage(h DataPageHeader) {
	last := int16(0)
	last = w.fieldHeader(last, 1, ctI32)
	w.i32(h.NumValues)
	last = w.fieldHeader(last, 2, ctI32)
	w.i32(int32(h.Encoding))
	last = w.fieldHeader(last, 3, ctI32)
	w.i32(int32(h.DefinitionLevelEncoding))
	last = w.fieldHeader(last, 4, ctI32)
	w.i32(int32(h.RepetitionLevelEncoding))
	if h.Statistics != nil {
		last = w.fieldHeader(last, 5, ctStruct)
		w.statistics(*h.Statistics)
	}
	_ = last
	w.stop()
}

// dictionaryPage 编码 DictionaryPageHeader。
func (w *compactEncoder) dictionaryPage(h DictionaryPageHeader) {
	last := int16(0)
	last = w.fieldHeader(last, 1, ctI32)
	w.i32(h.NumValues)
	last = w.fieldHeader(last, 2, ctI32)
	w.i32(int32(h.Encoding))
	last = w.boolField(last, 3, h.IsSorted)
	_ = last
	w.stop()
}

// dataPageV2 编码 DataPageHeaderV2。
func (w *compactEncoder) dataPageV2(h DataPageHeaderV2) {
	last := int16(0)
	last = w.fieldHeader(last, 1, ctI32)
	w.i32(h.NumValues)
	last = w.fieldHeader(last, 2, ctI32)
	w.i32(h.NumNulls)
	last = w.fieldHeader(last, 3, ctI32)
	w.i32(h.NumRows)
	last = w.fieldHeader(last, 4, ctI32)
	w.i32(int32(h.Encoding))
	last = w.fieldHeader(last, 5, ctI32)
	w.i32(h.DefinitionLevelsByteLength)
	last = w.fieldHeader(last, 6, ctI32)
	w.i32(h.RepetitionLevelsByteLength)
	last = w.boolField(last, 7, h.IsCompressed)
	if h.Statistics != nil {
		last = w.fieldHeader(last, 8, ctStruct)
		w.statistics(*h.Statistics)
	}
	_ = last
	w.stop()
}

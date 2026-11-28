package parquet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

const (
	ctStop byte = iota
	ctBoolTrue
	ctBoolFalse
	ctByte
	ctI16
	ctI32
	ctI64
	ctDouble
	ctBinary
	ctList
	ctSet
	ctMap
	ctStruct
)

const maxCompactCollection = 1 << 20
const maxCompactBinary = 64 << 20
const maxCompactDepth = 64

type compactReader struct {
	b   []byte
	off int
}

func newCompactReader(b []byte) *compactReader { return &compactReader{b: b} }
func (r *compactReader) remaining() int        { return len(r.b) - r.off }
func (r *compactReader) byte() (byte, error) {
	if r.off >= len(r.b) {
		return 0, ioErr()
	}
	v := r.b[r.off]
	r.off++
	return v, nil
}
func ioErr() error { return errors.New("parquet: unexpected end of compact thrift") }
func (r *compactReader) uvarint() (uint64, error) {
	var x uint64
	for s := uint(0); s < 64; s += 7 {
		b, e := r.byte()
		if e != nil {
			return 0, e
		}
		if b < 0x80 {
			return x | uint64(b)<<s, nil
		}
		x |= uint64(b&0x7f) << s
	}
	return 0, errors.New("parquet: compact varint overflow")
}
func zz(v uint64) int64 { return int64(v>>1) ^ -int64(v&1) }
func (r *compactReader) i16() (int16, error) {
	v, e := r.uvarint()
	return int16(zz(v)), e
}
func (r *compactReader) i32() (int32, error) {
	v, e := r.uvarint()
	return int32(zz(v)), e
}
func (r *compactReader) i64() (int64, error) {
	v, e := r.uvarint()
	return zz(v), e
}
func (r *compactReader) binary() ([]byte, error) {
	n, e := r.uvarint()
	if e != nil {
		return nil, e
	}
	if n > maxCompactBinary || n > uint64(r.remaining()) {
		return nil, fmt.Errorf("parquet: invalid compact binary length %d", n)
	}
	v := r.b[r.off : r.off+int(n)]
	r.off += int(n)
	return v, nil
}
func (r *compactReader) double() (float64, error) {
	if r.remaining() < 8 {
		return 0, ioErr()
	}
	u := binary.LittleEndian.Uint64(r.b[r.off : r.off+8])
	r.off += 8
	return math.Float64frombits(u), nil
}

type fieldHeader struct {
	id        int16
	typ       byte
	boolValue *bool
}

func (r *compactReader) field(last int16) (fieldHeader, int16, error) {
	b, e := r.byte()
	if e != nil {
		return fieldHeader{}, last, e
	}
	if b == ctStop {
		return fieldHeader{typ: ctStop}, last, nil
	}
	delta := int16(b >> 4)
	typ := b & 0x0f
	id := last + delta
	if delta == 0 {
		id, e = r.i16()
		if e != nil {
			return fieldHeader{}, last, e
		}
	}
	var bv *bool
	if typ == ctBoolTrue || typ == ctBoolFalse {
		v := typ == ctBoolTrue
		bv = &v
	}
	return fieldHeader{id: id, typ: typ, boolValue: bv}, id, nil
}
func (r *compactReader) listHeader() (byte, int, error) {
	b, e := r.byte()
	if e != nil {
		return 0, 0, e
	}
	n := int(b >> 4)
	typ := b & 0x0f
	if n == 15 {
		u, e := r.uvarint()
		if e != nil {
			return 0, 0, e
		}
		if u > maxCompactCollection {
			return 0, 0, fmt.Errorf("parquet: compact collection too large %d", u)
		}
		n = int(u)
	}
	// Every collection value occupies at least one byte, including an empty
	// struct's STOP marker. Reject truncated counts before allocating slices.
	if n > r.remaining() {
		return 0, 0, fmt.Errorf("parquet: compact collection length %d exceeds remaining bytes %d", n, r.remaining())
	}
	return typ, n, nil
}
func (r *compactReader) boolValue(f fieldHeader) (bool, error) {
	if f.boolValue != nil {
		return *f.boolValue, nil
	}
	b, e := r.byte()
	if e != nil {
		return false, e
	}
	if b == ctBoolTrue {
		return true, nil
	}
	if b == ctBoolFalse {
		return false, nil
	}
	return false, fmt.Errorf("parquet: invalid compact bool %d", b)
}
func (r *compactReader) skip(typ byte, depth int) error {
	if depth > maxCompactDepth {
		return errors.New("parquet: compact nesting too deep")
	}
	switch typ {
	case ctBoolTrue, ctBoolFalse:
		return nil
	case ctByte:
		_, e := r.byte()
		return e
	case ctI16, ctI32, ctI64:
		_, e := r.uvarint()
		return e
	case ctDouble:
		_, e := r.double()
		return e
	case ctBinary:
		_, e := r.binary()
		return e
	case ctList, ctSet:
		et, n, e := r.listHeader()
		if e != nil {
			return e
		}
		for i := 0; i < n; i++ {
			if e := r.skip(et, depth+1); e != nil {
				return e
			}
		}
		return nil
	case ctMap:
		n, e := r.uvarint()
		if e != nil {
			return e
		}
		if n == 0 {
			return nil
		}
		if n > maxCompactCollection {
			return fmt.Errorf("parquet: compact map too large %d", n)
		}
		types, e := r.byte()
		if e != nil {
			return e
		}
		kt, vt := types>>4, types&0xf
		for i := uint64(0); i < n; i++ {
			if e := r.skip(kt, depth+1); e != nil {
				return e
			}
			if e := r.skip(vt, depth+1); e != nil {
				return e
			}
		}
		return nil
	case ctStruct:
		last := int16(0)
		for {
			f, nl, e := r.field(last)
			if e != nil {
				return e
			}
			last = nl
			if f.typ == ctStop {
				return nil
			}
			if e := r.skip(f.typ, depth+1); e != nil {
				return e
			}
		}
	}
	return fmt.Errorf("parquet: unsupported compact type %d", typ)
}

func DecodeFileMetaData(b []byte) (FileMetaData, error) {
	r := newCompactReader(b)
	m, e := r.fileMeta()
	if e != nil {
		return FileMetaData{}, e
	}
	return m, nil
}
func (r *compactReader) fileMeta() (FileMetaData, error) {
	var m FileMetaData
	last := int16(0)
	for {
		f, nl, e := r.field(last)
		if e != nil {
			return m, e
		}
		last = nl
		if f.typ == ctStop {
			break
		}
		switch f.id {
		case 1:
			v, e := r.i32()
			if e != nil {
				return m, e
			}
			m.Version = v
		case 2:
			v, e := r.schemaList()
			if e != nil {
				return m, e
			}
			m.Schema = v
		case 3:
			v, e := r.i64()
			if e != nil {
				return m, e
			}
			m.NumRows = v
		case 4:
			v, e := r.rowGroupList()
			if e != nil {
				return m, e
			}
			m.RowGroups = v
		case 5:
			v, e := r.keyValueList()
			if e != nil {
				return m, e
			}
			m.KeyValueMetadata = v
		case 6:
			v, e := r.binary()
			if e != nil {
				return m, e
			}
			m.CreatedBy = string(v)
		case 7:
			v, e := r.columnOrderList()
			if e != nil {
				return m, e
			}
			m.ColumnOrders = v
		case 8, 9:
			return m, errors.New("parquet: plaintext-footer encryption is unsupported")
		default:
			if e := r.skip(f.typ, 1); e != nil {
				return m, e
			}
		}
	}
	if len(m.Schema) == 0 {
		return m, errors.New("parquet: metadata missing schema")
	}
	return m, nil
}

func (r *compactReader) columnOrderList() ([]ColumnOrder, error) {
	et, n, err := r.listHeader()
	if err != nil {
		return nil, err
	}
	if et != ctStruct {
		return nil, fmt.Errorf("parquet: column order list type %d", et)
	}
	orders := make([]ColumnOrder, n)
	for i := range orders {
		last := int16(0)
		for {
			field, next, err := r.field(last)
			if err != nil {
				return nil, err
			}
			last = next
			if field.typ == ctStop {
				break
			}
			if field.id == 1 && field.typ == ctStruct {
				orders[i] = TypeDefinedOrder
			}
			if err := r.skip(field.typ, 1); err != nil {
				return nil, err
			}
		}
	}
	return orders, nil
}
func (r *compactReader) schemaList() ([]SchemaElement, error) {
	et, n, e := r.listHeader()
	if e != nil {
		return nil, e
	}
	if et != ctStruct {
		return nil, fmt.Errorf("parquet: schema list type %d", et)
	}
	out := make([]SchemaElement, n)
	for i := range out {
		v, e := r.schemaElement()
		if e != nil {
			return nil, e
		}
		out[i] = v
	}
	return out, nil
}
func (r *compactReader) schemaElement() (SchemaElement, error) {
	var s SchemaElement
	last := int16(0)
	for {
		f, nl, e := r.field(last)
		if e != nil {
			return s, e
		}
		last = nl
		if f.typ == ctStop {
			return s, nil
		}
		switch f.id {
		case 1:
			v, e := r.i32()
			if e != nil {
				return s, e
			}
			x := PhysicalType(v)
			s.Type = &x
		case 2:
			v, e := r.i32()
			if e != nil {
				return s, e
			}
			s.TypeLength = &v
		case 3:
			v, e := r.i32()
			if e != nil {
				return s, e
			}
			x := RepetitionType(v)
			s.Repetition = &x
		case 4:
			v, e := r.binary()
			if e != nil {
				return s, e
			}
			s.Name = string(v)
		case 5:
			v, e := r.i32()
			if e != nil {
				return s, e
			}
			s.NumChildren = &v
		case 6:
			v, e := r.i32()
			if e != nil {
				return s, e
			}
			x := ConvertedType(v)
			s.Converted = &x
		case 7:
			v, e := r.i32()
			if e != nil {
				return s, e
			}
			s.Scale = &v
		case 8:
			v, e := r.i32()
			if e != nil {
				return s, e
			}
			s.Precision = &v
		case 9:
			v, e := r.i32()
			if e != nil {
				return s, e
			}
			s.FieldID = &v
		case 10:
			v, e := r.logicalType()
			if e != nil {
				return s, e
			}
			s.Logical = &v
		default:
			if e := r.skip(f.typ, 1); e != nil {
				return s, e
			}
		}
	}
}
func (r *compactReader) logicalType() (LogicalType, error) {
	var l LogicalType
	last := int16(0)
	for {
		f, nl, e := r.field(last)
		if e != nil {
			return l, e
		}
		last = nl
		if f.typ == ctStop {
			return l, nil
		}
		switch f.id {
		case 1:
			l.Kind = LogicalString
			if e := r.skip(f.typ, 1); e != nil {
				return l, e
			}
		case 2:
			l.Kind = LogicalMap
			if e := r.skip(f.typ, 1); e != nil {
				return l, e
			}
		case 3:
			l.Kind = LogicalList
			if e := r.skip(f.typ, 1); e != nil {
				return l, e
			}
		case 4:
			l.Kind = LogicalEnum
			if e := r.skip(f.typ, 1); e != nil {
				return l, e
			}
		case 5:
			l.Kind = LogicalDecimal
			v, e := r.decimalLogical()
			if e != nil {
				return l, e
			}
			l.Decimal = &v
		case 6:
			l.Kind = LogicalDate
			if e := r.skip(f.typ, 1); e != nil {
				return l, e
			}
		case 7:
			l.Kind = LogicalTime
			v, e := r.timeLogical()
			if e != nil {
				return l, e
			}
			l.Time = &v
		case 8:
			l.Kind = LogicalTimestamp
			v, e := r.timestampLogical()
			if e != nil {
				return l, e
			}
			l.Timestamp = &v
		case 10:
			l.Kind = LogicalInteger
			v, e := r.integerLogical()
			if e != nil {
				return l, e
			}
			l.Integer = &v
		default:
			l.Kind = LogicalUnknown
			if e := r.skip(f.typ, 1); e != nil {
				return l, e
			}
		}
	}
}
func (r *compactReader) decimalLogical() (DecimalLogical, error) {
	var d DecimalLogical
	last := int16(0)
	for {
		f, nl, e := r.field(last)
		if e != nil {
			return d, e
		}
		last = nl
		if f.typ == ctStop {
			return d, nil
		}
		switch f.id {
		case 1:
			v, e := r.i32()
			if e != nil {
				return d, e
			}
			d.Scale = v
		case 2:
			v, e := r.i32()
			if e != nil {
				return d, e
			}
			d.Precision = v
		default:
			if e := r.skip(f.typ, 1); e != nil {
				return d, e
			}
		}
	}
}
func (r *compactReader) timeUnit() (TimeUnit, error) {
	var unit = UnitMillis
	last := int16(0)
	for {
		f, nl, e := r.field(last)
		if e != nil {
			return unit, e
		}
		last = nl
		if f.typ == ctStop {
			return unit, nil
		}
		switch f.id {
		case 1:
			unit = UnitMillis
		case 2:
			unit = UnitMicros
		case 3:
			unit = UnitNanos
		}
		if e := r.skip(f.typ, 1); e != nil {
			return unit, e
		}
	}
}
func (r *compactReader) timestampLogical() (TimestampLogical, error) {
	var t TimestampLogical
	last := int16(0)
	for {
		f, nl, e := r.field(last)
		if e != nil {
			return t, e
		}
		last = nl
		if f.typ == ctStop {
			return t, nil
		}
		switch f.id {
		case 1:
			v, e := r.boolValue(f)
			if e != nil {
				return t, e
			}
			t.IsAdjustedToUTC = v
		case 2:
			v, e := r.timeUnit()
			if e != nil {
				return t, e
			}
			t.Unit = v
		default:
			if e := r.skip(f.typ, 1); e != nil {
				return t, e
			}
		}
	}
}
func (r *compactReader) timeLogical() (TimeLogical, error) {
	var t TimeLogical
	last := int16(0)
	for {
		f, nl, e := r.field(last)
		if e != nil {
			return t, e
		}
		last = nl
		if f.typ == ctStop {
			return t, nil
		}
		switch f.id {
		case 1:
			v, e := r.boolValue(f)
			if e != nil {
				return t, e
			}
			t.IsAdjustedToUTC = v
		case 2:
			v, e := r.timeUnit()
			if e != nil {
				return t, e
			}
			t.Unit = v
		default:
			if e := r.skip(f.typ, 1); e != nil {
				return t, e
			}
		}
	}
}
func (r *compactReader) integerLogical() (IntegerLogical, error) {
	var x IntegerLogical
	last := int16(0)
	for {
		f, nl, e := r.field(last)
		if e != nil {
			return x, e
		}
		last = nl
		if f.typ == ctStop {
			return x, nil
		}
		switch f.id {
		case 1:
			v, e := r.byte()
			if e != nil {
				return x, e
			}
			x.BitWidth = int8(v)
		case 2:
			v, e := r.boolValue(f)
			if e != nil {
				return x, e
			}
			x.IsSigned = v
		default:
			if e := r.skip(f.typ, 1); e != nil {
				return x, e
			}
		}
	}
}
func (r *compactReader) rowGroupList() ([]RowGroup, error) {
	et, n, e := r.listHeader()
	if e != nil {
		return nil, e
	}
	if et != ctStruct {
		return nil, fmt.Errorf("parquet: row group list type %d", et)
	}
	out := make([]RowGroup, n)
	for i := range out {
		v, e := r.rowGroup()
		if e != nil {
			return nil, e
		}
		out[i] = v
	}
	return out, nil
}
func (r *compactReader) rowGroup() (RowGroup, error) {
	var g RowGroup
	last := int16(0)
	for {
		f, nl, e := r.field(last)
		if e != nil {
			return g, e
		}
		last = nl
		if f.typ == ctStop {
			return g, nil
		}
		switch f.id {
		case 1:
			v, e := r.columnChunkList()
			if e != nil {
				return g, e
			}
			g.Columns = v
		case 2:
			v, e := r.i64()
			if e != nil {
				return g, e
			}
			g.TotalByteSize = v
		case 3:
			v, e := r.i64()
			if e != nil {
				return g, e
			}
			g.NumRows = v
		case 5:
			v, e := r.i64()
			if e != nil {
				return g, e
			}
			g.FileOffset = &v
		case 6:
			v, e := r.i64()
			if e != nil {
				return g, e
			}
			g.TotalCompressedSize = &v
		case 7:
			v, e := r.i16()
			if e != nil {
				return g, e
			}
			g.Ordinal = &v
		default:
			if e := r.skip(f.typ, 1); e != nil {
				return g, e
			}
		}
	}
}
func (r *compactReader) columnChunkList() ([]ColumnChunk, error) {
	et, n, e := r.listHeader()
	if e != nil {
		return nil, e
	}
	if et != ctStruct {
		return nil, fmt.Errorf("parquet: column chunk list type %d", et)
	}
	out := make([]ColumnChunk, n)
	for i := range out {
		v, e := r.columnChunk()
		if e != nil {
			return nil, e
		}
		out[i] = v
	}
	return out, nil
}
func (r *compactReader) columnChunk() (ColumnChunk, error) {
	var c ColumnChunk
	last := int16(0)
	for {
		f, nl, e := r.field(last)
		if e != nil {
			return c, e
		}
		last = nl
		if f.typ == ctStop {
			return c, nil
		}
		switch f.id {
		case 1:
			v, e := r.binary()
			if e != nil {
				return c, e
			}
			c.FilePath = string(v)
		case 2:
			v, e := r.i64()
			if e != nil {
				return c, e
			}
			c.FileOffset = v
		case 3:
			v, e := r.columnMeta()
			if e != nil {
				return c, e
			}
			c.MetaData = &v
		case 4:
			v, e := r.i64()
			if e != nil {
				return c, e
			}
			c.OffsetIndexOffset = &v
		case 5:
			v, e := r.i32()
			if e != nil {
				return c, e
			}
			c.OffsetIndexLength = &v
		case 6:
			v, e := r.i64()
			if e != nil {
				return c, e
			}
			c.ColumnIndexOffset = &v
		case 7:
			v, e := r.i32()
			if e != nil {
				return c, e
			}
			c.ColumnIndexLength = &v
		case 8:
			if f.typ != ctStruct {
				return c, errors.New("parquet: invalid column crypto metadata")
			}
			crypto, _, err := r.field(0)
			if err != nil {
				return c, err
			}
			if crypto.id != 1 || crypto.typ != ctStruct {
				return c, errors.New("parquet: only encryption with the footer key is supported")
			}
			if err := r.skip(ctStruct, 1); err != nil {
				return c, err
			}
			if err := r.expectCryptoStop(); err != nil {
				return c, err
			}
			c.usesFooterKey = true
		case 9:
			return c, errors.New("parquet: encrypted column metadata requires an unsupported column key")
		default:
			if e := r.skip(f.typ, 1); e != nil {
				return c, e
			}
		}
	}
}
func (r *compactReader) columnMeta() (ColumnMetaData, error) {
	var c ColumnMetaData
	last := int16(0)
	for {
		f, nl, e := r.field(last)
		if e != nil {
			return c, e
		}
		last = nl
		if f.typ == ctStop {
			return c, nil
		}
		switch f.id {
		case 1:
			v, e := r.i32()
			if e != nil {
				return c, e
			}
			c.Type = PhysicalType(v)
		case 2:
			v, e := r.i32List()
			if e != nil {
				return c, e
			}
			c.Encodings = make([]Encoding, len(v))
			for i := range v {
				c.Encodings[i] = Encoding(v[i])
			}
		case 3:
			v, e := r.stringList()
			if e != nil {
				return c, e
			}
			c.PathInSchema = v
		case 4:
			v, e := r.i32()
			if e != nil {
				return c, e
			}
			c.Codec = CompressionCodec(v)
		case 5:
			v, e := r.i64()
			if e != nil {
				return c, e
			}
			c.NumValues = v
		case 6:
			v, e := r.i64()
			if e != nil {
				return c, e
			}
			c.TotalUncompressedSize = v
		case 7:
			v, e := r.i64()
			if e != nil {
				return c, e
			}
			c.TotalCompressedSize = v
		case 9:
			v, e := r.i64()
			if e != nil {
				return c, e
			}
			c.DataPageOffset = v
		case 10:
			v, e := r.i64()
			if e != nil {
				return c, e
			}
			c.IndexPageOffset = &v
		case 11:
			v, e := r.i64()
			if e != nil {
				return c, e
			}
			c.DictionaryPageOffset = &v
		case 12:
			v, e := r.statistics()
			if e != nil {
				return c, e
			}
			c.Statistics = &v
		default:
			if e := r.skip(f.typ, 1); e != nil {
				return c, e
			}
		}
	}
}
func (r *compactReader) statistics() (Statistics, error) {
	var s Statistics
	last := int16(0)
	for {
		f, nl, e := r.field(last)
		if e != nil {
			return s, e
		}
		last = nl
		if f.typ == ctStop {
			return s, nil
		}
		switch f.id {
		case 1:
			v, e := r.binary()
			if e != nil {
				return s, e
			}
			s.Max = bytes.Clone(v)
		case 2:
			v, e := r.binary()
			if e != nil {
				return s, e
			}
			s.Min = bytes.Clone(v)
		case 3:
			v, e := r.i64()
			if e != nil {
				return s, e
			}
			s.NullCount = &v
		case 4:
			v, e := r.i64()
			if e != nil {
				return s, e
			}
			s.DistinctCount = &v
		case 5:
			v, e := r.binary()
			if e != nil {
				return s, e
			}
			s.MaxValue = bytes.Clone(v)
		case 6:
			v, e := r.binary()
			if e != nil {
				return s, e
			}
			s.MinValue = bytes.Clone(v)
		case 7:
			v, e := r.boolValue(f)
			if e != nil {
				return s, e
			}
			s.IsMaxValueExact = &v
		case 8:
			v, e := r.boolValue(f)
			if e != nil {
				return s, e
			}
			s.IsMinValueExact = &v
		case 9:
			v, e := r.i64()
			if e != nil {
				return s, e
			}
			s.NaNCount = &v
		default:
			if e := r.skip(f.typ, 1); e != nil {
				return s, e
			}
		}
	}
}
func (r *compactReader) keyValueList() ([]KeyValue, error) {
	et, n, e := r.listHeader()
	if e != nil {
		return nil, e
	}
	if et != ctStruct {
		return nil, fmt.Errorf("parquet: keyvalue list type %d", et)
	}
	out := make([]KeyValue, n)
	for i := range out {
		v, e := r.keyValue()
		if e != nil {
			return nil, e
		}
		out[i] = v
	}
	return out, nil
}
func (r *compactReader) keyValue() (KeyValue, error) {
	var k KeyValue
	last := int16(0)
	for {
		f, nl, e := r.field(last)
		if e != nil {
			return k, e
		}
		last = nl
		if f.typ == ctStop {
			return k, nil
		}
		switch f.id {
		case 1:
			v, e := r.binary()
			if e != nil {
				return k, e
			}
			k.Key = string(v)
		case 2:
			v, e := r.binary()
			if e != nil {
				return k, e
			}
			x := string(v)
			k.Value = &x
		default:
			if e := r.skip(f.typ, 1); e != nil {
				return k, e
			}
		}
	}
}
func (r *compactReader) i32List() ([]int32, error) {
	et, n, e := r.listHeader()
	if e != nil {
		return nil, e
	}
	if et != ctI32 {
		return nil, fmt.Errorf("parquet: i32 list type %d", et)
	}
	out := make([]int32, n)
	for i := range out {
		v, e := r.i32()
		if e != nil {
			return nil, e
		}
		out[i] = v
	}
	return out, nil
}
func (r *compactReader) stringList() ([]string, error) {
	et, n, e := r.listHeader()
	if e != nil {
		return nil, e
	}
	if et != ctBinary {
		return nil, fmt.Errorf("parquet: string list type %d", et)
	}
	out := make([]string, n)
	for i := range out {
		v, e := r.binary()
		if e != nil {
			return nil, e
		}
		out[i] = string(v)
	}
	return out, nil
}

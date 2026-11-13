package parquet

type PhysicalType int32

const (
	TypeBoolean PhysicalType = iota
	TypeInt32
	TypeInt64
	TypeInt96
	TypeFloat
	TypeDouble
	TypeByteArray
	TypeFixedLenByteArray
)

type RepetitionType int32

const (
	Required RepetitionType = iota
	Optional
	Repeated
)

type Encoding int32

const (
	EncodingPlain Encoding = iota
	EncodingGroupVarInt
	EncodingPlainDictionary
	EncodingRLE
	EncodingBitPacked
	EncodingDeltaBinaryPacked
	EncodingDeltaLengthByteArray
	EncodingDeltaByteArray
	EncodingRLEDictionary
	EncodingByteStreamSplit
)

type CompressionCodec int32

const (
	CodecUncompressed CompressionCodec = iota
	CodecSnappy
	CodecGzip
	CodecLZO
	CodecBrotli
	CodecLZ4
	CodecZSTD
	CodecLZ4Raw
)

type ConvertedType int32

const (
	ConvertedUTF8 ConvertedType = iota
	ConvertedMap
	ConvertedMapKeyValue
	ConvertedList
	ConvertedEnum
	ConvertedDecimal
	ConvertedDate
	ConvertedTimeMillis
	ConvertedTimeMicros
	ConvertedTimestampMillis
	ConvertedTimestampMicros
	ConvertedUint8
	ConvertedUint16
	ConvertedUint32
	ConvertedUint64
	ConvertedInt8
	ConvertedInt16
	ConvertedInt32
	ConvertedInt64
	ConvertedJSON
	ConvertedBSON
	ConvertedInterval
)

type SchemaElement struct {
	Type        *PhysicalType
	TypeLength  *int32
	Repetition  *RepetitionType
	Name        string
	NumChildren *int32
	Converted   *ConvertedType
	Scale       *int32
	Precision   *int32
	FieldID     *int32
	Logical     *LogicalType
}

type LogicalType struct {
	Kind      LogicalKind
	Decimal   *DecimalLogical
	Timestamp *TimestampLogical
	Time      *TimeLogical
	Integer   *IntegerLogical
}

type LogicalKind uint8

const (
	LogicalNone LogicalKind = iota
	LogicalString
	LogicalMap
	LogicalList
	LogicalEnum
	LogicalDecimal
	LogicalDate
	LogicalTime
	LogicalTimestamp
	LogicalInteger
	LogicalUnknown
)

type DecimalLogical struct{ Scale, Precision int32 }
type TimestampLogical struct {
	IsAdjustedToUTC bool
	Unit            TimeUnit
}
type TimeLogical struct {
	IsAdjustedToUTC bool
	Unit            TimeUnit
}
type IntegerLogical struct {
	BitWidth int8
	IsSigned bool
}
type TimeUnit uint8

const (
	UnitMillis TimeUnit = iota
	UnitMicros
	UnitNanos
)

type Statistics struct {
	Max             []byte
	Min             []byte
	NullCount       *int64
	DistinctCount   *int64
	MaxValue        []byte
	MinValue        []byte
	IsMaxValueExact *bool
	IsMinValueExact *bool
	NaNCount        *int64
}

type ColumnMetaData struct {
	Type                  PhysicalType
	Encodings             []Encoding
	PathInSchema          []string
	Codec                 CompressionCodec
	NumValues             int64
	TotalUncompressedSize int64
	TotalCompressedSize   int64
	DataPageOffset        int64
	IndexPageOffset       *int64
	DictionaryPageOffset  *int64
	Statistics            *Statistics
}

type ColumnChunk struct {
	FilePath          string
	FileOffset        int64
	MetaData          *ColumnMetaData
	OffsetIndexOffset *int64
	OffsetIndexLength *int32
	ColumnIndexOffset *int64
	ColumnIndexLength *int32
}

type RowGroup struct {
	Columns             []ColumnChunk
	TotalByteSize       int64
	NumRows             int64
	FileOffset          *int64
	TotalCompressedSize *int64
	Ordinal             *int16
}

type KeyValue struct {
	Key   string
	Value *string
}

// ColumnOrder identifies the ordering of modern min_value/max_value bounds.
type ColumnOrder uint8

const (
	UndefinedColumnOrder ColumnOrder = iota
	TypeDefinedOrder
)

type FileMetaData struct {
	Version          int32
	Schema           []SchemaElement
	NumRows          int64
	RowGroups        []RowGroup
	KeyValueMetadata []KeyValue
	CreatedBy        string
	ColumnOrders     []ColumnOrder
}

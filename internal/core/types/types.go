package types

import "fmt"

type LogicalTypeID uint8

const (
	Invalid LogicalTypeID = iota
	Boolean
	TinyInt
	SmallInt
	Integer
	BigInt
	Float
	Double
	Decimal
	Varchar
	DateType
	TimestampType
	List
	Struct
	// HugeInt is appended to preserve the numeric IDs of the pre-ADR-0002 logical types.
	HugeInt
)

type PhysicalType uint8

const (
	PhysicalInvalid PhysicalType = iota
	PhysicalBool
	PhysicalInt8
	PhysicalInt16
	PhysicalInt32
	PhysicalInt64
	PhysicalFloat32
	PhysicalFloat64
	PhysicalInt128
	PhysicalString
	PhysicalList
	PhysicalStruct
)

type TimestampUnit uint8

const (
	TimestampMicros TimestampUnit = iota
	TimestampNanos
	TimestampMillis
)

type TimestampInfo struct {
	Unit          TimestampUnit
	AdjustedToUTC bool
}

type DecimalInfo struct {
	Width uint8
	Scale uint8
}
type StructField struct {
	Name string
	Type LogicalType
}
type LogicalType struct {
	// NullType marks a resolved SQLNULL result. Its physical integer storage is
	// always invalid; it is distinct from an untyped binder NULL placeholder.
	NullType bool
	ID        LogicalTypeID
	Decimal   DecimalInfo
	Child     *LogicalType
	Fields    []StructField
	Unsigned  bool
	Binary    bool
	Timestamp TimestampInfo
}

func NewDecimal(width, scale uint8) (LogicalType, error) {
	if width == 0 || width > 38 || scale > width {
		return LogicalType{}, fmt.Errorf("invalid DECIMAL(%d,%d)", width, scale)
	}
	return LogicalType{ID: Decimal, Decimal: DecimalInfo{Width: width, Scale: scale}}, nil
}
func NewList(child LogicalType) LogicalType { c := child; return LogicalType{ID: List, Child: &c} }
func NewStruct(fields ...StructField) LogicalType {
	return LogicalType{ID: Struct, Fields: append([]StructField(nil), fields...)}
}

func NewUnsigned(id LogicalTypeID) (LogicalType, error) {
	switch id {
	case TinyInt, SmallInt, Integer, BigInt:
		return LogicalType{ID: id, Unsigned: true}, nil
	default:
		return LogicalType{}, fmt.Errorf("unsigned modifier is invalid for logical type %d", id)
	}
}

func UTinyIntType() LogicalType  { return LogicalType{ID: TinyInt, Unsigned: true} }
func USmallIntType() LogicalType { return LogicalType{ID: SmallInt, Unsigned: true} }
func UIntegerType() LogicalType  { return LogicalType{ID: Integer, Unsigned: true} }
func UBigIntType() LogicalType   { return LogicalType{ID: BigInt, Unsigned: true} }
func BlobType() LogicalType      { return LogicalType{ID: Varchar, Binary: true} }
func TimestampNSType() LogicalType {
	return LogicalType{ID: TimestampType, Timestamp: TimestampInfo{Unit: TimestampNanos}}
}
func TimestampTZType() LogicalType {
	return LogicalType{ID: TimestampType, Timestamp: TimestampInfo{Unit: TimestampMicros, AdjustedToUTC: true}}
}
func NewTimestamp(unit TimestampUnit, adjustedToUTC bool) (LogicalType, error) {
	if unit > TimestampMillis {
		return LogicalType{}, fmt.Errorf("invalid timestamp unit %d", unit)
	}
	return LogicalType{ID: TimestampType, Timestamp: TimestampInfo{Unit: unit, AdjustedToUTC: adjustedToUTC}}, nil
}

func (t LogicalType) IsUnsignedInteger() bool {
	if !t.Unsigned {
		return false
	}
	switch t.ID {
	case TinyInt, SmallInt, Integer, BigInt:
		return true
	default:
		return false
	}
}
func (t LogicalType) IsBlob() bool { return t.ID == Varchar && t.Binary }
func (t LogicalType) IsTimestampNS() bool {
	return t.ID == TimestampType && t.Timestamp.Unit == TimestampNanos
}
func (t LogicalType) IsTimestampTZ() bool {
	return t.ID == TimestampType && t.Timestamp.AdjustedToUTC
}
func (t LogicalType) Same(other LogicalType) bool {
	if t.NullType != other.NullType || t.ID != other.ID || t.Decimal != other.Decimal || t.Unsigned != other.Unsigned || t.Binary != other.Binary || t.Timestamp != other.Timestamp {
		return false
	}
	if (t.Child == nil) != (other.Child == nil) || len(t.Fields) != len(other.Fields) {
		return false
	}
	if t.Child != nil && !t.Child.Same(*other.Child) {
		return false
	}
	for i := range t.Fields {
		if t.Fields[i].Name != other.Fields[i].Name || !t.Fields[i].Type.Same(other.Fields[i].Type) {
			return false
		}
	}
	return true
}

func (t LogicalType) Physical() PhysicalType {
	switch t.ID {
	case Boolean:
		return PhysicalBool
	case TinyInt:
		return PhysicalInt8
	case SmallInt:
		return PhysicalInt16
	case Integer, DateType:
		return PhysicalInt32
	case BigInt, TimestampType:
		return PhysicalInt64
	case HugeInt:
		return PhysicalInt128
	case Float:
		return PhysicalFloat32
	case Double:
		return PhysicalFloat64
	case Decimal:
		if t.Decimal.Width <= 18 {
			return PhysicalInt64
		}
		return PhysicalInt128
	case Varchar:
		return PhysicalString
	case List:
		return PhysicalList
	case Struct:
		return PhysicalStruct
	default:
		return PhysicalInvalid
	}
}
func (t LogicalType) FixedSize() (int, bool) {
	switch t.Physical() {
	case PhysicalBool, PhysicalInt8:
		return 1, true
	case PhysicalInt16:
		return 2, true
	case PhysicalInt32, PhysicalFloat32:
		return 4, true
	case PhysicalInt64, PhysicalFloat64:
		return 8, true
	case PhysicalInt128:
		return 16, true
	default:
		return 0, false
	}
}
func (t LogicalType) IsNumeric() bool {
	switch t.ID {
	case TinyInt, SmallInt, Integer, BigInt, HugeInt, Float, Double, Decimal:
		return true
	}
	return false
}

type Date int32
type Timestamp int64
type TimestampNS int64
type TimestampTZ int64
type Decimal64 int64
type TypeOperations struct{}

func (TypeOperations) Physical(t LogicalType) PhysicalType { return t.Physical() }
func (TypeOperations) FixedSize(t LogicalType) (int, bool) { return t.FixedSize() }
func (TypeOperations) IsNumeric(t LogicalType) bool        { return t.IsNumeric() }

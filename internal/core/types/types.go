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

type DecimalInfo struct {
	Width uint8
	Scale uint8
}
type StructField struct {
	Name string
	Type LogicalType
}
type LogicalType struct {
	ID      LogicalTypeID
	Decimal DecimalInfo
	Child   *LogicalType
	Fields  []StructField
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
type Decimal64 int64
type TypeOperations struct{}

func (TypeOperations) Physical(t LogicalType) PhysicalType { return t.Physical() }
func (TypeOperations) FixedSize(t LogicalType) (int, bool) { return t.FixedSize() }
func (TypeOperations) IsNumeric(t LogicalType) bool        { return t.IsNumeric() }

package gosql

import (
	"fmt"
	"math/big"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

// Type is a public logical-type description. It intentionally mirrors semantic
// type information, not internal physical vector layout.
type Type struct {
	ID     string
	Width  uint8
	Scale  uint8
	Child  *Type
	Fields []StructField
}

// StructField describes one STRUCT child in the public type tree.
type StructField struct {
	Name string
	Type Type
}

// Int128 is the public lossless representation for HUGEINT and wide DECIMAL
// storage. Lo/Hi use the same two's-complement limbs as the native ABI without
// exposing an internal package type.
type Int128 struct {
	Lo uint64
	Hi int64
}

func (v Int128) String() string {
	unsigned := new(big.Int).SetUint64(uint64(v.Hi))
	unsigned.Lsh(unsigned, 64)
	unsigned.Or(unsigned, new(big.Int).SetUint64(v.Lo))
	if v.Hi < 0 {
		unsigned.Sub(unsigned, new(big.Int).Lsh(big.NewInt(1), 128))
	}
	return unsigned.String()
}

// Chunk is a borrowed view of one native DataChunk. A Chunk received by
// QueryChunks must not be retained after the callback returns.
type Chunk struct{ chunk *vector.DataChunk }

func (c *Chunk) RowCount() int {
	if c == nil || c.chunk == nil {
		return 0
	}
	return c.chunk.Cardinality
}
func (c *Chunk) ColumnCount() int {
	if c == nil || c.chunk == nil {
		return 0
	}
	return len(c.chunk.Columns)
}
func (c *Chunk) Type(column int) (Type, error) {
	v, err := c.column(column)
	if err != nil {
		return Type{}, err
	}
	return publicType(v.Type), nil
}
func (c *Chunk) IsNull(row, column int) (bool, error) {
	v, err := c.cell(row, column)
	if err != nil {
		return false, err
	}
	return !v.IsValid(row), nil
}

// Int64 returns exact values for signed integer/date/timestamp and narrow
// DECIMAL representations. Unsigned integers must use Uint64.
func (c *Chunk) Int64(row, column int) (int64, bool, error) {
	v, err := c.cell(row, column)
	if err != nil {
		return 0, false, err
	}
	if v.Type.IsUnsignedInteger() {
		return 0, false, fmt.Errorf("gosql: column %d type %s requires Uint64", column, logicalTypeNameOf(v.Type))
	}
	switch v.Type.ID {
	case types.TinyInt:
		x, ok := v.Int8(row)
		return int64(x), ok, nil
	case types.SmallInt:
		x, ok := v.Int16(row)
		return int64(x), ok, nil
	case types.Integer, types.DateType:
		x, ok := v.Int32(row)
		return int64(x), ok, nil
	case types.BigInt, types.TimestampType:
		x, ok := v.Int64(row)
		return x, ok, nil
	case types.Decimal:
		if v.Type.Decimal.Width <= 18 {
			x, ok := v.Int64(row)
			return x, ok, nil
		}
		return 0, false, fmt.Errorf("gosql: DECIMAL(%d,%d) requires Int128", v.Type.Decimal.Width, v.Type.Decimal.Scale)
	default:
		return 0, false, fmt.Errorf("gosql: column %d type %s is not Int64-compatible", column, logicalTypeNameOf(v.Type))
	}
}

// Uint64 returns exact values for UTINYINT, USMALLINT, UINTEGER and UBIGINT.
func (c *Chunk) Uint64(row, column int) (uint64, bool, error) {
	v, err := c.cell(row, column)
	if err != nil {
		return 0, false, err
	}
	if !v.Type.IsUnsignedInteger() {
		return 0, false, fmt.Errorf("gosql: column %d type %s is not unsigned", column, logicalTypeNameOf(v.Type))
	}
	switch v.Type.ID {
	case types.TinyInt:
		x, ok := v.Uint8(row)
		return uint64(x), ok, nil
	case types.SmallInt:
		x, ok := v.Uint16(row)
		return uint64(x), ok, nil
	case types.Integer:
		x, ok := v.Uint32(row)
		return uint64(x), ok, nil
	case types.BigInt:
		x, ok := v.Uint64(row)
		return x, ok, nil
	default:
		return 0, false, fmt.Errorf("gosql: column %d type %s is not unsigned", column, logicalTypeNameOf(v.Type))
	}
}

func (c *Chunk) Float64(row, column int) (float64, bool, error) {
	v, err := c.cell(row, column)
	if err != nil {
		return 0, false, err
	}
	switch v.Type.ID {
	case types.Float:
		x, ok := v.Float32(row)
		return float64(x), ok, nil
	case types.Double:
		x, ok := v.Float64(row)
		return x, ok, nil
	default:
		return 0, false, fmt.Errorf("gosql: column %d type %s is not floating-point", column, logicalTypeNameOf(v.Type))
	}
}

func (c *Chunk) Bool(row, column int) (bool, bool, error) {
	v, err := c.cell(row, column)
	if err != nil {
		return false, false, err
	}
	if v.Type.ID != types.Boolean {
		return false, false, fmt.Errorf("gosql: column %d type %s is not BOOLEAN", column, logicalTypeNameOf(v.Type))
	}
	x, ok := v.Bool(row)
	return x, ok, nil
}

func (c *Chunk) String(row, column int) (string, bool, error) {
	v, err := c.cell(row, column)
	if err != nil {
		return "", false, err
	}
	if v.Type.ID != types.Varchar || v.Type.Binary {
		return "", false, fmt.Errorf("gosql: column %d type %s is not VARCHAR", column, logicalTypeNameOf(v.Type))
	}
	x, ok := v.String(row)
	return x, ok, nil
}

// Bytes returns a copy of a BLOB value, so callers may retain it after the
// borrowed Chunk callback returns.
func (c *Chunk) Bytes(row, column int) ([]byte, bool, error) {
	v, err := c.cell(row, column)
	if err != nil {
		return nil, false, err
	}
	if !v.Type.IsBlob() {
		return nil, false, fmt.Errorf("gosql: column %d type %s is not BLOB", column, logicalTypeNameOf(v.Type))
	}
	x, ok := v.Bytes(row)
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), x...), true, nil
}

func (c *Chunk) Int128(row, column int) (Int128, bool, error) {
	v, err := c.cell(row, column)
	if err != nil {
		return Int128{}, false, err
	}
	if v.Type.ID != types.HugeInt && !(v.Type.ID == types.Decimal && v.Type.Decimal.Width > 18) {
		return Int128{}, false, fmt.Errorf("gosql: column %d type %s is not Int128-compatible", column, logicalTypeNameOf(v.Type))
	}
	x, ok := v.Int128(row)
	return Int128{Lo: x.Lo, Hi: x.Hi}, ok, nil
}

func (c *Chunk) column(column int) (*vector.Vector, error) {
	if c == nil || c.chunk == nil {
		return nil, fmt.Errorf("gosql: nil Chunk")
	}
	if column < 0 || column >= len(c.chunk.Columns) {
		return nil, fmt.Errorf("gosql: column index %d out of range", column)
	}
	return c.chunk.Columns[column], nil
}
func (c *Chunk) cell(row, column int) (*vector.Vector, error) {
	v, err := c.column(column)
	if err != nil {
		return nil, err
	}
	if row < 0 || row >= c.chunk.Cardinality {
		return nil, fmt.Errorf("gosql: row index %d out of range", row)
	}
	return v, nil
}

func publicType(t types.LogicalType) Type {
	out := Type{ID: logicalTypeNameOf(t), Width: t.Decimal.Width, Scale: t.Decimal.Scale}
	if t.Child != nil {
		child := publicType(*t.Child)
		out.Child = &child
	}
	if len(t.Fields) > 0 {
		out.Fields = make([]StructField, len(t.Fields))
		for i, field := range t.Fields {
			out.Fields[i] = StructField{Name: field.Name, Type: publicType(field.Type)}
		}
	}
	return out
}

func logicalTypeNameOf(t types.LogicalType) string {
	if t.IsUnsignedInteger() {
		switch t.ID {
		case types.TinyInt:
			return "UTINYINT"
		case types.SmallInt:
			return "USMALLINT"
		case types.Integer:
			return "UINTEGER"
		case types.BigInt:
			return "UBIGINT"
		}
	}
	if t.IsBlob() {
		return "BLOB"
	}
	if t.ID == types.TimestampType {
		if t.Timestamp.AdjustedToUTC {
			return "TIMESTAMPTZ"
		}
		if t.Timestamp.Unit == types.TimestampNanos {
			return "TIMESTAMP_NS"
		}
	}
	return logicalTypeName(t.ID)
}

func logicalTypeName(id types.LogicalTypeID) string {
	switch id {
	case types.Boolean:
		return "BOOLEAN"
	case types.TinyInt:
		return "TINYINT"
	case types.SmallInt:
		return "SMALLINT"
	case types.Integer:
		return "INTEGER"
	case types.BigInt:
		return "BIGINT"
	case types.HugeInt:
		return "HUGEINT"
	case types.Float:
		return "FLOAT"
	case types.Double:
		return "DOUBLE"
	case types.Decimal:
		return "DECIMAL"
	case types.Varchar:
		return "VARCHAR"
	case types.DateType:
		return "DATE"
	case types.TimestampType:
		return "TIMESTAMP"
	case types.List:
		return "LIST"
	case types.Struct:
		return "STRUCT"
	default:
		return "INVALID"
	}
}

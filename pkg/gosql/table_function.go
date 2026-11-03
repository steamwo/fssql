package gosql

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

const (
	maxPublicTableFunctions = 27
	defaultBatchTargetBytes = 1 << 20
)

// TableFunction registers one fixed-schema zero-argument table function.
// Open is invoked lazily on first execution and at most once per query.
type TableFunction struct {
	Name    string
	Columns []Column
	Open    func(context.Context) (BatchReader, error)
}

// BatchReader fills engine-owned managed batches. Returning io.EOF with n == 0
// ends the source; returning n > 0 with io.EOF publishes the final batch.
type BatchReader interface {
	Next(context.Context, BatchWriter) (int, error)
	Close() error
}

// BatchWriter is valid only during BatchReader.Next. Setters copy values into
// engine-owned storage and never retain caller byte/string buffers.
type BatchWriter interface {
	Capacity() int
	SetNull(row, column int) error
	SetBool(row, column int, value bool) error
	SetInt64(row, column int, value int64) error
	SetUint64(row, column int, value uint64) error
	SetFloat64(row, column int, value float64) error
	SetDecimal(row, column int, value string) error
	SetString(row, column int, value string) error
	SetBytes(row, column int, value []byte) error
	SetDate(row, column int, value time.Time) error
	SetTimestamp(row, column int, value time.Time) error
	SetTimestampNS(row, column int, value int64) error
}

type registeredTableFunction struct {
	id      function.ID
	name    string
	columns []binder.RelationColumn
	open    func(context.Context) (BatchReader, error)
}

func registerTableFunctions(base *function.Catalog, definitions []TableFunction) (*function.Catalog, []registeredTableFunction, error) {
	if base == nil {
		base = function.NewBuiltinCatalog()
	}
	if len(definitions) > maxPublicTableFunctions {
		return nil, nil, fmt.Errorf("gosql: too many table functions: %d > %d", len(definitions), maxPublicTableFunctions)
	}
	catalog := base
	registered := make([]registeredTableFunction, 0, len(definitions))
	seen := make(map[string]struct{}, len(definitions))
	for i, definition := range definitions {
		name := strings.TrimSpace(definition.Name)
		if !validFunctionIdentifier(name) {
			return nil, nil, fmt.Errorf("gosql: invalid table function name %q", definition.Name)
		}
		key := strings.ToLower(name)
		if _, ok := seen[key]; ok {
			return nil, nil, fmt.Errorf("gosql: duplicate table function %q", name)
		}
		seen[key] = struct{}{}
		if definition.Open == nil {
			return nil, nil, fmt.Errorf("gosql: table function %s has nil Open", name)
		}
		columns, err := internalColumns(definition.Columns)
		if err != nil {
			return nil, nil, fmt.Errorf("gosql: table function %s: %w", name, err)
		}
		id := function.ID(int(function.GlobID) + 1 + i)
		next, err := catalog.With(function.Definition{ID: id, Name: name, Kind: function.Table})
		if err != nil {
			return nil, nil, fmt.Errorf("gosql: register table function %s: %w", name, err)
		}
		catalog = next
		registered = append(registered, registeredTableFunction{id: id, name: name, columns: columns, open: definition.Open})
	}
	return catalog, registered, nil
}

func validFunctionIdentifier(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_' || (i > 0 && r >= '0' && r <= '9') {
			continue
		}
		return false
	}
	return true
}

func internalColumns(columns []Column) ([]binder.RelationColumn, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("fixed schema requires at least one column")
	}
	out := make([]binder.RelationColumn, len(columns))
	seen := make(map[string]struct{}, len(columns))
	for i, column := range columns {
		name := strings.TrimSpace(column.Name)
		if name == "" {
			return nil, fmt.Errorf("column %d has empty name", i)
		}
		key := strings.ToLower(name)
		if _, ok := seen[key]; ok {
			return nil, fmt.Errorf("duplicate column name %q", name)
		}
		seen[key] = struct{}{}
		typ, err := internalTableFunctionType(column.Type)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", name, err)
		}
		out[i] = binder.RelationColumn{Name: name, Type: typ, Nullable: column.Nullable}
	}
	return out, nil
}

func internalTableFunctionType(public Type) (types.LogicalType, error) {
	id := strings.ToUpper(strings.TrimSpace(public.ID))
	switch id {
	case "BOOLEAN":
		return types.LogicalType{ID: types.Boolean}, nil
	case "TINYINT":
		return types.LogicalType{ID: types.TinyInt}, nil
	case "SMALLINT":
		return types.LogicalType{ID: types.SmallInt}, nil
	case "INTEGER":
		return types.LogicalType{ID: types.Integer}, nil
	case "BIGINT":
		return types.LogicalType{ID: types.BigInt}, nil
	case "UTINYINT":
		return types.UTinyIntType(), nil
	case "USMALLINT":
		return types.USmallIntType(), nil
	case "UINTEGER":
		return types.UIntegerType(), nil
	case "UBIGINT":
		return types.UBigIntType(), nil
	case "FLOAT":
		return types.LogicalType{ID: types.Float}, nil
	case "DOUBLE":
		return types.LogicalType{ID: types.Double}, nil
	case "DECIMAL":
		return types.NewDecimal(public.Width, public.Scale)
	case "VARCHAR":
		return types.LogicalType{ID: types.Varchar}, nil
	case "BLOB":
		return types.BlobType(), nil
	case "DATE":
		return types.LogicalType{ID: types.DateType}, nil
	case "TIMESTAMP":
		return types.LogicalType{ID: types.TimestampType, Timestamp: types.TimestampInfo{Unit: types.TimestampMicros}}, nil
	case "TIMESTAMP_NS":
		return types.TimestampNSType(), nil
	case "TIMESTAMPTZ":
		return types.TimestampTZType(), nil
	default:
		return types.LogicalType{}, fmt.Errorf("unsupported typed-ingestion type %q", public.ID)
	}
}

type managedBatchWriter struct {
	chunk    *vector.DataChunk
	columns  []binder.RelationColumn
	capacity int
}

func newManagedBatchWriter(scope *memory.Scope, bm *memory.BufferManager, columns []binder.RelationColumn) (*managedBatchWriter, error) {
	capacity := managedBatchCapacity(columns)
	vectors := make([]*vector.Vector, 0, len(columns))
	for _, column := range columns {
		v, err := vector.NewFlat(scope, bm, column.Type, capacity)
		if err != nil {
			for _, owned := range vectors {
				owned.Release()
			}
			return nil, err
		}
		for row := 0; row < capacity; row++ {
			v.SetNull(row)
		}
		vectors = append(vectors, v)
	}
	chunk, err := vector.NewDataChunk(vectors...)
	if err != nil {
		for _, owned := range vectors {
			owned.Release()
		}
		return nil, err
	}
	return &managedBatchWriter{chunk: chunk, columns: columns, capacity: capacity}, nil
}

func managedBatchCapacity(columns []binder.RelationColumn) int {
	rowBytes := 0
	for _, column := range columns {
		if width, ok := column.Type.FixedSize(); ok {
			rowBytes += width
			continue
		}
		// VARCHAR/BLOB vectors keep a 24-byte StringRef per row; payload bytes
		// remain separately managed and may make one large value dominate a batch.
		if column.Type.ID == types.Varchar {
			rowBytes += 24
		}
	}
	if rowBytes <= 0 {
		return vector.DefaultVectorSize
	}
	capacity := defaultBatchTargetBytes / rowBytes
	if capacity < 1 {
		capacity = 1
	}
	if capacity > vector.DefaultVectorSize {
		capacity = vector.DefaultVectorSize
	}
	return capacity
}

func (w *managedBatchWriter) Capacity() int { return w.capacity }

func (w *managedBatchWriter) cell(row, column int) (*vector.Vector, binder.RelationColumn, error) {
	if w == nil || w.chunk == nil {
		return nil, binder.RelationColumn{}, fmt.Errorf("gosql: BatchWriter is closed")
	}
	if row < 0 || row >= w.capacity {
		return nil, binder.RelationColumn{}, fmt.Errorf("gosql: batch row %d out of range [0,%d)", row, w.capacity)
	}
	if column < 0 || column >= len(w.columns) {
		return nil, binder.RelationColumn{}, fmt.Errorf("gosql: batch column %d out of range", column)
	}
	return w.chunk.Columns[column], w.columns[column], nil
}

func (w *managedBatchWriter) SetNull(row, column int) error {
	v, meta, err := w.cell(row, column)
	if err != nil {
		return err
	}
	if !meta.Nullable {
		return fmt.Errorf("gosql: column %s is not nullable", meta.Name)
	}
	v.SetNull(row)
	return nil
}

func (w *managedBatchWriter) SetBool(row, column int, value bool) error {
	v, meta, err := w.cell(row, column)
	if err != nil {
		return err
	}
	if meta.Type.ID != types.Boolean {
		return setterTypeError(meta, "BOOLEAN")
	}
	v.SetBool(row, value)
	return nil
}

func (w *managedBatchWriter) SetInt64(row, column int, value int64) error {
	v, meta, err := w.cell(row, column)
	if err != nil {
		return err
	}
	if meta.Type.Unsigned {
		return setterTypeError(meta, "signed integer")
	}
	switch meta.Type.ID {
	case types.TinyInt:
		if value < -128 || value > 127 {
			return fmt.Errorf("gosql: %d overflows TINYINT", value)
		}
		v.SetInt8(row, int8(value))
	case types.SmallInt:
		if value < -32768 || value > 32767 {
			return fmt.Errorf("gosql: %d overflows SMALLINT", value)
		}
		v.SetInt16(row, int16(value))
	case types.Integer:
		if value < -2147483648 || value > 2147483647 {
			return fmt.Errorf("gosql: %d overflows INTEGER", value)
		}
		v.SetInt32(row, int32(value))
	case types.BigInt:
		v.SetInt64(row, value)
	default:
		return setterTypeError(meta, "signed integer")
	}
	return nil
}

func (w *managedBatchWriter) SetUint64(row, column int, value uint64) error {
	v, meta, err := w.cell(row, column)
	if err != nil {
		return err
	}
	if !meta.Type.IsUnsignedInteger() {
		return setterTypeError(meta, "unsigned integer")
	}
	switch meta.Type.ID {
	case types.TinyInt:
		if value > 255 {
			return fmt.Errorf("gosql: %d overflows UTINYINT", value)
		}
		v.SetUint8(row, uint8(value))
	case types.SmallInt:
		if value > 65535 {
			return fmt.Errorf("gosql: %d overflows USMALLINT", value)
		}
		v.SetUint16(row, uint16(value))
	case types.Integer:
		if value > 4294967295 {
			return fmt.Errorf("gosql: %d overflows UINTEGER", value)
		}
		v.SetUint32(row, uint32(value))
	case types.BigInt:
		v.SetUint64(row, value)
	default:
		return setterTypeError(meta, "unsigned integer")
	}
	return nil
}

func (w *managedBatchWriter) SetFloat64(row, column int, value float64) error {
	v, meta, err := w.cell(row, column)
	if err != nil {
		return err
	}
	switch meta.Type.ID {
	case types.Float:
		v.SetFloat32(row, float32(value))
	case types.Double:
		v.SetFloat64(row, value)
	default:
		return setterTypeError(meta, "floating-point")
	}
	return nil
}

func (w *managedBatchWriter) SetDecimal(row, column int, value string) error {
	v, meta, err := w.cell(row, column)
	if err != nil {
		return err
	}
	if meta.Type.ID != types.Decimal {
		return setterTypeError(meta, "DECIMAL")
	}
	unscaled, err := parseExactDecimal(value, meta.Type.Decimal.Width, meta.Type.Decimal.Scale)
	if err != nil {
		return fmt.Errorf("gosql: column %s: %w", meta.Name, err)
	}
	if meta.Type.Decimal.Width <= 18 {
		if !unscaled.IsInt64() {
			return fmt.Errorf("gosql: DECIMAL(%d,%d) is outside int64 storage", meta.Type.Decimal.Width, meta.Type.Decimal.Scale)
		}
		v.SetInt64(row, unscaled.Int64())
		return nil
	}
	wide, ok := types.Int128FromBig(unscaled)
	if !ok {
		return fmt.Errorf("gosql: DECIMAL(%d,%d) exceeds Int128 storage", meta.Type.Decimal.Width, meta.Type.Decimal.Scale)
	}
	v.SetInt128(row, wide)
	return nil
}

func (w *managedBatchWriter) SetString(row, column int, value string) error {
	v, meta, err := w.cell(row, column)
	if err != nil {
		return err
	}
	if meta.Type.ID != types.Varchar || meta.Type.Binary {
		return setterTypeError(meta, "VARCHAR")
	}
	return v.SetString(row, value)
}

func (w *managedBatchWriter) SetBytes(row, column int, value []byte) error {
	v, meta, err := w.cell(row, column)
	if err != nil {
		return err
	}
	if !meta.Type.IsBlob() {
		return setterTypeError(meta, "BLOB")
	}
	return v.SetBytes(row, value)
}

func (w *managedBatchWriter) SetDate(row, column int, value time.Time) error {
	v, meta, err := w.cell(row, column)
	if err != nil {
		return err
	}
	if meta.Type.ID != types.DateType {
		return setterTypeError(meta, "DATE")
	}
	y, m, d := value.Date()
	midnight := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	days := midnight.Unix() / 86400
	if days < -2147483648 || days > 2147483647 {
		return fmt.Errorf("gosql: DATE out of int32 range")
	}
	v.SetInt32(row, int32(days))
	return nil
}

func (w *managedBatchWriter) SetTimestamp(row, column int, value time.Time) error {
	v, meta, err := w.cell(row, column)
	if err != nil {
		return err
	}
	if meta.Type.ID != types.TimestampType {
		return setterTypeError(meta, "TIMESTAMP")
	}
	if meta.Type.Timestamp.Unit == types.TimestampNanos {
		v.SetInt64(row, value.UnixNano())
	} else {
		v.SetInt64(row, value.UnixMicro())
	}
	return nil
}

func (w *managedBatchWriter) SetTimestampNS(row, column int, value int64) error {
	v, meta, err := w.cell(row, column)
	if err != nil {
		return err
	}
	if !meta.Type.IsTimestampNS() {
		return setterTypeError(meta, "TIMESTAMP_NS")
	}
	v.SetInt64(row, value)
	return nil
}

func (w *managedBatchWriter) finish(rows int) (*vector.DataChunk, error) {
	if w == nil || w.chunk == nil {
		return nil, fmt.Errorf("gosql: BatchWriter is closed")
	}
	if rows < 0 || rows > w.capacity {
		w.chunk.Release()
		w.chunk = nil
		return nil, fmt.Errorf("gosql: reader returned invalid batch row count %d", rows)
	}
	for column, meta := range w.columns {
		if meta.Nullable {
			continue
		}
		for row := 0; row < rows; row++ {
			if !w.chunk.Columns[column].IsValid(row) {
				w.chunk.Release()
				w.chunk = nil
				return nil, fmt.Errorf("gosql: non-nullable column %s row %d was not written", meta.Name, row)
			}
		}
	}
	w.chunk.Cardinality = rows
	chunk := w.chunk
	w.chunk = nil
	return chunk, nil
}

func (w *managedBatchWriter) abort() {
	if w != nil && w.chunk != nil {
		w.chunk.Release()
		w.chunk = nil
	}
}

func setterTypeError(meta binder.RelationColumn, want string) error {
	return fmt.Errorf("gosql: column %s has type %s; setter requires %s", meta.Name, logicalTypeNameOf(meta.Type), want)
}

func parseExactDecimal(input string, width, scale uint8) (*big.Int, error) {
	text := strings.TrimSpace(input)
	if text == "" {
		return nil, fmt.Errorf("empty DECIMAL value")
	}
	negative := false
	if text[0] == '+' || text[0] == '-' {
		negative = text[0] == '-'
		text = text[1:]
	}
	if text == "" || strings.Count(text, ".") > 1 {
		return nil, fmt.Errorf("invalid DECIMAL %q", input)
	}
	parts := strings.SplitN(text, ".", 2)
	whole := parts[0]
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	if whole == "" {
		whole = "0"
	}
	if !decimalDigits(whole) || (fraction != "" && !decimalDigits(fraction)) {
		return nil, fmt.Errorf("invalid DECIMAL %q", input)
	}
	for len(fraction) > int(scale) {
		if fraction[len(fraction)-1] != '0' {
			return nil, fmt.Errorf("DECIMAL %q cannot be represented exactly at scale %d", input, scale)
		}
		fraction = fraction[:len(fraction)-1]
	}
	for len(fraction) < int(scale) {
		fraction += "0"
	}
	digits := strings.TrimLeft(whole+fraction, "0")
	if digits == "" {
		digits = "0"
	}
	if len(digits) > int(width) {
		return nil, fmt.Errorf("DECIMAL %q exceeds precision %d", input, width)
	}
	value := new(big.Int)
	if _, ok := value.SetString(digits, 10); !ok {
		return nil, fmt.Errorf("invalid DECIMAL %q", input)
	}
	if negative && value.Sign() != 0 {
		value.Neg(value)
	}
	return value, nil
}

func decimalDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

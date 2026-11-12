package gosql

import (
	"fmt"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

// Value owns one lossless cell. Data is a scalar native value, []Value for LIST,
// or []FieldValue for STRUCT. DECIMAL stores its exact unscaled integer and Type
// carries the scale. NULL has Valid=false and Data=nil. Values may outlive Chunk.
type Value struct {
	Type  Type
	Valid bool
	Data  any
}
type FieldValue struct {
	Name  string
	Value Value
}

func (c *Chunk) Value(row, column int) (Value, error) {
	v, err := c.cell(row, column)
	if err != nil {
		return Value{}, err
	}
	return ownedValue(v, row)
}
func (c *Chunk) List(row, column int) ([]Value, bool, error) {
	v, err := c.Value(row, column)
	if err != nil {
		return nil, false, err
	}
	if v.Type.ID != "LIST" {
		return nil, false, fmt.Errorf("column is not LIST")
	}
	if !v.Valid {
		return nil, false, nil
	}
	return v.Data.([]Value), true, nil
}
func (c *Chunk) Struct(row, column int) ([]FieldValue, bool, error) {
	v, err := c.Value(row, column)
	if err != nil {
		return nil, false, err
	}
	if v.Type.ID != "STRUCT" {
		return nil, false, fmt.Errorf("column is not STRUCT")
	}
	if !v.Valid {
		return nil, false, nil
	}
	return v.Data.([]FieldValue), true, nil
}

func ownedValue(v *vector.Vector, row int) (Value, error) {
	out := Value{Type: publicType(v.Type), Valid: v.IsValid(row)}
	if !out.Valid {
		return out, nil
	}
	base, index := v.NestedRow(row)
	if v.Type.ID == types.List {
		entry, _ := v.ListEntry(row)
		if len(base.Children()) != 1 {
			return Value{}, fmt.Errorf("invalid LIST child")
		}
		items := make([]Value, int(entry.Length))
		for i := range items {
			item, err := ownedValue(base.Children()[0], int(entry.Offset)+i)
			if err != nil {
				return Value{}, err
			}
			items[i] = item
		}
		out.Data = items
		return out, nil
	}
	if v.Type.ID == types.Struct {
		if len(base.Children()) != len(v.Type.Fields) {
			return Value{}, fmt.Errorf("invalid STRUCT children")
		}
		fields := make([]FieldValue, len(v.Type.Fields))
		for i, field := range v.Type.Fields {
			item, err := ownedValue(base.Children()[i], index)
			if err != nil {
				return Value{}, err
			}
			fields[i] = FieldValue{Name: field.Name, Value: item}
		}
		out.Data = fields
		return out, nil
	}
	view := &Chunk{chunk: &vector.DataChunk{Columns: []*vector.Vector{v}, Cardinality: v.Count()}}
	var err error
	switch {
	case v.Type.IsUnsignedInteger():
		out.Data, _, err = view.Uint64(row, 0)
	case v.Type.ID == types.Boolean:
		out.Data, _, err = view.Bool(row, 0)
	case v.Type.ID == types.Float || v.Type.ID == types.Double:
		out.Data, _, err = view.Float64(row, 0)
	case v.Type.ID == types.HugeInt || (v.Type.ID == types.Decimal && v.Type.Decimal.Width > 18):
		out.Data, _, err = view.Int128(row, 0)
	case v.Type.ID == types.Varchar:
		if v.Type.Binary {
			out.Data, _, err = view.Bytes(row, 0)
		} else {
			out.Data, _, err = view.String(row, 0)
		}
	default:
		out.Data, _, err = view.Int64(row, 0)
	}
	return out, err
}

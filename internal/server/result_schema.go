package server

import (
	"fmt"

	"github.com/steamwo/fssql/internal/core/types"
)

// ResultColumn 是 ResultCursor 对 consumer 暴露的 immutable column metadata。
// LogicalType 仍是 internal control metadata；public API 必须复制为自身类型，不能把 internal 类型泄漏到 pkg/gosql ABI。
type ResultColumn struct {
	Name     string            // Name 是 Binder/Planner 已确定的结果列名。
	Type     types.LogicalType // Type 是结果列 logical type 的深拷贝。
	Nullable bool              // Nullable 表示结果可能为 SQL NULL。
}

// SetColumns 在第一个结果 chunk Push 前冻结 cursor schema；重复设置只允许完全相同 schema。
func (c *ResultCursor) SetColumns(columns []ResultColumn) error {
	if c == nil {
		return fmt.Errorf("server: nil result cursor")
	}
	cloned := cloneResultColumns(columns)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.schemaFrozen || c.count != 0 || c.inflightCount != 0 || c.finished || c.closed {
		if resultColumnsEqual(c.columns, cloned) {
			return nil
		}
		return fmt.Errorf("server: result cursor schema is already frozen")
	}
	c.columns = cloned
	c.schemaFrozen = true
	return nil
}

// Columns 返回 schema 深拷贝；consumer 修改返回值不会影响 ResultCursor 或其他调用方。
func (c *ResultCursor) Columns() []ResultColumn {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return cloneResultColumns(c.columns)
}

func cloneResultColumns(columns []ResultColumn) []ResultColumn {
	out := make([]ResultColumn, len(columns))
	for i, column := range columns {
		out[i] = column
		out[i].Type = cloneResultLogicalType(column.Type)
	}
	return out
}

func cloneResultLogicalType(value types.LogicalType) types.LogicalType {
	out := value
	if value.Child != nil {
		child := cloneResultLogicalType(*value.Child)
		out.Child = &child
	}
	if value.Fields != nil {
		out.Fields = make([]types.StructField, len(value.Fields))
		for i, field := range value.Fields {
			out.Fields[i] = types.StructField{Name: field.Name, Type: cloneResultLogicalType(field.Type)}
		}
	}
	return out
}

func resultColumnsEqual(left, right []ResultColumn) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].Name != right[i].Name || left[i].Nullable != right[i].Nullable || !resultLogicalTypeEqual(left[i].Type, right[i].Type) {
			return false
		}
	}
	return true
}

func resultLogicalTypeEqual(left, right types.LogicalType) bool {
	if left.ID != right.ID || left.Decimal != right.Decimal || (left.Child == nil) != (right.Child == nil) || len(left.Fields) != len(right.Fields) {
		return false
	}
	if left.Child != nil && !resultLogicalTypeEqual(*left.Child, *right.Child) {
		return false
	}
	for i := range left.Fields {
		if left.Fields[i].Name != right.Fields[i].Name || !resultLogicalTypeEqual(left.Fields[i].Type, right.Fields[i].Type) {
			return false
		}
	}
	return true
}

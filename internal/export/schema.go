package export

import (
	"fmt"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

// Column 描述导出结果的一列；名称来自绑定后查询结果，类型来自核心 LogicalType。
type Column struct {
	// Name 是 CSV header/JSON object key 使用的稳定列名。
	Name string
	// Type 是写入时必须与 DataChunk 对应列一致的逻辑类型。
	Type types.LogicalType
}

// validateChunkSchema 防止 writer 在 schema 漂移时产生部分错误文件。
func validateChunkSchema(schema []Column, chunk *vector.DataChunk) error {
	if chunk == nil {
		return fmt.Errorf("export: nil data chunk")
	}
	if len(schema) != len(chunk.Columns) {
		return fmt.Errorf("export: schema columns=%d chunk columns=%d", len(schema), len(chunk.Columns))
	}
	if chunk.Cardinality < 0 || chunk.Cardinality > vector.DefaultVectorSize {
		return fmt.Errorf("export: invalid chunk cardinality %d", chunk.Cardinality)
	}
	for i := range schema {
		if chunk.Columns[i] == nil {
			return fmt.Errorf("export: nil vector at column %d", i)
		}
		if chunk.Columns[i].Count() != chunk.Cardinality {
			return fmt.Errorf("export: column %d cardinality=%d chunk=%d", i, chunk.Columns[i].Count(), chunk.Cardinality)
		}
		if !sameLogicalType(schema[i].Type, chunk.Columns[i].Type) {
			return fmt.Errorf("export: column %d type mismatch schema=%d chunk=%d", i, schema[i].Type.ID, chunk.Columns[i].Type.ID)
		}
	}
	return nil
}

// sameLogicalType 比较写出所需的 LogicalType identity，避免 DECIMAL/LIST/STRUCT 只按 physical type 混淆。
func sameLogicalType(a, b types.LogicalType) bool {
	if a.ID != b.ID || a.Decimal != b.Decimal {
		return false
	}
	if a.ID == types.List {
		if a.Child == nil || b.Child == nil {
			return a.Child == nil && b.Child == nil
		}
		return sameLogicalType(*a.Child, *b.Child)
	}
	if a.ID == types.Struct {
		if len(a.Fields) != len(b.Fields) {
			return false
		}
		for i := range a.Fields {
			if a.Fields[i].Name != b.Fields[i].Name || !sameLogicalType(a.Fields[i].Type, b.Fields[i].Type) {
				return false
			}
		}
	}
	return true
}

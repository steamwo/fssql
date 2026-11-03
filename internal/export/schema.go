package export

import (
	"fmt"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

// Column 描述导出结果的一列；名称来自绑定后查询结果，类型来自核心 LogicalType。
type Column struct {
	Name string
	Type types.LogicalType
}

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

// sameLogicalType includes all semantic modifiers (unsigned, binary, timestamp
// unit/UTC adjustment) in addition to nested and DECIMAL identity.
func sameLogicalType(a, b types.LogicalType) bool { return a.Same(b) }

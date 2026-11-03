package json

import "github.com/steamwo/fssql/internal/core/types"

// A selected LIST needs a count pass for exact managed child allocation.
// Scalar projections can validate and decode each JSON record in one pass.
func projectedList(typ types.LogicalType, projection ProjectedPathSet, path []string) bool {
	if !projection.Relevant(path) {
		return false
	}
	if typ.ID == types.List {
		return true
	}
	if typ.ID == types.Struct {
		for _, field := range typ.Fields {
			if projectedList(field.Type, projection, append(path, field.Name)) {
				return true
			}
		}
	}
	return false
}

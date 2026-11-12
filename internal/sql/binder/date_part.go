package binder

import (
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/semantics"
)

func datePartType(part, value *BoundExpression) (types.LogicalType, error) {
	t := types.LogicalType{ID: types.Double}
	if part.Kind == BoundNull || datePartNullConstant(value) {
		return t, nil
	}
	if text, ok := constantString(part); ok {
		name, err := semantics.NormalizeDatePart(text)
		if err != nil {
			return types.LogicalType{}, err
		}
		if name != "epoch" && name != "julian" {
			t.ID = types.BigInt
		}
		return t, nil
	}
	return t, nil
}

// Recognize direct NULL casts without executing scalar expressions at bind time.
func datePartNullConstant(value *BoundExpression) bool {
	if value.Kind == BoundNull || value.SQLNull {
		return true
	}
	if len(value.Children) != 1 {
		return false
	}
	if value.Kind == BoundCast || (value.Kind == BoundFunction && value.FunctionID >= function.FirstCastFunctionID && value.FunctionID < function.FirstDynamicScalarID) {
		return datePartNullConstant(value.Children[0])
	}
	return false
}

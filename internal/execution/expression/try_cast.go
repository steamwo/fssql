package expression

import (
	"fmt"
	"math"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func (e *Executor) executeTryCast(expr *binder.BoundExpression, chunk *vector.DataChunk) (*vector.Vector, error) {
	if len(expr.Children) != 1 {
		return nil, fmt.Errorf("TRY_CAST requires one argument")
	}
	src, err := e.ExecuteSQL(expr.Children[0], chunk)
	if err != nil {
		return nil, err
	}
	defer src.Release()
	out, err := vector.NewFlat(e.Scope, e.Buffers, expr.Type, chunk.Cardinality)
	if err != nil {
		return nil, err
	}
	for row := 0; row < chunk.Cardinality; row++ {
		v, err := scalarFromVector(src, row)
		if err != nil {
			out.Release()
			return nil, err
		}
		casted, conversionErr := tryCastScalarInZone(v, expr.Type, e.TimeZoneName)
		if conversionErr != nil {
			out.SetNull(row)
			continue
		}
		if err := writeScalarValue(out, row, expr.Type, casted); err != nil {
			out.Release()
			return nil, err
		}
	}
	return out, nil
}

func tryCastScalar(value ScalarValue, target types.LogicalType) (ScalarValue, error) {
	return tryCastScalarInZone(value, target, "UTC")
}
func tryCastScalarInZone(value ScalarValue, target types.LogicalType, zone string) (ScalarValue, error) {
	casted, err := castScalarInZone(value, target, zone)
	if err != nil {
		return ScalarValue{}, err
	}
	if casted.Valid && target.ID == types.Float && !math.IsInf(casted.Float64, 0) && math.IsInf(float64(float32(casted.Float64)), 0) {
		return ScalarValue{}, fmt.Errorf("TRY_CAST FLOAT overflow")
	}
	return casted, nil
}

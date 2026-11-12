package function

import (
	"fmt"

	"github.com/steamwo/fssql/internal/core/types"
)

// Keep ordinary CAST IDs in their existing order; TRY_CAST owns a separate range
// so optimizations cannot accidentally apply throwing CAST semantics to it.
func registerCasts(c *Catalog) {
	next := FirstCastFunctionID
	register := func(name string, target types.LogicalType) {
		id := next
		next++
		if next > FirstTryCastFunctionID {
			panic("cast function ID range exhausted")
		}
		for _, try := range []bool{false, true} {
			def := Definition{ID: id, Name: "__fssql_cast_" + name, Kind: Scalar, MinArgs: 1, MaxArgs: 1}
			if try {
				def.ID = FirstTryCastFunctionID + (id - FirstCastFunctionID)
				def.Name = "__fssql_try_cast_" + name
			}
			def.Bind = func(in BindInput) (BindResult, error) {
				if len(in.Arguments) != 1 {
					return BindResult{}, fmt.Errorf("CAST expects one argument")
				}
				from := in.Arguments[0]
				if from.SQLNull {
					return BindResult{ReturnType: target, Arguments: []types.LogicalType{target}}, nil
				}
				if try && !supportsTryCast(from.Type, target) {
					return BindResult{}, fmt.Errorf("TRY_CAST conversion %d -> %d is not implemented", from.Type.ID, target.ID)
				}
				return BindResult{ReturnType: target, Arguments: []types.LogicalType{from.Type}}, nil
			}
			if err := c.Register(def); err != nil {
				panic(err)
			}
		}
	}
	for _, entry := range []struct {
		name string
		typ  types.LogicalType
	}{
		{"boolean", types.LogicalType{ID: types.Boolean}}, {"tinyint", types.LogicalType{ID: types.TinyInt}},
		{"smallint", types.LogicalType{ID: types.SmallInt}}, {"integer", types.LogicalType{ID: types.Integer}},
		{"bigint", types.LogicalType{ID: types.BigInt}}, {"utinyint", types.UTinyIntType()},
		{"usmallint", types.USmallIntType()}, {"uinteger", types.UIntegerType()}, {"ubigint", types.UBigIntType()},
		{"float", types.LogicalType{ID: types.Float}}, {"double", types.LogicalType{ID: types.Double}},
		{"varchar", types.LogicalType{ID: types.Varchar}}, {"blob", types.BlobType()},
		{"date", types.LogicalType{ID: types.DateType}}, {"timestamp", types.LogicalType{ID: types.TimestampType}},
		{"timestamp_ns", types.TimestampNSType()}, {"timestamptz", types.TimestampTZType()},
	} {
		register(entry.name, entry.typ)
	}
	for width := uint8(1); width <= 38; width++ {
		for scale := uint8(0); scale <= width; scale++ {
			t, _ := types.NewDecimal(width, scale)
			register(fmt.Sprintf("decimal_%d_%d", width, scale), t)
		}
	}
	register("hugeint", types.LogicalType{ID: types.HugeInt})
}

// Reject missing conversion kernels at binding instead of hiding them as NULL.
func supportsTryCast(from, to types.LogicalType) bool {
	if from.Same(to) {
		return true
	}
	if from.ID == types.Varchar {
		return !from.Binary || (to.ID == types.Varchar && !to.Binary)
	}
	if to.ID == types.Varchar && !to.Binary {
		return from.IsNumeric() || from.ID == types.Boolean || from.ID == types.DateType || from.ID == types.TimestampType
	}
	if to.ID == types.HugeInt {
		return from.IsNumeric() || from.ID == types.Boolean
	}
	if from.ID == types.HugeInt {
		return to.IsNumeric() || to.ID == types.Boolean
	}
	if to.ID == types.Decimal {
		return from.IsNumeric() || from.ID == types.Boolean
	}
	if to.IsNumeric() {
		return from.IsNumeric() || from.ID == types.Boolean
	}
	if to.ID == types.Boolean {
		return from.IsNumeric()
	}
	if to.ID == types.DateType {
		return from.ID == types.TimestampType
	}
	if to.ID == types.TimestampType {
		return from.ID == types.TimestampType || from.ID == types.DateType
	}
	return false
}

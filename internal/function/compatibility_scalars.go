package function

import (
	"fmt"

	"github.com/steamwo/fssql/internal/core/types"
)

func registerCompatibilityScalars(c *Catalog) {
	registerTextCompatibility(c)
	for _, d := range []Definition{
		{ID: UpperID, Name: "upper", Kind: Scalar, MinArgs: 1, MaxArgs: 1, Bind: bindFixed(types.LogicalType{ID: types.Varchar}, types.LogicalType{ID: types.Varchar})},
		{ID: AbsID, Name: "abs", Kind: Scalar, MinArgs: 1, MaxArgs: 1, Bind: bindAbs},
		{ID: RoundID, Name: "round", Kind: Scalar, MinArgs: 1, MaxArgs: 2, Bind: bindRound},
		{ID: DatePartID, Name: "date_part", Kind: Scalar, MinArgs: 2, MaxArgs: 2, Bind: bindDatePart},
	} {
		if err := c.Register(d); err != nil {
			panic(err)
		}
	}
}

func bindDatePart(in BindInput) (BindResult, error) {
	if len(in.Arguments) != 2 || in.Arguments[1].SQLNull {
		return BindResult{}, fmt.Errorf("date_part requires typed DATE or TIMESTAMP input")
	}
	t := in.Arguments[1].Type
	if t.ID != types.DateType && t.ID != types.TimestampType {
		return BindResult{}, fmt.Errorf("date_part expects DATE or TIMESTAMP")
	}
	// The binder selects BIGINT for literal integer part names. A dynamic part
	// name may select epoch/julian at runtime, so its result must remain DOUBLE.
	if in.Arguments[0].Type.ID == types.List {
		return BindResult{ReturnType: types.LogicalType{ID: types.Struct}, Arguments: []types.LogicalType{in.Arguments[0].Type, t}}, nil
	}
	return BindResult{ReturnType: types.LogicalType{ID: types.Double}, Arguments: []types.LogicalType{{ID: types.Varchar}, t}}, nil
}

func bindRound(in BindInput) (BindResult, error) {
	if len(in.Arguments) < 1 || len(in.Arguments) > 2 {
		return BindResult{}, fmt.Errorf("round expects one or two arguments")
	}
	t := in.Arguments[0].Type
	if in.Arguments[0].SQLNull {
		t = types.LogicalType{ID: types.BigInt}
	}
	if !t.IsNumeric() {
		return BindResult{}, fmt.Errorf("round expects numeric input")
	}
	if t.IsUnsignedInteger() {
		if t.ID == types.BigInt {
			t = types.LogicalType{ID: types.HugeInt}
		} else {
			t = types.LogicalType{ID: types.BigInt}
		}
	}
	result := BindResult{ReturnType: t, Arguments: []types.LogicalType{t}}
	if t.ID == types.Decimal {
		result.ReturnType.Decimal.Scale = 0
	}
	if len(in.Arguments) == 2 {
		p := in.Arguments[1]
		if !p.SQLNull && (p.Type.Unsigned || (p.Type.ID != types.TinyInt && p.Type.ID != types.SmallInt && p.Type.ID != types.Integer)) {
			return BindResult{}, fmt.Errorf("round precision must be INTEGER")
		}
		result.Arguments = append(result.Arguments, types.LogicalType{ID: types.Integer})
	}
	return result, nil
}

func bindAbs(in BindInput) (BindResult, error) {
	if len(in.Arguments) != 1 {
		return BindResult{}, fmt.Errorf("abs requires one argument")
	}
	t := in.Arguments[0].Type
	if in.Arguments[0].SQLNull {
		t = types.LogicalType{ID: types.BigInt}
	}
	if !t.IsNumeric() {
		return BindResult{}, fmt.Errorf("abs expects numeric input")
	}
	return BindResult{ReturnType: t, Arguments: []types.LogicalType{t}}, nil
}

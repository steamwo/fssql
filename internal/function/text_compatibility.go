package function

import (
	"fmt"
	"github.com/steamwo/fssql/internal/core/types"
)

func registerTextCompatibility(c *Catalog) {
	text := types.LogicalType{ID: types.Varchar}
	list := types.NewList(text)
	for _, d := range []Definition{
		{ID: RangeID, Name: "range", Kind: Table, MinArgs: 1, MaxArgs: 3},
		{ID: GenerateSeriesID, Name: "generate_series", Kind: Table, MinArgs: 1, MaxArgs: 3},
		{ID: UnnestID, Name: "unnest", Kind: Table, MinArgs: 1, MaxArgs: 1},
		{ID: StructExtractID, Name: "struct_extract", Kind: Scalar, MinArgs: 2, MaxArgs: 2, Bind: func(in BindInput) (BindResult, error) {
			if len(in.Arguments) != 2 || in.Arguments[0].Type.ID != types.Struct {
				return BindResult{}, fmt.Errorf("struct_extract requires STRUCT and field name")
			}
			return BindResult{ReturnType: types.LogicalType{}, Arguments: []types.LogicalType{in.Arguments[0].Type, text}}, nil
		}},
		{ID: RegexpMatchesID, Name: "regexp_matches", Kind: Scalar, MinArgs: 2, MaxArgs: 3, Bind: bindRegex(RegexpMatchesID)},
		{ID: RegexpFullMatchID, Name: "regexp_full_match", Kind: Scalar, MinArgs: 2, MaxArgs: 3, Bind: bindRegex(RegexpFullMatchID)},
		{ID: RegexpReplaceID, Name: "regexp_replace", Kind: Scalar, MinArgs: 3, MaxArgs: 4, Bind: bindRegex(RegexpReplaceID)},
		{ID: RegexpExtractID, Name: "regexp_extract", Kind: Scalar, MinArgs: 2, MaxArgs: 4, Bind: bindRegex(RegexpExtractID)},
		{ID: RegexpExtractAllID, Name: "regexp_extract_all", Kind: Scalar, MinArgs: 2, MaxArgs: 4, Bind: bindRegex(RegexpExtractAllID)},
		{ID: RegexpSplitID, Name: "regexp_split_to_array", Kind: Scalar, MinArgs: 2, MaxArgs: 3, Bind: bindRegex(RegexpSplitID)},
		{ID: RegexpEscapeID, Name: "regexp_escape", Kind: Scalar, MinArgs: 1, MaxArgs: 1, Bind: bindFixed(text, text)},
		{ID: StringSplitID, Name: "string_split", Kind: Scalar, MinArgs: 2, MaxArgs: 2, Bind: bindFixed(list, text, text)},
		{ID: ListExtractID, Name: "list_extract", Kind: Scalar, MinArgs: 2, MaxArgs: 2, Bind: func(in BindInput) (BindResult, error) {
			if len(in.Arguments) != 2 || in.Arguments[0].Type.ID != types.List || in.Arguments[0].Type.Child == nil {
				return BindResult{}, fmt.Errorf("list_extract requires LIST and index")
			}
			return BindResult{ReturnType: *in.Arguments[0].Type.Child, Arguments: []types.LogicalType{in.Arguments[0].Type, {ID: types.BigInt}}}, nil
		}},
		{ID: ArrayLengthID, Name: "array_length", Kind: Scalar, MinArgs: 1, MaxArgs: 1, Bind: func(in BindInput) (BindResult, error) {
			if len(in.Arguments) != 1 || in.Arguments[0].Type.ID != types.List {
				return BindResult{}, fmt.Errorf("array_length requires LIST")
			}
			return BindResult{ReturnType: types.LogicalType{ID: types.BigInt}, Arguments: []types.LogicalType{in.Arguments[0].Type}}, nil
		}},
		{ID: ListValueID, Name: "list_value", Kind: Scalar, MinArgs: 0, MaxArgs: 1024, Bind: func(in BindInput) (BindResult, error) {
			child := types.LogicalType{ID: types.Integer}
			allNull := true
			for _, arg := range in.Arguments {
				allNull = allNull && arg.SQLNull
			}
			if len(in.Arguments) > 0 && !allNull {
				var err error
				child, err = commonType(in.Arguments, false)
				if err != nil {
					return BindResult{}, err
				}
			}
			args := make([]types.LogicalType, len(in.Arguments))
			for i := range args {
				args[i] = child
			}
			return BindResult{ReturnType: types.NewList(child), Arguments: args}, nil
		}},
	} {
		if err := c.Register(d); err != nil {
			panic(err)
		}
	}
}

func bindRegex(id ID) BindFunc {
	return func(in BindInput) (BindResult, error) {
		text := types.LogicalType{ID: types.Varchar}
		args := make([]types.LogicalType, len(in.Arguments))
		for i := range args {
			args[i] = text
		}
		result := text
		if id == RegexpMatchesID || id == RegexpFullMatchID {
			result = types.LogicalType{ID: types.Boolean}
		}
		if id == RegexpExtractAllID || id == RegexpSplitID {
			result = types.NewList(text)
		}
		if (id == RegexpExtractID || id == RegexpExtractAllID) && len(args) >= 3 {
			if id == RegexpExtractID && in.Arguments[2].Type.ID == types.List {
				args[2] = in.Arguments[2].Type
				result = types.LogicalType{ID: types.Struct}
			} else if in.Arguments[2].Type.ID != types.Varchar || len(args) == 4 {
				args[2] = types.LogicalType{ID: types.Integer}
			}
		}
		return BindResult{ReturnType: result, Arguments: args}, nil
	}
}

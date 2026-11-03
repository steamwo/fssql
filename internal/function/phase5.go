package function

import (
	"fmt"

	"github.com/steamwo/fssql/internal/core/types"
)

const (
	WindowRowNumberID ID = 700 + iota
	WindowCountID
	WindowSumID
	SampleMarkerID
	CTEMarkerID
)

func NewPhase5Catalog() *Catalog {
	c := NewPhase4Catalog()
	must := func(d Definition) { if err := c.Register(d); err != nil { panic(err) } }
	must(Definition{ID: WindowRowNumberID, Name: "__fssql_window_row_number", Kind: Scalar, MinArgs: 1, MaxArgs: -1, Bind: bindWindowRowNumber})
	must(Definition{ID: WindowCountID, Name: "__fssql_window_count", Kind: Scalar, MinArgs: 1, MaxArgs: -1, Bind: bindWindowCount})
	must(Definition{ID: WindowSumID, Name: "__fssql_window_sum", Kind: Scalar, MinArgs: 2, MaxArgs: -1, Bind: bindWindowSum})
	must(Definition{ID: SampleMarkerID, Name: "__fssql_sample_marker", Kind: Scalar, MinArgs: 1, MaxArgs: 1, Bind: bindPhase5Marker})
	must(Definition{ID: CTEMarkerID, Name: "__fssql_cte_marker", Kind: Scalar, MinArgs: 1, MaxArgs: 1, Bind: bindPhase5Marker})
	return c
}

func IsWindowID(id ID) bool { return id == WindowRowNumberID || id == WindowCountID || id == WindowSumID }
func IsPhase5MarkerID(id ID) bool { return id == SampleMarkerID || id == CTEMarkerID }

func windowArgumentTypes(in BindInput, valueIndex int, valueType types.LogicalType) []types.LogicalType {
	out := make([]types.LogicalType, len(in.Arguments))
	if len(out) > 0 { out[0] = types.LogicalType{ID: types.Varchar} }
	if valueIndex >= 0 && valueIndex < len(out) { out[valueIndex] = valueType }
	return out
}

func bindWindowRowNumber(in BindInput) (BindResult, error) {
	if len(in.Arguments) < 1 { return BindResult{}, fmt.Errorf("ROW_NUMBER window is missing its specification") }
	return BindResult{ReturnType: types.LogicalType{ID: types.BigInt}, Arguments: windowArgumentTypes(in, -1, types.LogicalType{})}, nil
}

func bindWindowCount(in BindInput) (BindResult, error) {
	if len(in.Arguments) < 1 { return BindResult{}, fmt.Errorf("COUNT window is missing its specification") }
	return BindResult{ReturnType: types.LogicalType{ID: types.BigInt}, Arguments: windowArgumentTypes(in, -1, types.LogicalType{})}, nil
}

func bindWindowSum(in BindInput) (BindResult, error) {
	if len(in.Arguments) < 2 || in.Arguments[1].SQLNull { return BindResult{}, fmt.Errorf("SUM window requires typed numeric input") }
	r, err := bindSum(BindInput{Arguments: []ArgumentType{in.Arguments[1]}})
	if err != nil { return BindResult{}, err }
	r.Arguments = windowArgumentTypes(in, 1, in.Arguments[1].Type)
	return r, nil
}

func bindPhase5Marker(in BindInput) (BindResult, error) {
	if len(in.Arguments) != 1 || in.Arguments[0].SQLNull || in.Arguments[0].Type.ID != types.Varchar {
		return BindResult{}, fmt.Errorf("phase 5 internal marker requires one VARCHAR literal")
	}
	return BindResult{ReturnType: types.LogicalType{ID: types.Boolean}, Arguments: []types.LogicalType{{ID: types.Varchar}}}, nil
}

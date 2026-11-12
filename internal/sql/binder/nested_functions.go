package binder

import (
	"fmt"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/semantics"
	"strings"
)

func constantStringList(expr *BoundExpression) ([]string, error) {
	if expr.Kind != BoundFunction || expr.FunctionID != function.ListValueID {
		return nil, fmt.Errorf("field names must be a constant string list")
	}
	out := make([]string, len(expr.Children))
	seen := map[string]bool{}
	for i, c := range expr.Children {
		s, ok := constantString(c)
		if !ok {
			return nil, fmt.Errorf("field names must be constant strings")
		}
		key := strings.ToLower(s)
		if seen[key] {
			return nil, fmt.Errorf("duplicate field name %q", s)
		}
		seen[key] = true
		out[i] = s
	}
	return out, nil
}
func datePartStructType(args []*BoundExpression) (types.LogicalType, error) {
	names, err := constantStringList(args[0])
	if err != nil {
		return types.LogicalType{}, err
	}
	if datePartNullConstant(args[1]) {
		return types.NewStruct(), nil
	}
	if len(names) == 0 {
		return types.LogicalType{}, fmt.Errorf("date_part requires at least one part")
	}
	fields := []types.StructField{}
	for _, name := range names {
		part, err := semantics.NormalizeDatePart(name)
		if err != nil {
			return types.LogicalType{}, err
		}
		t := types.LogicalType{ID: types.BigInt}
		if part == "epoch" || part == "julian" {
			t.ID = types.Double
		}
		fields = append(fields, types.StructField{Name: name, Type: t})
	}
	return types.NewStruct(fields...), nil
}
func regexpStructType(expr *BoundExpression) (types.LogicalType, error) {
	names, err := constantStringList(expr)
	if err != nil {
		return types.LogicalType{}, err
	}
	if len(names) == 0 {
		return types.LogicalType{}, fmt.Errorf("regexp_extract requires at least one group name")
	}
	fields := make([]types.StructField, len(names))
	for i, name := range names {
		fields[i] = types.StructField{Name: name, Type: types.LogicalType{ID: types.Varchar}}
	}
	return types.NewStruct(fields...), nil
}
func structExtractType(args []*BoundExpression) (types.LogicalType, error) {
	name, ok := constantString(args[1])
	if !ok {
		return types.LogicalType{}, fmt.Errorf("struct_extract field name must be constant")
	}
	for _, f := range args[0].Type.Fields {
		if strings.EqualFold(name, f.Name) {
			return f.Type, nil
		}
	}
	return types.LogicalType{}, fmt.Errorf("STRUCT field %q not found", name)
}

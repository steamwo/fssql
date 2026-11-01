package json

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"

	"github.com/steamwo/fssql/internal/core/types"
)

type InferredSchema struct {
	Fields     []types.StructField
	SampleRows int
}

const maxInferenceDepth = 32

func InferSchema(records [][]byte, maxRows int) (InferredSchema, error) {
	if maxRows <= 0 || maxRows > len(records) {
		maxRows = len(records)
	}
	merged := types.LogicalType{ID: types.Invalid}
	for i := 0; i < maxRows; i++ {
		t, err := inferValue(bytes.TrimSpace(records[i]), 0)
		if err != nil {
			return InferredSchema{}, fmt.Errorf("json: infer record %d: %w", i+1, err)
		}
		if t.ID != types.Struct {
			return InferredSchema{}, fmt.Errorf("json: top-level record %d is not an object", i+1)
		}
		merged = mergeType(merged, t)
	}
	if merged.ID != types.Struct {
		return InferredSchema{}, fmt.Errorf("json: no object rows available for inference")
	}
	return InferredSchema{Fields: merged.Fields, SampleRows: maxRows}, nil
}

func inferValue(raw []byte, depth int) (types.LogicalType, error) {
	if depth > maxInferenceDepth {
		return types.LogicalType{}, fmt.Errorf("maximum nesting depth exceeded")
	}
	if bytes.Equal(raw, []byte("null")) {
		return types.LogicalType{ID: types.Invalid}, nil
	}
	t := NewTokenizer(raw)
	first, err := t.Next()
	if err != nil {
		return types.LogicalType{}, err
	}
	switch first.Kind {
	case TokenTrue, TokenFalse:
		return types.LogicalType{ID: types.Boolean}, nil
	case TokenString:
		return types.LogicalType{ID: types.Varchar}, nil
	case TokenNumber:
		s := string(raw[first.Start:first.End])
		if _, err := strconv.ParseInt(s, 10, 64); err == nil {
			return types.LogicalType{ID: types.BigInt}, nil
		}
		if _, err := strconv.ParseFloat(s, 64); err == nil {
			return types.LogicalType{ID: types.Double}, nil
		}
		return types.LogicalType{}, fmt.Errorf("invalid number %q", s)
	case TokenArrayStart:
		cells, err := extractArray(raw)
		if err != nil {
			return types.LogicalType{}, err
		}
		child := types.LogicalType{ID: types.Invalid}
		for _, c := range cells {
			x, err := inferValue(c.raw, depth+1)
			if err != nil {
				return types.LogicalType{}, err
			}
			child = mergeType(child, x)
		}
		if child.ID == types.Invalid {
			child = types.LogicalType{ID: types.Varchar}
		}
		return types.NewList(child), nil
	case TokenObjectStart:
		fields, err := inferObject(raw, depth+1)
		if err != nil {
			return types.LogicalType{}, err
		}
		return types.NewStruct(fields...), nil
	case TokenNull:
		return types.LogicalType{ID: types.Invalid}, nil
	default:
		return types.LogicalType{}, fmt.Errorf("unexpected token %d", first.Kind)
	}
}

func inferObject(raw []byte, depth int) ([]types.StructField, error) {
	t := NewTokenizer(raw)
	first, err := t.Next()
	if err != nil {
		return nil, err
	}
	if first.Kind != TokenObjectStart {
		return nil, fmt.Errorf("expected object")
	}
	m := map[string]types.LogicalType{}
	tok, err := t.Next()
	if err != nil {
		return nil, err
	}
	if tok.Kind == TokenObjectEnd {
		return nil, nil
	}
	for {
		if tok.Kind != TokenString {
			return nil, fmt.Errorf("expected key")
		}
		key, err := decodeKey(raw[tok.Start:tok.End])
		if err != nil {
			return nil, err
		}
		colon, err := t.Next()
		if err != nil {
			return nil, err
		}
		if colon.Kind != TokenColon {
			return nil, fmt.Errorf("expected colon")
		}
		val, err := t.Next()
		if err != nil {
			return nil, err
		}
		start, end, err := consumeValue(raw, t, val)
		if err != nil {
			return nil, err
		}
		it, err := inferValue(raw[start:end], depth)
		if err != nil {
			return nil, err
		}
		m[key] = mergeType(m[key], it)
		delim, err := t.Next()
		if err != nil {
			return nil, err
		}
		if delim.Kind == TokenObjectEnd {
			break
		}
		if delim.Kind != TokenComma {
			return nil, fmt.Errorf("expected comma")
		}
		tok, err = t.Next()
		if err != nil {
			return nil, err
		}
	}
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	out := make([]types.StructField, 0, len(names))
	for _, k := range names {
		v := m[k]
		if v.ID == types.Invalid {
			v = types.LogicalType{ID: types.Varchar}
		}
		out = append(out, types.StructField{Name: k, Type: v})
	}
	return out, nil
}

func mergeType(a, b types.LogicalType) types.LogicalType {
	if a.ID == types.Invalid {
		return b
	}
	if b.ID == types.Invalid {
		return a
	}
	if a.ID == b.ID {
		switch a.ID {
		case types.List:
			if a.Child == nil {
				return b
			}
			if b.Child == nil {
				return a
			}
			return types.NewList(mergeType(*a.Child, *b.Child))
		case types.Struct:
			return mergeStruct(a, b)
		default:
			return a
		}
	}
	if (a.ID == types.BigInt && b.ID == types.Double) || (a.ID == types.Double && b.ID == types.BigInt) {
		return types.LogicalType{ID: types.Double}
	}
	return types.LogicalType{ID: types.Varchar}
}

func mergeStruct(a, b types.LogicalType) types.LogicalType {
	m := map[string]types.LogicalType{}
	for _, f := range a.Fields {
		m[f.Name] = f.Type
	}
	for _, f := range b.Fields {
		m[f.Name] = mergeType(m[f.Name], f.Type)
	}
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	fields := make([]types.StructField, 0, len(names))
	for _, k := range names {
		fields = append(fields, types.StructField{Name: k, Type: m[k]})
	}
	return types.NewStruct(fields...)
}

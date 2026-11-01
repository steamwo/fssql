package json

import (
	"bytes"
	"fmt"
	"github.com/steamwo/fssql/internal/core/types"
)

type InferredSchema struct {
	Fields     []types.StructField
	SampleRows int
}
type inferredNode struct {
	id     types.LogicalTypeID
	fields []inferredField
	child  *inferredNode
}
type inferredField struct {
	name string
	node inferredNode
}

func InferRecords(records [][]byte, maxRows int) (InferredSchema, error) {
	if maxRows <= 0 || maxRows > len(records) {
		maxRows = len(records)
	}
	var root inferredNode
	for i := 0; i < maxRows; i++ {
		t := NewTokenizer(records[i])
		first, err := t.Next()
		if err != nil {
			return InferredSchema{}, err
		}
		if first.Kind != TokenLBrace {
			return InferredSchema{}, fmt.Errorf("json: record %d is not object", i+1)
		}
		node, err := inferValue(t, first)
		if err != nil {
			return InferredSchema{}, err
		}
		end, err := t.Next()
		if err != nil || end.Kind != TokenEOF {
			return InferredSchema{}, fmt.Errorf("json: trailing data")
		}
		root = mergeNode(root, node)
	}
	if root.id != types.Struct {
		return InferredSchema{}, fmt.Errorf("json: no object records")
	}
	lt := nodeLogical(root)
	return InferredSchema{Fields: lt.Fields, SampleRows: maxRows}, nil
}
func inferValue(t *Tokenizer, first Token) (inferredNode, error) {
	switch first.Kind {
	case TokenNull:
		return inferredNode{}, nil
	case TokenTrue, TokenFalse:
		return inferredNode{id: types.Boolean}, nil
	case TokenString:
		return inferredNode{id: types.Varchar}, nil
	case TokenNumber:
		if !bytes.ContainsAny(first.Raw, ".eE") {
			if _, err := parseInt64(first.Raw); err == nil {
				return inferredNode{id: types.BigInt}, nil
			}
		}
		return inferredNode{id: types.Double}, nil
	case TokenLBracket:
		var child inferredNode
		next, err := t.Next()
		if err != nil {
			return inferredNode{}, err
		}
		if next.Kind == TokenRBracket {
			return inferredNode{id: types.List, child: &child}, nil
		}
		for {
			item, err := inferValue(t, next)
			if err != nil {
				return inferredNode{}, err
			}
			child = mergeNode(child, item)
			sep, err := t.Next()
			if err != nil {
				return inferredNode{}, err
			}
			if sep.Kind == TokenRBracket {
				break
			}
			if sep.Kind != TokenComma {
				return inferredNode{}, fmt.Errorf("json: expected comma or ]")
			}
			next, err = t.Next()
			if err != nil {
				return inferredNode{}, err
			}
		}
		return inferredNode{id: types.List, child: &child}, nil
	case TokenLBrace:
		node := inferredNode{id: types.Struct}
		key, err := t.Next()
		if err != nil {
			return inferredNode{}, err
		}
		if key.Kind == TokenRBrace {
			return node, nil
		}
		for {
			if key.Kind != TokenString {
				return inferredNode{}, fmt.Errorf("json: expected object key")
			}
			nameBytes := key.Raw
			if bytes.ContainsRune(nameBytes, '\\') {
				decoded, err := UnescapeString(nameBytes, make([]byte, 0, len(nameBytes)))
				if err != nil {
					return inferredNode{}, err
				}
				nameBytes = decoded
			}
			name := string(nameBytes)
			for _, f := range node.fields {
				if f.name == name {
					return inferredNode{}, fmt.Errorf("json: duplicate field %q", name)
				}
			}
			colon, err := t.Next()
			if err != nil {
				return inferredNode{}, err
			}
			if colon.Kind != TokenColon {
				return inferredNode{}, fmt.Errorf("json: expected colon")
			}
			value, err := t.Next()
			if err != nil {
				return inferredNode{}, err
			}
			cn, err := inferValue(t, value)
			if err != nil {
				return inferredNode{}, err
			}
			node.fields = append(node.fields, inferredField{name: name, node: cn})
			sep, err := t.Next()
			if err != nil {
				return inferredNode{}, err
			}
			if sep.Kind == TokenRBrace {
				break
			}
			if sep.Kind != TokenComma {
				return inferredNode{}, fmt.Errorf("json: expected comma or }")
			}
			key, err = t.Next()
			if err != nil {
				return inferredNode{}, err
			}
		}
		return node, nil
	default:
		return inferredNode{}, fmt.Errorf("json: invalid value token %d", first.Kind)
	}
}
func mergeNode(a, b inferredNode) inferredNode {
	if a.id == types.Invalid {
		return cloneNode(b)
	}
	if b.id == types.Invalid {
		return cloneNode(a)
	}
	if a.id == b.id {
		switch a.id {
		case types.Struct:
			out := cloneNode(a)
			for _, bf := range b.fields {
				found := false
				for i := range out.fields {
					if out.fields[i].name == bf.name {
						out.fields[i].node = mergeNode(out.fields[i].node, bf.node)
						found = true
						break
					}
				}
				if !found {
					out.fields = append(out.fields, inferredField{name: bf.name, node: cloneNode(bf.node)})
				}
			}
			return out
		case types.List:
			out := cloneNode(a)
			if out.child == nil {
				c := inferredNode{}
				out.child = &c
			}
			if b.child != nil {
				m := mergeNode(*out.child, *b.child)
				out.child = &m
			}
			return out
		default:
			return cloneNode(a)
		}
	}
	if (a.id == types.BigInt && b.id == types.Double) || (a.id == types.Double && b.id == types.BigInt) {
		return inferredNode{id: types.Double}
	}
	return inferredNode{id: types.Varchar}
}
func cloneNode(n inferredNode) inferredNode {
	out := inferredNode{id: n.id}
	if n.child != nil {
		c := cloneNode(*n.child)
		out.child = &c
	}
	if n.fields != nil {
		out.fields = make([]inferredField, len(n.fields))
		for i, f := range n.fields {
			out.fields[i] = inferredField{name: f.name, node: cloneNode(f.node)}
		}
	}
	return out
}
func nodeLogical(n inferredNode) types.LogicalType {
	switch n.id {
	case types.Invalid:
		return types.LogicalType{ID: types.Varchar}
	case types.Struct:
		fs := make([]types.StructField, len(n.fields))
		for i, f := range n.fields {
			fs[i] = types.StructField{Name: f.name, Type: nodeLogical(f.node)}
		}
		return types.NewStruct(fs...)
	case types.List:
		c := types.LogicalType{ID: types.Varchar}
		if n.child != nil {
			c = nodeLogical(*n.child)
		}
		return types.NewList(c)
	default:
		return types.LogicalType{ID: n.id}
	}
}

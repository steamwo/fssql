package parquet

import (
	"errors"
	"fmt"
	"strings"

	coretypes "github.com/steamwo/fssql/internal/core/types"
)

var ErrUnsupportedParquetType = errors.New("parquet: unsupported logical type")

type SchemaNode struct {
	Name      string
	Element   SchemaElement
	Children  []*SchemaNode
	Parent    *SchemaNode
	Path      []string
	MaxDef    int
	MaxRep    int
	Logical   coretypes.LogicalType
	LeafIndex int
}

type SchemaTree struct {
	Root   *SchemaNode
	Leaves []*SchemaNode
}

func BuildSchemaTree(flat []SchemaElement) (*SchemaTree, error) {
	if len(flat) == 0 {
		return nil, errors.New("parquet: empty schema")
	}
	idx := 0
	root, err := buildNode(flat, &idx, nil, nil, 0, 0)
	if err != nil {
		return nil, err
	}
	if idx != len(flat) {
		return nil, fmt.Errorf("parquet: schema has %d trailing elements", len(flat)-idx)
	}
	if root.Element.Type != nil {
		return nil, errors.New("parquet: root schema element must be a group")
	}
	tree := &SchemaTree{Root: root}
	collectLeaves(root, &tree.Leaves)
	for i, leaf := range tree.Leaves {
		leaf.LeafIndex = i
	}
	return tree, nil
}

func buildNode(flat []SchemaElement, idx *int, parent *SchemaNode, parentPath []string, parentDef, parentRep int) (*SchemaNode, error) {
	if *idx >= len(flat) {
		return nil, errors.New("parquet: truncated flattened schema")
	}
	e := flat[*idx]
	(*idx)++
	def, rep := parentDef, parentRep
	if parent != nil && e.Repetition != nil {
		switch *e.Repetition {
		case Optional:
			def++
		case Repeated:
			def++
			rep++
		case Required:
		default:
			return nil, fmt.Errorf("parquet: invalid repetition %d for %s", *e.Repetition, e.Name)
		}
	}
	p := append([]string(nil), parentPath...)
	if parent != nil {
		p = append(p, e.Name)
	}
	n := &SchemaNode{Name: e.Name, Element: e, Parent: parent, Path: p, MaxDef: def, MaxRep: rep, LeafIndex: -1}
	children := int32(0)
	if e.NumChildren != nil {
		children = *e.NumChildren
		if children < 0 {
			return nil, fmt.Errorf("parquet: negative child count for %s", e.Name)
		}
	}
	if e.Type != nil && children != 0 {
		return nil, fmt.Errorf("parquet: primitive %s has children", e.Name)
	}
	for i := int32(0); i < children; i++ {
		c, err := buildNode(flat, idx, n, p, def, rep)
		if err != nil {
			return nil, err
		}
		n.Children = append(n.Children, c)
	}
	logical, err := nodeLogicalType(n)
	if err != nil {
		return nil, err
	}
	n.Logical = logical
	return n, nil
}

func collectLeaves(n *SchemaNode, out *[]*SchemaNode) {
	if len(n.Children) == 0 {
		*out = append(*out, n)
		return
	}
	for _, c := range n.Children {
		collectLeaves(c, out)
	}
}

func nodeLogicalType(n *SchemaNode) (coretypes.LogicalType, error) {
	if n.Element.Type != nil {
		base, err := primitiveLogical(n.Element)
		if err != nil {
			return coretypes.LogicalType{}, fmt.Errorf("%w: %s: %v", ErrUnsupportedParquetType, strings.Join(n.Path, "."), err)
		}
		if n.Element.Repetition != nil && *n.Element.Repetition == Repeated {
			return coretypes.NewList(base), nil
		}
		return base, nil
	}
	if n.Parent == nil {
		fields := make([]coretypes.StructField, 0, len(n.Children))
		for _, c := range n.Children {
			fields = append(fields, coretypes.StructField{Name: c.Name, Type: c.Logical})
		}
		return coretypes.NewStruct(fields...), nil
	}
	if isListAnnotated(n.Element) {
		child, err := listElementLogical(n)
		if err != nil {
			return coretypes.LogicalType{}, err
		}
		return coretypes.NewList(child), nil
	}
	fields := make([]coretypes.StructField, 0, len(n.Children))
	for _, c := range n.Children {
		fields = append(fields, coretypes.StructField{Name: c.Name, Type: c.Logical})
	}
	st := coretypes.NewStruct(fields...)
	if n.Element.Repetition != nil && *n.Element.Repetition == Repeated {
		return coretypes.NewList(st), nil
	}
	return st, nil
}

func isListAnnotated(e SchemaElement) bool {
	return (e.Converted != nil && *e.Converted == ConvertedList) || (e.Logical != nil && e.Logical.Kind == LogicalList)
}

func listElementLogical(n *SchemaNode) (coretypes.LogicalType, error) {
	if len(n.Children) != 1 {
		return coretypes.LogicalType{}, fmt.Errorf("parquet: LIST %s must have one repeated child", n.Name)
	}
	repeated := n.Children[0]
	if repeated.Element.Repetition == nil || *repeated.Element.Repetition != Repeated {
		return coretypes.LogicalType{}, fmt.Errorf("parquet: LIST %s child is not repeated", n.Name)
	}
	if repeated.Element.Type == nil && len(repeated.Children) == 1 {
		return repeated.Children[0].Logical, nil
	}
	if repeated.Element.Type != nil {
		base, err := primitiveLogical(repeated.Element)
		if err != nil {
			return coretypes.LogicalType{}, err
		}
		return base, nil
	}
	return repeated.Logical, nil
}

func primitiveLogical(e SchemaElement) (coretypes.LogicalType, error) {
	if e.Type == nil {
		return coretypes.LogicalType{}, errors.New("group is not primitive")
	}
	if dec, ok := decimalSpec(e); ok {
		return coretypes.NewDecimal(uint8(dec.precision), uint8(dec.scale))
	}
	logicalKind := LogicalNone
	if e.Logical != nil {
		logicalKind = e.Logical.Kind
	}
	switch *e.Type {
	case TypeBoolean:
		return coretypes.LogicalType{ID: coretypes.Boolean}, nil
	case TypeInt32:
		if logicalKind == LogicalDate || (e.Converted != nil && *e.Converted == ConvertedDate) {
			return coretypes.LogicalType{ID: coretypes.DateType}, nil
		}
		return coretypes.LogicalType{ID: coretypes.Integer}, nil
	case TypeInt64:
		if logicalKind == LogicalTimestamp || (e.Converted != nil && (*e.Converted == ConvertedTimestampMillis || *e.Converted == ConvertedTimestampMicros)) {
			return coretypes.LogicalType{ID: coretypes.TimestampType}, nil
		}
		return coretypes.LogicalType{ID: coretypes.BigInt}, nil
	case TypeInt96:
		return coretypes.LogicalType{ID: coretypes.TimestampType}, nil
	case TypeFloat:
		return coretypes.LogicalType{ID: coretypes.Float}, nil
	case TypeDouble:
		return coretypes.LogicalType{ID: coretypes.Double}, nil
	case TypeByteArray:
		if logicalKind == LogicalString || (e.Converted != nil && *e.Converted == ConvertedUTF8) {
			return coretypes.LogicalType{ID: coretypes.Varchar}, nil
		}
		return coretypes.LogicalType{}, errors.New("raw BYTE_ARRAY has no v1 core BLOB type")
	case TypeFixedLenByteArray:
		return coretypes.LogicalType{}, errors.New("FIXED_LEN_BYTE_ARRAY requires a supported logical annotation")
	default:
		return coretypes.LogicalType{}, fmt.Errorf("physical type %d", *e.Type)
	}
}

type decimalInfo struct{ precision, scale int32 }

func decimalSpec(e SchemaElement) (decimalInfo, bool) {
	if e.Logical != nil && e.Logical.Kind == LogicalDecimal && e.Logical.Decimal != nil {
		return decimalInfo{precision: e.Logical.Decimal.Precision, scale: e.Logical.Decimal.Scale}, true
	}
	if e.Converted != nil && *e.Converted == ConvertedDecimal && e.Precision != nil && e.Scale != nil {
		return decimalInfo{precision: *e.Precision, scale: *e.Scale}, true
	}
	return decimalInfo{}, false
}

func (t *SchemaTree) Find(path ...string) *SchemaNode {
	if t == nil || t.Root == nil {
		return nil
	}
	for _, leaf := range t.Leaves {
		if len(leaf.Path) != len(path) {
			continue
		}
		ok := true
		for i := range path {
			if leaf.Path[i] != path[i] {
				ok = false
				break
			}
		}
		if ok {
			return leaf
		}
	}
	return nil
}

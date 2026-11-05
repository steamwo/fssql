package binder

import (
	"fmt"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/ast"
)

func combineRelationScopes(left, right *BoundRelation) Scope {
	out := cloneScope(left.Scope)
	for _, c := range right.Scope.Columns {
		c.Index += len(left.Columns)
		out.Columns = append(out.Columns, c)
	}
	for _, c := range right.Scope.Aliases {
		c.Index += len(left.Columns)
		out.Aliases = append(out.Aliases, c)
	}
	return out
}

func boundRelationColumn(relation *BoundRelation, index, offset int) *BoundExpression {
	c := relation.Columns[index]
	return &BoundExpression{Kind: BoundColumn, Type: c.Type, Nullable: c.Nullable, ColumnIndex: index + offset}
}

func appendUsingCast(input *BoundRelation, index int, target types.LogicalType) (*BoundRelation, int) {
	if input.Columns[index].Type.Same(target) {
		return input, index
	}
	projection := make([]*BoundExpression, 0, len(input.Columns)+1)
	for i := range input.Columns {
		projection = append(projection, boundRelationColumn(input, i, 0))
	}
	cast := &BoundExpression{Kind: BoundCast, Type: target, Nullable: input.Columns[index].Nullable, Children: []*BoundExpression{boundRelationColumn(input, index, 0)}}
	projection = append(projection, cast)
	columns := cloneRelationColumns(input.Columns)
	columns = append(columns, RelationColumn{Name: fmt.Sprintf("__using_cast_%d", len(columns)), Type: target, Nullable: cast.Nullable})
	return &BoundRelation{Kind: RelationProject, Columns: columns, Scope: input.Scope, Children: []*BoundRelation{input}, Projection: projection}, len(columns) - 1
}

func (b *QueryBinder) bindUsingJoin(join *ast.JoinExpr, left, right *BoundRelation) (*BoundRelation, error) {
	type key struct {
		name        ast.Identifier
		left, right int
	}
	keys := make([]key, 0, len(join.Using))
	seen := map[string]bool{}
	for _, name := range join.Using {
		if seen[name.Name] {
			return nil, b.errorAt(name.Span, "duplicate USING column "+name.Name)
		}
		seen[name.Name] = true
		qn := ast.QualifiedName{Parts: []ast.Identifier{name}}
		lc, err := left.Scope.Resolve(qn)
		if err != nil {
			return nil, b.errorAt(name.Span, "left USING column: "+err.Error())
		}
		rc, err := right.Scope.Resolve(qn)
		if err != nil {
			return nil, b.errorAt(name.Span, "right USING column: "+err.Error())
		}
		l, r, err := New(b.Functions, Scope{}).coerceComparable(boundRelationColumn(left, lc.Index, 0), boundRelationColumn(right, rc.Index, 0))
		if err != nil {
			return nil, err
		}
		var li, ri int
		left, li = appendUsingCast(left, lc.Index, l.Type)
		right, ri = appendUsingCast(right, rc.Index, r.Type)
		keys = append(keys, key{name: name, left: li, right: ri})
	}
	var condition *BoundExpression
	for _, key := range keys {
		eq := &BoundExpression{Kind: BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Nullable: true, Op: "=", Children: []*BoundExpression{boundRelationColumn(left, key.left, 0), boundRelationColumn(right, key.right, len(left.Columns))}}
		if condition == nil {
			condition = eq
		} else {
			condition = &BoundExpression{Kind: BoundBinary, Type: eq.Type, Nullable: true, Op: "AND", Children: []*BoundExpression{condition, eq}}
		}
	}
	columns := append(cloneRelationColumns(left.Columns), cloneRelationColumns(right.Columns)...)
	if join.Type == ast.JoinSemi || join.Type == ast.JoinAnti {
		columns = cloneRelationColumns(left.Columns)
	} else {
		for i := range columns {
			if join.Type == ast.JoinFull || (join.Type == ast.JoinLeft && i >= len(left.Columns)) || (join.Type == ast.JoinRight && i < len(left.Columns)) {
				columns[i].Nullable = true
			}
		}
	}
	scope := makeJoinOutputScope(left, right, columns)
	if join.Type == ast.JoinSemi || join.Type == ast.JoinAnti {
		scope = cloneScope(left.Scope)
	}
	source := &BoundRelation{Kind: RelationJoin, Columns: columns, Scope: scope, Children: []*BoundRelation{left, right}, Join: join.Type, Condition: condition}
	out := &BoundRelation{Kind: RelationProject, Children: []*BoundRelation{source}}
	mapping := map[int]int{}
	appendVisible := func(expr *BoundExpression, col ColumnBinding) {
		col.Index = len(out.Columns)
		col.Type = expr.Type
		col.Nullable = expr.Nullable
		out.Projection = append(out.Projection, expr)
		out.Columns = append(out.Columns, RelationColumn{Name: col.Name, Type: expr.Type, Nullable: expr.Nullable})
		out.Scope.Columns = append(out.Scope.Columns, col)
		if expr.Kind == BoundColumn {
			mapping[expr.ColumnIndex] = col.Index
		}
	}
	merged := make(map[string]*BoundExpression)
	for _, key := range keys {
		l := boundRelationColumn(source, key.left, 0)
		value := l
		if join.Type == ast.JoinRight {
			value = boundRelationColumn(source, key.right+len(left.Columns), 0)
		} else if join.Type == ast.JoinFull {
			r := boundRelationColumn(source, key.right+len(left.Columns), 0)
			value = &BoundExpression{Kind: BoundFunction, Type: l.Type, Nullable: true, FunctionID: function.CoalesceID, FunctionKind: function.Scalar, Children: []*BoundExpression{l, r}}
		}
		merged[key.name.Name] = value
	}
	findKey := func(name string) *BoundExpression {
		for _, key := range keys {
			if matchesIdentifier(name, key.name) {
				return merged[key.name.Name]
			}
		}
		return nil
	}
	for _, col := range left.Scope.Columns {
		if value := findKey(col.Name); value != nil {
			appendVisible(value, ColumnBinding{Name: col.Name})
		} else {
			appendVisible(boundRelationColumn(source, col.Index, 0), col)
		}
	}
	if join.Type != ast.JoinSemi && join.Type != ast.JoinAnti {
		for _, col := range right.Scope.Columns {
			if findKey(col.Name) != nil {
				continue
			}
			col.Index += len(left.Columns)
			appendVisible(boundRelationColumn(source, col.Index, 0), col)
		}
	}
	// Preserve each side's original qualified value, including NULL on the
	// unmatched side of an outer join. These columns are hidden from SELECT *.
	bindings := append(append([]ColumnBinding(nil), scope.Columns...), scope.Aliases...)
	for _, col := range bindings {
		if col.Table == "" {
			continue
		}
		index, ok := mapping[col.Index]
		if !ok {
			index = len(out.Columns)
			out.Projection = append(out.Projection, boundRelationColumn(source, col.Index, 0))
			out.Columns = append(out.Columns, source.Columns[col.Index])
			mapping[col.Index] = index
		}
		col.Index = index
		col.Type = out.Columns[index].Type
		col.Nullable = out.Columns[index].Nullable
		duplicate := false
		for _, visible := range out.Scope.Columns {
			if visible.Table == col.Table && visible.Name == col.Name && visible.Index == col.Index {
				duplicate = true
				break
			}
		}
		if !duplicate {
			out.Scope.Aliases = append(out.Scope.Aliases, col)
		}
	}
	return out, nil
}

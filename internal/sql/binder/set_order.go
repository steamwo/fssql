package binder

import (
	"strconv"
	"strings"

	"github.com/steamwo/fssql/internal/sql/ast"
)

// ORDER BY can use a branch's output alias, although the result itself retains
// the left branch's names. Resolve aliases to positions without exposing them
// as columns to consumers of the resulting relation.
func resolveSetOrderAlias(scope Scope, term ast.OrderTerm) (ast.OrderTerm, error) {
	name, ok := term.Expr.(*ast.NameExpr)
	if !ok || len(name.Name.Parts) != 1 {
		return term, nil
	}
	c, err := scope.Resolve(name.Name)
	if _, missing := err.(*missingColumnError); missing {
		return term, nil
	}
	if err != nil {
		return term, err
	}
	term.Expr = &ast.LiteralExpr{Span: term.Expr.NodeSpan(), Kind: ast.LiteralInteger, Value: strconv.Itoa(c.Index + 1)}
	return term, nil
}

func setOrderScope(stmt *ast.SelectStmt, branches [][]RelationColumn, columns []RelationColumn) Scope {
	scope := makeScope("", columns)
	add := func(name string, index int) {
		for _, c := range scope.Columns {
			if strings.EqualFold(c.Name, name) {
				return
			}
		}
		col := columns[index]
		scope.Columns = append(scope.Columns, ColumnBinding{Name: name, Index: index, Type: cloneLogicalType(col.Type), Nullable: col.Nullable})
	}
	for _, columns := range branches {
		for i, col := range columns {
			add(col.Name, i)
		}
	}
	width := len(branches[0])
	var visit func(*ast.SelectStmt)
	visit = func(q *ast.SelectStmt) {
		if q == nil {
			return
		}
		if q.UnionLeft != nil {
			visit(q.UnionLeft)
			visit(q.UnionRight)
			return
		}
		if len(q.Projection) != width {
			return
		}
		for _, p := range q.Projection {
			if p.Star {
				return
			}
		}
		for i, p := range q.Projection {
			if p.Alias != nil {
				add(p.Alias.Name, i)
			} else if name, ok := p.Expr.(*ast.NameExpr); ok && len(name.Name.Parts) > 0 {
				add(name.Name.Parts[len(name.Name.Parts)-1].Name, i)
			}
		}
	}
	visit(stmt)
	return scope
}

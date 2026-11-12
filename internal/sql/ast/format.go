package ast

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

func quoteName(s string) string { return "\"" + strings.ReplaceAll(s, "\"", "\"\"") + "\"" }
func formatName(n QualifiedName) string {
	parts := make([]string, len(n.Parts))
	for i, p := range n.Parts {
		parts[i] = quoteName(p.Name)
	}
	return strings.Join(parts, ".")
}

// FormatSelect serializes bound parameter AST values for persistent view SQL.
// It never substitutes SQL fragments supplied by the caller.
func FormatSelect(q *SelectStmt) (string, error) {
	if q == nil {
		return "", fmt.Errorf("nil SELECT")
	}
	var result string
	if q.UnionLeft != nil {
		l, e := FormatSelect(q.UnionLeft)
		if e != nil {
			return "", e
		}
		r, e := FormatSelect(q.UnionRight)
		if e != nil {
			return "", e
		}
		op := map[SetOperation]string{UnionAll: "UNION ALL", UnionDistinct: "UNION", IntersectDistinct: "INTERSECT", ExceptDistinct: "EXCEPT", IntersectAll: "INTERSECT ALL", ExceptAll: "EXCEPT ALL"}[q.SetOperation]
		if q.ByName {
			op += " BY NAME"
		}
		result = "(" + l + ") " + op + " (" + r + ")"
	} else {
		items := []string{}
		for _, p := range q.Projection {
			v := "*"
			if p.StarQualifier != nil {
				v = quoteName(p.StarQualifier.Name) + ".*"
			}
			if !p.Star {
				var err error
				v, err = FormatExpression(p.Expr)
				if err != nil {
					return "", err
				}
			}
			if p.Alias != nil {
				v += " AS " + quoteName(p.Alias.Name)
			}
			items = append(items, v)
		}
		result = "SELECT "
		if q.Distinct {
			result += "DISTINCT "
		}
		result += strings.Join(items, ", ")
		if q.From != nil {
			t, e := formatTable(q.From)
			if e != nil {
				return "", e
			}
			result += " FROM " + t
		}
		if q.Where != nil {
			x, e := FormatExpression(q.Where)
			if e != nil {
				return "", e
			}
			result += " WHERE " + x
		}
		if len(q.GroupBy) > 0 {
			xs, e := formatExpressions(q.GroupBy)
			if e != nil {
				return "", e
			}
			result += " GROUP BY " + strings.Join(xs, ", ")
		}
		if q.Having != nil {
			x, e := FormatExpression(q.Having)
			if e != nil {
				return "", e
			}
			result += " HAVING " + x
		}
	}
	if q.With != nil {
		ctes := []string{}
		for _, c := range q.With.CTEs {
			x, e := FormatSelect(c.Query)
			if e != nil {
				return "", e
			}
			name := quoteName(c.Name.Name)
			if len(c.Columns) > 0 {
				names := []string{}
				for _, n := range c.Columns {
					names = append(names, quoteName(n.Name))
				}
				name += "(" + strings.Join(names, ",") + ")"
			}
			ctes = append(ctes, name+" AS ("+x+")")
		}
		result = "WITH " + strings.Join(ctes, ", ") + " " + result
	}
	if len(q.OrderBy) > 0 {
		terms := []string{}
		for _, o := range q.OrderBy {
			x, e := FormatExpression(o.Expr)
			if e != nil {
				return "", e
			}
			if o.Descending {
				x += " DESC"
			}
			if o.NullsFirst != nil {
				if *o.NullsFirst {
					x += " NULLS FIRST"
				} else {
					x += " NULLS LAST"
				}
			}
			terms = append(terms, x)
		}
		result += " ORDER BY " + strings.Join(terms, ", ")
	}
	for _, tail := range []struct {
		name string
		expr Expr
	}{{" LIMIT ", q.Limit}, {" OFFSET ", q.Offset}} {
		if tail.expr != nil {
			x, e := FormatExpression(tail.expr)
			if e != nil {
				return "", e
			}
			result += tail.name + x
		}
	}
	return result, nil
}
func formatExpressions(es []Expr) ([]string, error) {
	out := make([]string, len(es))
	for i, e := range es {
		s, err := FormatExpression(e)
		if err != nil {
			return nil, err
		}
		out[i] = s
	}
	return out, nil
}
func FormatExpression(e Expr) (string, error) {
	switch x := e.(type) {
	case *ParameterExpr:
		if x.Value == nil {
			return "", fmt.Errorf("unbound view parameter")
		}
		if lit, ok := x.Value.(*LiteralExpr); ok && lit.Kind == LiteralInteger {
			return "CAST(" + lit.Value + " AS BIGINT)", nil
		}
		return FormatExpression(x.Value)
	case *LiteralExpr:
		switch x.Kind {
		case LiteralString:
			return "'" + strings.ReplaceAll(x.Value, "'", "''") + "'", nil
		case LiteralNull:
			return "NULL", nil
		default:
			return x.Value, nil
		}
	case *NameExpr:
		return formatName(x.Name), nil
	case *UnaryExpr:
		v, e := FormatExpression(x.Expr)
		return "(" + x.Op + " " + v + ")", e
	case *BinaryExpr:
		l, e := FormatExpression(x.Left)
		if e != nil {
			return "", e
		}
		r, e := FormatExpression(x.Right)
		return "(" + l + " " + x.Op + " " + r + ")", e
	case *CallExpr:
		args, e := formatExpressions(x.Args)
		if e != nil {
			return "", e
		}
		s := strings.Join(args, ", ")
		if x.Star {
			s = "*"
		}
		if x.Distinct {
			s = "DISTINCT " + s
		}
		return formatName(x.Function) + "(" + s + ")", nil
	case *IsNullExpr:
		v, e := FormatExpression(x.Expr)
		op := " IS NULL"
		if x.Not {
			op = " IS NOT NULL"
		}
		return "(" + v + op + ")", e
	case *SubqueryExpr:
		q, e := FormatSelect(x.Query)
		prefix := ""
		if x.Exists {
			prefix = "EXISTS "
		}
		return prefix + "(" + q + ")", e
	case *InSubqueryExpr:
		v, e := FormatExpression(x.Value)
		if e != nil {
			return "", e
		}
		q, e := FormatSelect(x.Query)
		op := " IN "
		if x.Not {
			op = " NOT IN "
		}
		if x.Comparison != "" {
			op = " " + x.Comparison + " ANY "
			if x.Not {
				return "NOT (" + v + op + "(" + q + "))", e
			}
		}
		return "(" + v + op + "(" + q + "))", e
	}
	return "", fmt.Errorf("cannot persist expression %T", e)
}
func formatTable(t TableExpr) (string, error) {
	switch x := t.(type) {
	case *TableName:
		s := formatName(x.Name)
		if x.Alias != nil {
			s += " AS " + quoteName(x.Alias.Name)
		}
		return s, nil
	case *TableFunction:
		s, e := FormatExpression(x.Call)
		if x.Alias != nil {
			s += " AS " + quoteName(x.Alias.Name)
		}
		return s, e
	case *DerivedTable:
		s, e := FormatSelect(x.Query)
		prefix := ""
		if x.Lateral {
			prefix = "LATERAL "
		}
		result := prefix + "(" + s + ") AS " + quoteName(x.Alias.Name)
		if len(x.Columns) > 0 {
			ns := []string{}
			for _, n := range x.Columns {
				ns = append(ns, quoteName(n.Name))
			}
			result += "(" + strings.Join(ns, ",") + ")"
		}
		return result, e
	case *JoinExpr:
		l, e := formatTable(x.Left)
		if e != nil {
			return "", e
		}
		r, e := formatTable(x.Right)
		if e != nil {
			return "", e
		}
		op := map[JoinType]string{JoinInner: "INNER", JoinLeft: "LEFT", JoinRight: "RIGHT", JoinFull: "FULL", JoinCross: "CROSS", JoinSemi: "SEMI", JoinAnti: "ANTI"}[x.Type]
		result := l + " " + op + " JOIN " + r
		if x.Condition != nil {
			v, e := FormatExpression(x.Condition)
			if e != nil {
				return "", e
			}
			result += " ON " + v
		}
		if len(x.Using) > 0 {
			ns := []string{}
			for _, n := range x.Using {
				ns = append(ns, quoteName(n.Name))
			}
			result += " USING (" + strings.Join(ns, ",") + ")"
		}
		return result, nil
	case *SampleTableExpr:
		s, e := formatTable(x.Input)
		keyword := " TABLESAMPLE "
		if x.Using {
			keyword = " USING SAMPLE "
		}
		if e != nil {
			return "", e
		}
		parts := strings.Split(x.Spec, ":")
		if len(parts) != 5 || parts[0] != "sample" {
			return "", fmt.Errorf("invalid sample specification")
		}
		amount, err := strconv.ParseUint(parts[2], 16, 64)
		if err != nil {
			return "", err
		}
		method := "reservoir"
		value := strconv.FormatUint(amount, 10) + " ROWS"
		if parts[1] != "r" {
			value = strconv.FormatFloat(math.Float64frombits(amount), 'g', -1, 64) + " PERCENT"
			if parts[1] == "b" {
				method = "bernoulli"
			}
			if parts[1] == "s" {
				method = "system"
			}
		}
		result := s + keyword + method + "(" + value + ")"
		if parts[3] == "s" {
			seed, err := strconv.ParseUint(parts[4], 16, 64)
			if err != nil {
				return "", err
			}
			result += " REPEATABLE(" + strconv.FormatUint(seed, 10) + ")"
		}
		return result, nil
	}
	return "", fmt.Errorf("cannot persist table %T", t)
}

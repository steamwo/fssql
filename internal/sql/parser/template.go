package parser

import (
	"fmt"
	"strconv"
	"unicode"
	"unicode/utf8"

	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/lexer"
)

// Template holds immutable parsed syntax. Binding clones parameterized SELECT
// trees; the caller can safely reuse the template for concurrent executions.
type Template struct {
	statement ast.Statement
	keys      []string
	named     bool
}

func Compile(sql string) (*Template, error) {
	tokens, err := lexer.All(sql)
	if err != nil {
		return nil, err
	}
	result := &Template{}
	slots := map[int]int{}
	names := map[string]int{}
	style := byte(0)
	for _, token := range tokens {
		if token.Kind != lexer.Parameter {
			continue
		}
		kind, key := byte('?'), ""
		if token.Raw != "?" {
			key = token.Raw[1:]
			kind = 'n'
			if key[0] >= '0' && key[0] <= '9' {
				kind = 'i'
			} else {
				r, _ := utf8.DecodeRuneInString(key)
				if !unicode.IsLetter(r) && r != '_' {
					return nil, fmt.Errorf("sql parameter: invalid name")
				}
			}
		}
		if style != 0 && style != kind {
			return nil, fmt.Errorf("sql parameter: cannot mix ?, numbered and named placeholders")
		}
		style = kind
		slot := len(result.keys)
		switch kind {
		case '?':
			result.keys = append(result.keys, "")
		case 'i':
			n, err := strconv.Atoi(key)
			if err != nil || n < 1 || n > len(tokens) {
				return nil, fmt.Errorf("sql parameter: invalid parameter number")
			}
			slot = n - 1
			for len(result.keys) < n {
				result.keys = append(result.keys, "")
			}
			names[key] = slot
		case 'n':
			if existing, ok := names[key]; ok {
				slot = existing
			} else {
				names[key] = slot
				result.keys = append(result.keys, key)
			}
			result.named = true
		}
		slots[token.Span.Start.Offset] = slot
	}
	if style == 'i' && len(names) != len(result.keys) {
		return nil, fmt.Errorf("sql parameter: numbered placeholders must be contiguous from $1")
	}
	p := &Parser{tokens: tokens, parameterSlots: slots}
	stmt, ok, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, p.errorf("expected Stable v1 statement")
	}
	p.accept(lexer.Semicolon)
	if p.peek().Kind != lexer.EOF {
		return nil, p.errorf("unexpected token %q", p.peek().Raw)
	}
	if len(result.keys) != 0 {
		if _, ok := stmt.(*ast.SelectStmt); !ok {
			return nil, fmt.Errorf("sql parameters are currently supported for SELECT statements only")
		}
	}
	result.statement = stmt
	return result, nil
}

func (t *Template) NumInput() int { return len(t.keys) }

// Bind accepts expressions representing values, not SQL fragments.
func (t *Template) Bind(values []ast.Expr, names []string) (ast.Statement, error) {
	if len(values) != len(t.keys) || len(names) != len(values) {
		return nil, fmt.Errorf("sql parameter: got %d arguments, want %d", len(values), len(t.keys))
	}
	ordered := make([]ast.Expr, len(values))
	if t.named {
		byName := map[string]ast.Expr{}
		for i, name := range names {
			if name == "" {
				return nil, fmt.Errorf("sql parameter: named placeholders require named arguments")
			}
			if _, exists := byName[name]; exists {
				return nil, fmt.Errorf("sql parameter: duplicate argument name %q", name)
			}
			byName[name] = values[i]
		}
		for i, key := range t.keys {
			value, ok := byName[key]
			if !ok {
				return nil, fmt.Errorf("sql parameter: missing argument %q", key)
			}
			ordered[i] = value
		}
	} else {
		for i, name := range names {
			if name != "" {
				return nil, fmt.Errorf("sql parameter: positional placeholders require positional arguments")
			}
			ordered[i] = values[i]
		}
	}
	if len(values) == 0 {
		return t.statement, nil
	}
	return bindTemplateSelect(t.statement.(*ast.SelectStmt), ordered)
}

func expressionHasParameter(expr ast.Expr) bool {
	switch e := expr.(type) {
	case *ast.ParameterExpr:
		return true
	case *ast.UnaryExpr:
		return expressionHasParameter(e.Expr)
	case *ast.BinaryExpr:
		return expressionHasParameter(e.Left) || expressionHasParameter(e.Right)
	case *ast.CallExpr:
		for _, arg := range e.Args {
			if expressionHasParameter(arg) {
				return true
			}
		}
	case *ast.IsNullExpr:
		return expressionHasParameter(e.Expr)
	}
	return false
}

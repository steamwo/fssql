package expression

import (
	"fmt"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/semantics"
)

func evalPredicate(id function.ID, args []ScalarValue) (ScalarValue, error) {
	if id == function.NotDistinctID {
		out := ScalarValue{Type: types.LogicalType{ID: types.Boolean}, Valid: true}
		if !args[0].Valid || !args[1].Valid {
			out.Bool = !args[0].Valid && !args[1].Valid
			return out, nil
		}
		cmp, err := predicateCompare(args[0], args[1])
		out.Bool = cmp == 0
		return out, err
	}
	out := ScalarValue{Type: types.LogicalType{ID: types.Boolean}, Valid: true}
	if id == function.MembershipMarkID {
		if args[1].Valid {
			out.Bool = true
			return out, nil
		}
		total, nonnull := int64(0), int64(0)
		if args[2].Valid {
			total = args[2].Int64
		}
		if args[3].Valid {
			nonnull = args[3].Int64
		}
		if total == 0 {
			return out, nil
		}
		if !args[0].Valid || total > nonnull {
			out.Valid = false
		}
		return out, nil
	}
	if id == function.ScalarCardinalityID {
		if args[1].Valid && args[1].Int64 > 1 {
			return ScalarValue{}, fmt.Errorf("scalar subquery returned more than one row")
		}
		return args[0], nil
	}
	if id == function.InListID {
		if !args[0].Valid {
			out.Valid = false
			return out, nil
		}
		haveNull := false
		for _, arg := range args[1:] {
			if !arg.Valid {
				haveNull = true
				continue
			}
			order, err := predicateCompare(args[0], arg)
			if err != nil {
				return ScalarValue{}, err
			}
			if order == 0 {
				out.Bool = true
				return out, nil
			}
		}
		out.Valid = !haveNull
		return out, nil
	}
	if id == function.BetweenID {
		if !args[0].Valid {
			out.Valid = false
			return out, nil
		}
		unknown := false
		for i, arg := range args[1:] {
			if !arg.Valid {
				unknown = true
				continue
			}
			order, err := predicateCompare(args[0], arg)
			if err != nil {
				return ScalarValue{}, err
			}
			if (i == 0 && order < 0) || (i == 1 && order > 0) {
				out.Bool = false
				return out, nil
			}
		}
		out.Valid = !unknown
		out.Bool = true
		return out, nil
	}
	for _, arg := range args {
		if !arg.Valid {
			out.Valid = false
			return out, nil
		}
	}
	escape := -1
	if len(args) == 3 {
		text := args[2].Bytes
		if len(text) > 1 {
			return ScalarValue{}, fmt.Errorf("LIKE ESCAPE must be empty or one byte")
		}
		if len(text) == 1 {
			escape = int(text[0])
		}
	}
	value, err := matchLike(string(args[0].Bytes), string(args[1].Bytes), escape)
	out.Bool = value
	return out, err
}

func predicateCompare(a, b ScalarValue) (int, error) {
	if a.Type.ID == types.Float || a.Type.ID == types.Double {
		return semantics.CompareFloat64(a.Float64, b.Float64), nil
	}
	return compareScalar(a, b)
}

// Match Unicode characters with SQL wildcards. The last % provides a bounded
// backtracking point; no regex compilation or input-sized DP matrix is needed.
func matchLike(text, pattern string, escape int) (bool, error) {
	type token struct {
		value rune
		wild  bool
	}
	var tokens []token
	escaped := false
	for _, r := range pattern {
		if escaped {
			tokens = append(tokens, token{value: r})
			escaped = false
			continue
		}
		if escape >= 0 && int(r) == escape {
			escaped = true
			continue
		}
		tokens = append(tokens, token{value: r, wild: r == '%' || r == '_'})
	}
	if escaped {
		return false, fmt.Errorf("LIKE pattern ends with escape character")
	}
	runes := []rune(text)
	i, j, star, retry := 0, 0, -1, 0
	for i < len(runes) {
		if j < len(tokens) && tokens[j].wild && tokens[j].value == '%' {
			star = j
			j++
			retry = i
			continue
		}
		if j < len(tokens) && ((tokens[j].wild && tokens[j].value == '_') || (!tokens[j].wild && tokens[j].value == runes[i])) {
			i++
			j++
			continue
		}
		if star >= 0 {
			retry++
			i = retry
			j = star + 1
			continue
		}
		return false, nil
	}
	for j < len(tokens) && tokens[j].wild && tokens[j].value == '%' {
		j++
	}
	return j == len(tokens), nil
}

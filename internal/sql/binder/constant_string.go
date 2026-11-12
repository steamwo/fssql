package binder

import (
	"crypto/md5"
	"encoding/hex"
	"strconv"
	"strings"
	"unicode"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
)

// Fold only built-in, pure string expressions. Never execute registered UDFs or
// perform I/O while binding type-dependent functions.
func constantString(e *BoundExpression) (string, bool) {
	if e == nil || e.Type.ID != types.Varchar || e.Type.Binary {
		return "", false
	}
	if e.Kind == BoundLiteral {
		return e.Literal, true
	}
	if e.Kind == BoundCast && len(e.Children) == 1 {
		return constantString(e.Children[0])
	}
	if e.Kind != BoundFunction {
		return "", false
	}
	if e.FunctionID >= function.FirstCastFunctionID && e.FunctionID < function.FirstTryCastFunctionID && len(e.Children) == 1 {
		if s, ok := constantString(e.Children[0]); ok {
			return s, true
		}
		if n, ok := constantRoundPrecision(e.Children[0]); ok {
			return strconv.FormatInt(n, 10), true
		}
		return "", false
	}
	args := make([]string, len(e.Children))
	for i, c := range e.Children {
		s, ok := constantString(c)
		if !ok {
			return "", false
		}
		args[i] = s
	}
	switch e.FunctionID {
	case function.LowerID:
		return strings.ToLower(args[0]), true
	case function.UpperID:
		return strings.Map(func(r rune) rune {
			if r == 'ß' {
				return 'ẞ'
			}
			return unicode.ToUpper(r)
		}, args[0]), true
	case function.ConcatID:
		return args[0] + args[1], true
	case function.ReplaceID:
		return strings.ReplaceAll(args[0], args[1], args[2]), true
	case function.MD5ID:
		sum := md5.Sum([]byte(args[0]))
		return hex.EncodeToString(sum[:]), true
	case function.HexID:
		return strings.ToUpper(hex.EncodeToString([]byte(args[0]))), true
	}
	return "", false
}

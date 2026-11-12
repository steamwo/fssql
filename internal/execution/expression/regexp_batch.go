package expression

import (
	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
	"strings"
)

func (e *Executor) executeRegexpBatch(expr *binder.BoundExpression, args []*vector.Vector, rows int) (*vector.Vector, bool, error) {
	id := expr.FunctionID
	if expr.Type.ID == types.Struct || (id != function.RegexpMatchesID && id != function.RegexpFullMatchID && id != function.RegexpReplaceID && id != function.RegexpExtractID && id != function.RegexpEscapeID) {
		return nil, false, nil
	}
	out, err := vector.NewFlat(e.Scope, e.Buffers, expr.Type, rows)
	if err != nil {
		return nil, true, err
	}
	fail := func(err error) (*vector.Vector, bool, error) { out.Release(); return nil, true, err }
	for row := 0; row < rows; row++ {
		values := make([]ScalarValue, len(args))
		null := false
		for i, arg := range args {
			value, err := scalarFromVector(arg, row)
			if err != nil {
				return fail(err)
			}
			values[i] = value
			null = null || !value.Valid
		}
		if null {
			out.SetNull(row)
			continue
		}
		budget := int64(len(values[0].Bytes))*2 + 1024
		if id != function.RegexpEscapeID {
			budget += int64(len(values[1].Bytes)) * 256
		}
		if id == function.RegexpReplaceID {
			options, _, err := regexArguments(id, values)
			if err != nil {
				return fail(err)
			}
			captures := strings.Count(string(values[1].Bytes), "(") + 1
			count := int64(1)
			if strings.ContainsRune(options, 'g') {
				count = int64(len(values[0].Bytes)) + 1
			}
			replacement := values[2].Bytes
			references := 0
			for i := 0; i+1 < len(replacement); i++ {
				if replacement[i] == '\\' {
					i++
					if replacement[i] >= '0' && replacement[i] <= '9' {
						references++
					}
				}
			}
			budget += count*(int64(captures)*16+24+int64(len(replacement))) + int64(references)*int64(len(values[0].Bytes))
		}

		reservation, err := e.Scope.Reserve(memory.TagVector, budget)
		if err != nil {
			return fail(err)
		}
		value, err := regexScalar(id, expr.Type, values)
		if err == nil {
			err = writeScalarValue(out, row, expr.Type, value)
		}
		reservation.Release()
		if err != nil {
			return fail(err)
		}
	}
	return out, true, nil
}

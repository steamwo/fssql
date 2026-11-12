package expression

import (
	"fmt"
	"github.com/steamwo/fssql/internal/core/memory"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func (e *Executor) executeNestedBuiltin(expr *binder.BoundExpression, args []*vector.Vector, rows int) (*vector.Vector, bool, error) {
	id := expr.FunctionID
	if id == function.StructExtractID || (expr.Type.ID == types.Struct && (id == function.DatePartID || id == function.RegexpExtractID)) {
		out, err := e.executeStructBuiltin(expr, args, rows)
		return out, true, err
	}
	if id != function.StringSplitID && id != function.RegexpExtractAllID && id != function.RegexpSplitID && id != function.ListValueID && id != function.ListExtractID && id != function.ArrayLengthID {
		return nil, false, nil
	}
	out, err := vector.NewFlat(e.Scope, e.Buffers, expr.Type, rows)
	if err != nil {
		return nil, true, err
	}
	fail := func(err error) (*vector.Vector, bool, error) { out.Release(); return nil, true, err }
	if id == function.ListExtractID || id == function.ArrayLengthID {
		for row := 0; row < rows; row++ {
			entry, valid := args[0].ListEntry(row)
			if !valid {
				out.SetNull(row)
				continue
			}
			if id == function.ArrayLengthID {
				out.SetInt64(row, int64(entry.Length))
				continue
			}
			index, ok := args[1].Int64(row)
			if !ok {
				out.SetNull(row)
				continue
			}
			if index < 0 {
				index = int64(entry.Length) + index + 1
			}
			if index < 1 || index > int64(entry.Length) {
				out.SetNull(row)
				continue
			}
			base, _ := args[0].NestedRow(row)
			if len(base.Children()) != 1 {
				return fail(fmt.Errorf("invalid LIST child"))
			}
			if err := vector.CopyValue(base.Children()[0], int(entry.Offset)+int(index)-1, out, row); err != nil {
				return fail(err)
			}
		}
		return out, true, nil
	}
	if id == function.ListValueID {
		child, err := vector.NewFlat(e.Scope, e.Buffers, *expr.Type.Child, rows*len(args))
		if err != nil {
			return fail(err)
		}
		out.SetChildren(child)
		for row := 0; row < rows; row++ {
			out.SetListEntry(row, vector.ListEntry{Offset: uint32(row * len(args)), Length: uint32(len(args))})
			for j, arg := range args {
				if err := vector.CopyValue(arg, row, child, row*len(args)+j); err != nil {
					return fail(err)
				}
			}
		}
		return out, true, nil
	}
	// First count literal splits, so their potentially large child storage is
	// charged to the query before allocating temporary string slices.
	var scratch []*memory.Reservation
	defer func() {
		for _, r := range scratch {
			r.Release()
		}
	}()
	parts := make([][]string, rows)
	partValid := make([][]bool, rows)
	total := 0
	for row := 0; row < rows; row++ {
		values := make([]ScalarValue, len(args))
		null := false
		for i, arg := range args {
			values[i], err = scalarFromVector(arg, row)
			if err != nil {
				return fail(err)
			}
			null = null || !values[i].Valid
		}
		if null {
			out.SetNull(row)
			continue
		}
		s := string(values[0].Bytes)
		pattern := string(values[1].Bytes)
		if id == function.StringSplitID {
			n := strings.Count(s, pattern) + 1
			if pattern == "" {
				n = utf8.RuneCountInString(s)
				if n == 0 {
					n = 1
				}
			}
			out.SetListEntry(row, vector.ListEntry{Offset: uint32(total), Length: uint32(n)})
			total += n
			continue
		}
		options, group, err := regexArguments(id, values)
		if err != nil {
			return fail(err)
		}
		budget := int64(len(pattern))*256 + int64(len(s)+1)*(40+int64(strings.Count(pattern, "(")+1)*16)
		reservation, err := e.Scope.Reserve(memory.TagVector, budget)
		if err != nil {
			return fail(err)
		}
		scratch = append(scratch, reservation)
		r, err := compileSQLRegex(pattern, options, false)
		if err != nil {
			return fail(err)
		}
		if id == function.RegexpSplitID {
			parts[row] = r.Split(s, -1)
		} else {
			if group > r.NumSubexp() {
				return fail(fmt.Errorf("regexp_extract group exceeds capture count"))
			}
			matches := r.FindAllStringSubmatchIndex(s, -1)
			parts[row] = make([]string, len(matches))
			partValid[row] = make([]bool, len(matches))
			for i, m := range matches {
				if m[group*2] >= 0 {
					parts[row][i] = s[m[group*2]:m[group*2+1]]
					partValid[row][i] = true
				}
			}
		}
		out.SetListEntry(row, vector.ListEntry{Offset: uint32(total), Length: uint32(len(parts[row]))})
		total += len(parts[row])
	}
	if uint64(total) > uint64(^uint32(0)) {
		return fail(fmt.Errorf("LIST child count overflow"))
	}
	if id == function.StringSplitID {
		reservation, err := e.Scope.Reserve(memory.TagVector, int64(total)*16)
		if err != nil {
			return fail(err)
		}
		scratch = append(scratch, reservation)
	}
	child, err := vector.NewFlat(e.Scope, e.Buffers, types.LogicalType{ID: types.Varchar}, total)
	if err != nil {
		return fail(err)
	}
	out.SetChildren(child)
	for row := 0; row < rows; row++ {
		if !out.IsValid(row) {
			continue
		}
		if id == function.StringSplitID {
			s, _ := args[0].Bytes(row)
			sep, _ := args[1].Bytes(row)
			parts[row] = strings.Split(string(s), string(sep))
			if len(parts[row]) == 0 {
				parts[row] = []string{""}
			}
		}
		entry, _ := out.ListEntry(row)
		for i, part := range parts[row] {
			if partValid[row] != nil && !partValid[row][i] {
				child.SetNull(int(entry.Offset) + i)
				continue
			}
			if err := child.SetBytes(int(entry.Offset)+i, []byte(part)); err != nil {
				return fail(err)
			}
		}
	}
	return out, true, nil
}

func (e *Executor) executeStructBuiltin(expr *binder.BoundExpression, args []*vector.Vector, rows int) (*vector.Vector, error) {
	out, err := vector.NewFlat(e.Scope, e.Buffers, expr.Type, rows)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*vector.Vector, error) { out.Release(); return nil, err }
	if expr.FunctionID == function.StructExtractID {
		for row := 0; row < rows; row++ {
			if !args[0].IsValid(row) || !args[1].IsValid(row) {
				out.SetNull(row)
				continue
			}
			name, _ := args[1].String(row)
			base, index := args[0].NestedRow(row)
			found := -1
			for i, f := range args[0].Type.Fields {
				if strings.EqualFold(name, f.Name) {
					found = i
					break
				}
			}
			if found < 0 || found >= len(base.Children()) {
				return fail(fmt.Errorf("STRUCT field %q not found", name))
			}
			if err := vector.CopyValue(base.Children()[found], index, out, row); err != nil {
				return fail(err)
			}
		}
		return out, nil
	}
	children := make([]*vector.Vector, len(expr.Type.Fields))
	for i, f := range expr.Type.Fields {
		v, err := vector.NewFlat(e.Scope, e.Buffers, f.Type, rows)
		if err != nil {
			for _, c := range children {
				if c != nil {
					c.Release()
				}
			}
			return fail(err)
		}
		children[i] = v
	}
	out.SetChildren(children...)
	for row := 0; row < rows; row++ {
		null := false
		for _, arg := range args {
			null = null || !arg.IsValid(row)
		}
		if null {
			out.SetNull(row)
			for _, c := range children {
				c.SetNull(row)
			}
			continue
		}
		if expr.FunctionID == function.DatePartID {
			value, err := scalarFromVector(args[1], row)
			if err != nil {
				return fail(err)
			}
			value.TimeZoneName = e.TimeZoneName
			for i, f := range expr.Type.Fields {
				part, err := datePartScalar(f.Type, f.Name, value)
				if err != nil {
					return fail(err)
				}
				if err = writeScalarValue(children[i], row, f.Type, part); err != nil {
					return fail(err)
				}
			}
			continue
		}
		s, _ := args[0].String(row)
		pattern, _ := args[1].String(row)
		options := ""
		if len(args) == 4 {
			options, _ = args[3].String(row)
		}
		var r *regexp.Regexp
		r, err = compileSQLRegex(pattern, options, false)
		if err != nil {
			return fail(err)
		}
		if r.NumSubexp() < len(children) {
			return fail(fmt.Errorf("regexp_extract group count is too small"))
		}
		match := r.FindStringSubmatch(s)
		for i, c := range children {
			value := ""
			if len(match) > i+1 {
				value = match[i+1]
			}
			if err := c.SetBytes(row, []byte(value)); err != nil {
				return fail(err)
			}
		}
	}
	return out, nil
}

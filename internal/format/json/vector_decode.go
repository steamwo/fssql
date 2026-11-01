package json

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

func buildVector(typ types.LogicalType, cells []cell, budget *cellBudget, scope *memory.Scope, bm *memory.BufferManager) (*vector.Vector, error) {
	switch typ.ID {
	case types.Struct:
		parent, err := vector.NewFlat(scope, bm, typ, len(cells))
		if err != nil {
			return nil, err
		}
		childCells := make([][]cell, len(typ.Fields))
		for i := range childCells {
			childCells[i] = make([]cell, len(cells))
		}
		idx := make(map[string]int, len(typ.Fields))
		for i, f := range typ.Fields {
			idx[f.Name] = i
		}
		if err := budget.grow(len(cells) * len(typ.Fields)); err != nil {
			parent.Release()
			return nil, err
		}
		for r, c := range cells {
			if !c.present || isNull(c.raw) {
				parent.SetNull(r)
				continue
			}
			parts, err := extractObject(c.raw, idx, len(typ.Fields))
			if err != nil {
				parent.Release()
				return nil, err
			}
			for i := range parts {
				childCells[i][r] = parts[i]
			}
		}
		children := make([]*vector.Vector, 0, len(typ.Fields))
		for i, f := range typ.Fields {
			cv, err := buildVector(f.Type, childCells[i], budget, scope, bm)
			if err != nil {
				for _, child := range children {
					child.Release()
				}
				parent.Release()
				return nil, err
			}
			children = append(children, cv)
		}
		parent.SetChildren(children...)
		return parent, nil
	case types.List:
		if typ.Child == nil {
			return nil, fmt.Errorf("LIST has no child type")
		}
		parent, err := vector.NewFlat(scope, bm, typ, len(cells))
		if err != nil {
			return nil, err
		}
		var elems []cell
		for r, c := range cells {
			if !c.present || isNull(c.raw) {
				parent.SetNull(r)
				continue
			}
			parts, err := extractArray(c.raw)
			if err != nil {
				parent.Release()
				return nil, err
			}
			if err := budget.grow(len(parts)); err != nil {
				parent.Release()
				return nil, err
			}
			off := len(elems)
			elems = append(elems, parts...)
			parent.SetListEntry(r, vector.ListEntry{Offset: uint32(off), Length: uint32(len(parts))})
		}
		child, err := buildVector(*typ.Child, elems, budget, scope, bm)
		if err != nil {
			parent.Release()
			return nil, err
		}
		parent.SetChildren(child)
		return parent, nil
	default:
		v, err := vector.NewFlat(scope, bm, typ, len(cells))
		if err != nil {
			return nil, err
		}
		for i, c := range cells {
			if !c.present || isNull(c.raw) {
				v.SetNull(i)
				continue
			}
			if err := decodePrimitive(v, typ, i, c.raw, scope, bm); err != nil {
				v.Release()
				return nil, err
			}
		}
		return v, nil
	}
}

func isNull(raw []byte) bool { return bytes.Equal(bytes.TrimSpace(raw), []byte("null")) }

func decodePrimitive(v *vector.Vector, typ types.LogicalType, row int, raw []byte, scope *memory.Scope, bm *memory.BufferManager) error {
	raw = bytes.TrimSpace(raw)
	switch typ.ID {
	case types.Boolean:
		if bytes.Equal(raw, []byte("true")) {
			v.SetBool(row, true)
			return nil
		}
		if bytes.Equal(raw, []byte("false")) {
			v.SetBool(row, false)
			return nil
		}
		return fmt.Errorf("invalid BOOLEAN %q", raw)
	case types.Integer:
		if len(raw) > 128 {
			return fmt.Errorf("number too long")
		}
		x, err := strconv.ParseInt(string(raw), 10, 32)
		if err != nil {
			return fmt.Errorf("invalid INTEGER %q", raw)
		}
		v.SetInt32(row, int32(x))
		return nil
	case types.BigInt:
		if len(raw) > 128 {
			return fmt.Errorf("number too long")
		}
		x, err := strconv.ParseInt(string(raw), 10, 64)
		if err != nil {
			return fmt.Errorf("invalid BIGINT %q", raw)
		}
		v.SetInt64(row, x)
		return nil
	case types.Float:
		if len(raw) > 128 {
			return fmt.Errorf("number too long")
		}
		x, err := strconv.ParseFloat(string(raw), 32)
		if err != nil || math.IsInf(x, 0) {
			return fmt.Errorf("invalid FLOAT %q", raw)
		}
		v.SetFloat32(row, float32(x))
		return nil
	case types.Double:
		if len(raw) > 128 {
			return fmt.Errorf("number too long")
		}
		x, err := strconv.ParseFloat(string(raw), 64)
		if err != nil || math.IsInf(x, 0) {
			return fmt.Errorf("invalid DOUBLE %q", raw)
		}
		v.SetFloat64(row, x)
		return nil
	case types.Varchar:
		if len(raw) < 2 || raw[0] != '"' || raw[len(raw)-1] != '"' {
			return fmt.Errorf("expected JSON string")
		}
		buf, err := unescapeJSONString(raw, scope, bm)
		if err != nil {
			return err
		}
		defer buf.Release()
		return v.SetStringBytes(row, buf.Bytes())
	default:
		return fmt.Errorf("unsupported typed decode type %d", typ.ID)
	}
}

func unescapeJSONString(raw []byte, scope *memory.Scope, bm *memory.BufferManager) (*memory.Buffer, error) {
	if len(raw) < 2 {
		return nil, fmt.Errorf("invalid JSON string")
	}
	buf, err := bm.Allocate(scope, memory.TagJSONDecode, len(raw)-2)
	if err != nil {
		return nil, err
	}
	dst := buf.Bytes()[:0]
	src := raw[1 : len(raw)-1]
	for i := 0; i < len(src); i++ {
		c := src[i]
		if c != '\\' {
			dst = append(dst, c)
			continue
		}
		i++
		if i >= len(src) {
			buf.Release()
			return nil, fmt.Errorf("invalid string escape")
		}
		switch src[i] {
		case '"', '\\', '/':
			dst = append(dst, src[i])
		case 'b':
			dst = append(dst, '\b')
		case 'f':
			dst = append(dst, '\f')
		case 'n':
			dst = append(dst, '\n')
		case 'r':
			dst = append(dst, '\r')
		case 't':
			dst = append(dst, '\t')
		case 'u':
			if i+4 >= len(src) {
				buf.Release()
				return nil, fmt.Errorf("short unicode escape")
			}
			u, err := parseHex4(src[i+1 : i+5])
			if err != nil {
				buf.Release()
				return nil, err
			}
			i += 4
			r := rune(u)
			if utf16.IsSurrogate(r) {
				if i+6 >= len(src) || src[i+1] != '\\' || src[i+2] != 'u' {
					buf.Release()
					return nil, fmt.Errorf("unpaired surrogate")
				}
				v, err := parseHex4(src[i+3 : i+7])
				if err != nil {
					buf.Release()
					return nil, err
				}
				r = utf16.DecodeRune(r, rune(v))
				if r == utf8.RuneError {
					buf.Release()
					return nil, fmt.Errorf("invalid surrogate pair")
				}
				i += 6
			}
			var tmp [utf8.UTFMax]byte
			n := utf8.EncodeRune(tmp[:], r)
			dst = append(dst, tmp[:n]...)
		default:
			buf.Release()
			return nil, fmt.Errorf("invalid escape \\%c", src[i])
		}
	}
	if err := buf.Resize(len(dst), nil); err != nil {
		buf.Release()
		return nil, err
	}
	return buf, nil
}

func parseHex4(b []byte) (uint16, error) {
	var v uint16
	for _, c := range b {
		v <<= 4
		switch {
		case c >= '0' && c <= '9':
			v += uint16(c - '0')
		case c >= 'a' && c <= 'f':
			v += uint16(c - 'a' + 10)
		case c >= 'A' && c <= 'F':
			v += uint16(c - 'A' + 10)
		default:
			return 0, fmt.Errorf("invalid unicode escape")
		}
	}
	return v, nil
}

package json

import (
	"bytes"
	"fmt"
	"math"
	"strconv"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

type vectorPlan struct {
	typ      types.LogicalType
	count    int
	next     int
	children []*vectorPlan
}

func newPlan(t types.LogicalType) (*vectorPlan, error) {
	p := &vectorPlan{typ: t}
	switch t.ID {
	case types.Struct:
		p.children = make([]*vectorPlan, len(t.Fields))
		for i, f := range t.Fields {
			c, err := newPlan(f.Type)
			if err != nil {
				return nil, err
			}
			p.children[i] = c
		}
	case types.List:
		if t.Child == nil {
			return nil, fmt.Errorf("json: LIST has nil child")
		}
		c, err := newPlan(*t.Child)
		if err != nil {
			return nil, err
		}
		p.children = []*vectorPlan{c}
	}
	return p, nil
}
func setInstances(p *vectorPlan, n int) {
	p.count = n
	if p.typ.ID == types.Struct {
		for _, c := range p.children {
			setInstances(c, n)
		}
	}
}
func addInstance(p *vectorPlan) {
	p.count++
	if p.typ.ID == types.Struct {
		for _, c := range p.children {
			addInstance(c)
		}
	}
}
func resetNext(p *vectorPlan) {
	p.next = 0
	for _, c := range p.children {
		resetNext(c)
	}
}
func allocatePlan(p *vectorPlan, scope *memory.Scope, bm *memory.BufferManager) (*vector.Vector, error) {
	v, err := vector.NewFlat(scope, bm, p.typ, p.count)
	if err != nil {
		return nil, err
	}
	if len(p.children) == 0 {
		return v, nil
	}
	children := make([]*vector.Vector, 0, len(p.children))
	for _, cp := range p.children {
		c, err := allocatePlan(cp, scope, bm)
		if err != nil {
			for _, x := range children {
				x.Release()
			}
			v.Release()
			return nil, err
		}
		children = append(children, c)
	}
	v.SetChildren(children...)
	return v, nil
}

type typedDecoder struct {
	projection ProjectedPathSet
	scratch    *memory.Buffer
	path       [maxJSONDepth]string
}

func newTypedDecoder(proj ProjectedPathSet, scope *memory.Scope, bm *memory.BufferManager) (*typedDecoder, error) {
	b, err := bm.Allocate(scope, memory.TagJSONDecode, 256)
	if err != nil {
		return nil, err
	}
	return &typedDecoder{projection: proj, scratch: b}, nil
}
func (d *typedDecoder) close() {
	if d.scratch != nil {
		d.scratch.Release()
		d.scratch = nil
	}
}
func (d *typedDecoder) ensureScratch(n int) error {
	if n < 1 {
		n = 1
	}
	if d.scratch.Len() >= n {
		return nil
	}
	cap := d.scratch.Len()
	if cap < 1 {
		cap = 1
	}
	for cap < n {
		cap *= 2
	}
	return d.scratch.Resize(cap, nil)
}
func (d *typedDecoder) decodedString(raw []byte) ([]byte, error) {
	if !bytes.ContainsRune(raw, '\\') {
		return raw, nil
	}
	if err := d.ensureScratch(len(raw)); err != nil {
		return nil, err
	}
	return UnescapeString(raw, d.scratch.Bytes()[:0])
}
func (d *typedDecoder) findField(raw []byte, fields []types.StructField) (int, error) {
	decoded, err := d.decodedString(raw)
	if err != nil {
		return -1, err
	}
	for i, f := range fields {
		if len(decoded) == len(f.Name) && bytes.Equal(decoded, []byte(f.Name)) {
			return i, nil
		}
	}
	return -1, nil
}
func (d *typedDecoder) relevant(depth int) bool { return d.projection.Relevant(d.path[:depth]) }

func DecodeRecords(records [][]byte, schema []types.StructField, projection ProjectedPathSet, scope *memory.Scope, bm *memory.BufferManager) (*vector.DataChunk, error) {
	if scope == nil || bm == nil {
		return nil, fmt.Errorf("json: managed memory required")
	}
	if len(schema) == 0 {
		return nil, fmt.Errorf("json: empty schema")
	}
	if len(records) > vector.DefaultVectorSize {
		return nil, fmt.Errorf("json: batch exceeds vector size")
	}
	plans := make([]*vectorPlan, len(schema))
	for i, f := range schema {
		p, err := newPlan(f.Type)
		if err != nil {
			return nil, err
		}
		setInstances(p, len(records))
		plans[i] = p
	}
	d, err := newTypedDecoder(projection, scope, bm)
	if err != nil {
		return nil, err
	}
	defer d.close()
	for _, rec := range records {
		if err := d.countRecord(rec, schema, plans); err != nil {
			return nil, err
		}
	}
	cols := make([]*vector.Vector, 0, len(plans))
	for _, p := range plans {
		v, err := allocatePlan(p, scope, bm)
		if err != nil {
			for _, c := range cols {
				c.Release()
			}
			return nil, err
		}
		cols = append(cols, v)
	}
	chunk, err := vector.NewDataChunk(cols...)
	if err != nil {
		for _, c := range cols {
			c.Release()
		}
		return nil, err
	}
	for _, p := range plans {
		resetNext(p)
	}
	for row, rec := range records {
		if err := d.decodeRecord(rec, row, schema, plans, chunk.Columns); err != nil {
			chunk.Release()
			return nil, err
		}
	}
	return chunk, nil
}

func (d *typedDecoder) countRecord(rec []byte, schema []types.StructField, plans []*vectorPlan) error {
	t := NewTokenizer(rec)
	first, err := t.Next()
	if err != nil {
		return err
	}
	if first.Kind != TokenLBrace {
		return fmt.Errorf("json: record must be object")
	}
	seen := make([]bool, len(schema))
	key, err := t.Next()
	if err != nil {
		return err
	}
	if key.Kind == TokenRBrace {
		return ensureEOF(t)
	}
	for {
		if key.Kind != TokenString {
			return fmt.Errorf("json: expected object key")
		}
		idx, err := d.findField(key.Raw, schema)
		if err != nil {
			return err
		}
		colon, err := t.Next()
		if err != nil {
			return err
		}
		if colon.Kind != TokenColon {
			return fmt.Errorf("json: expected colon")
		}
		value, err := t.Next()
		if err != nil {
			return err
		}
		if idx >= 0 {
			if seen[idx] {
				return fmt.Errorf("json: duplicate field %q", schema[idx].Name)
			}
			seen[idx] = true
			d.path[0] = schema[idx].Name
			if d.relevant(1) {
				if err := d.countValue(t, value, schema[idx].Type, plans[idx], 1); err != nil {
					return err
				}
			} else if err := SkipValue(t, value); err != nil {
				return err
			}
		} else if err := SkipValue(t, value); err != nil {
			return err
		}
		sep, err := t.Next()
		if err != nil {
			return err
		}
		if sep.Kind == TokenRBrace {
			break
		}
		if sep.Kind != TokenComma {
			return fmt.Errorf("json: expected comma or }")
		}
		key, err = t.Next()
		if err != nil {
			return err
		}
	}
	return ensureEOF(t)
}
func (d *typedDecoder) countValue(t *Tokenizer, first Token, typ types.LogicalType, plan *vectorPlan, depth int) error {
	if first.Kind == TokenNull {
		return nil
	}
	switch typ.ID {
	case types.List:
		if first.Kind != TokenLBracket {
			return SkipValue(t, first)
		}
		next, err := t.Next()
		if err != nil {
			return err
		}
		if next.Kind == TokenRBracket {
			return nil
		}
		for {
			addInstance(plan.children[0])
			if err := d.countValue(t, next, *typ.Child, plan.children[0], depth); err != nil {
				return err
			}
			sep, err := t.Next()
			if err != nil {
				return err
			}
			if sep.Kind == TokenRBracket {
				return nil
			}
			if sep.Kind != TokenComma {
				return fmt.Errorf("json: expected comma or ]")
			}
			next, err = t.Next()
			if err != nil {
				return err
			}
		}
	case types.Struct:
		if first.Kind != TokenLBrace {
			return SkipValue(t, first)
		}
		key, err := t.Next()
		if err != nil {
			return err
		}
		if key.Kind == TokenRBrace {
			return nil
		}
		seen := make([]bool, len(typ.Fields))
		for {
			if key.Kind != TokenString {
				return fmt.Errorf("json: expected struct key")
			}
			idx, err := d.findField(key.Raw, typ.Fields)
			if err != nil {
				return err
			}
			colon, err := t.Next()
			if err != nil {
				return err
			}
			if colon.Kind != TokenColon {
				return fmt.Errorf("json: expected colon")
			}
			value, err := t.Next()
			if err != nil {
				return err
			}
			if idx >= 0 {
				if seen[idx] {
					return fmt.Errorf("json: duplicate struct field %q", typ.Fields[idx].Name)
				}
				seen[idx] = true
				if depth >= len(d.path) {
					return fmt.Errorf("json: nesting too deep")
				}
				d.path[depth] = typ.Fields[idx].Name
				if d.relevant(depth + 1) {
					if err := d.countValue(t, value, typ.Fields[idx].Type, plan.children[idx], depth+1); err != nil {
						return err
					}
				} else if err := SkipValue(t, value); err != nil {
					return err
				}
			} else if err := SkipValue(t, value); err != nil {
				return err
			}
			sep, err := t.Next()
			if err != nil {
				return err
			}
			if sep.Kind == TokenRBrace {
				return nil
			}
			if sep.Kind != TokenComma {
				return fmt.Errorf("json: expected comma or }")
			}
			key, err = t.Next()
			if err != nil {
				return err
			}
		}
	default:
		return SkipValue(t, first)
	}
}

func (d *typedDecoder) decodeRecord(rec []byte, row int, schema []types.StructField, plans []*vectorPlan, cols []*vector.Vector) error {
	for _, c := range cols {
		c.SetNull(row)
	}
	t := NewTokenizer(rec)
	first, err := t.Next()
	if err != nil {
		return err
	}
	if first.Kind != TokenLBrace {
		return fmt.Errorf("json: record must be object")
	}
	seen := make([]bool, len(schema))
	key, err := t.Next()
	if err != nil {
		return err
	}
	if key.Kind == TokenRBrace {
		return ensureEOF(t)
	}
	for {
		if key.Kind != TokenString {
			return fmt.Errorf("json: expected object key")
		}
		idx, err := d.findField(key.Raw, schema)
		if err != nil {
			return err
		}
		colon, err := t.Next()
		if err != nil {
			return err
		}
		if colon.Kind != TokenColon {
			return fmt.Errorf("json: expected colon")
		}
		value, err := t.Next()
		if err != nil {
			return err
		}
		if idx >= 0 {
			if seen[idx] {
				return fmt.Errorf("json: duplicate field %q", schema[idx].Name)
			}
			seen[idx] = true
			d.path[0] = schema[idx].Name
			if d.relevant(1) {
				if err := d.decodeValue(t, value, schema[idx].Type, plans[idx], cols[idx], row, 1); err != nil {
					return fmt.Errorf("json: field %s: %w", schema[idx].Name, err)
				}
			} else if err := SkipValue(t, value); err != nil {
				return err
			}
		} else if err := SkipValue(t, value); err != nil {
			return err
		}
		sep, err := t.Next()
		if err != nil {
			return err
		}
		if sep.Kind == TokenRBrace {
			break
		}
		if sep.Kind != TokenComma {
			return fmt.Errorf("json: expected comma or }")
		}
		key, err = t.Next()
		if err != nil {
			return err
		}
	}
	return ensureEOF(t)
}

func (d *typedDecoder) decodeValue(t *Tokenizer, first Token, typ types.LogicalType, plan *vectorPlan, dst *vector.Vector, row, depth int) error {
	if first.Kind == TokenNull {
		dst.SetNull(row)
		return nil
	}
	switch typ.ID {
	case types.Boolean:
		if first.Kind == TokenTrue {
			dst.SetBool(row, true)
			return nil
		}
		if first.Kind == TokenFalse {
			dst.SetBool(row, false)
			return nil
		}
		return fmt.Errorf("expected BOOLEAN")
	case types.Integer:
		if first.Kind != TokenNumber {
			return fmt.Errorf("expected INTEGER")
		}
		v, err := parseInt64(first.Raw)
		if err != nil || v < math.MinInt32 || v > math.MaxInt32 {
			return fmt.Errorf("invalid INTEGER")
		}
		dst.SetInt32(row, int32(v))
		return nil
	case types.BigInt:
		if first.Kind != TokenNumber {
			return fmt.Errorf("expected BIGINT")
		}
		v, err := parseInt64(first.Raw)
		if err != nil {
			return fmt.Errorf("invalid BIGINT")
		}
		dst.SetInt64(row, v)
		return nil
	case types.Float:
		if first.Kind != TokenNumber {
			return fmt.Errorf("expected FLOAT")
		}
		v, err := strconv.ParseFloat(string(first.Raw), 32)
		if err != nil || math.IsInf(v, 0) {
			return fmt.Errorf("invalid FLOAT")
		}
		dst.SetFloat32(row, float32(v))
		return nil
	case types.Double:
		if first.Kind != TokenNumber {
			return fmt.Errorf("expected DOUBLE")
		}
		v, err := strconv.ParseFloat(string(first.Raw), 64)
		if err != nil || math.IsInf(v, 0) {
			return fmt.Errorf("invalid DOUBLE")
		}
		dst.SetFloat64(row, v)
		return nil
	case types.Varchar:
		var raw []byte
		switch first.Kind {
		case TokenString:
			decoded, err := d.decodedString(first.Raw)
			if err != nil {
				return err
			}
			raw = decoded
		case TokenNumber, TokenTrue, TokenFalse:
			raw = first.Raw
		case TokenLBrace, TokenLBracket:
			start := first.Start
			if err := SkipValue(t, first); err != nil {
				return err
			}
			raw = t.input[start:t.Position()]
		default:
			return fmt.Errorf("cannot coerce token %d to VARCHAR", first.Kind)
		}
		return dst.SetStringBytes(row, raw)
	case types.Struct:
		if first.Kind != TokenLBrace {
			return fmt.Errorf("expected STRUCT")
		}
		dst.SetValid(row)
		children := dst.Children()
		for _, c := range children {
			c.SetNull(row)
		}
		key, err := t.Next()
		if err != nil {
			return err
		}
		if key.Kind == TokenRBrace {
			return nil
		}
		seen := make([]bool, len(typ.Fields))
		for {
			if key.Kind != TokenString {
				return fmt.Errorf("expected struct key")
			}
			idx, err := d.findField(key.Raw, typ.Fields)
			if err != nil {
				return err
			}
			colon, err := t.Next()
			if err != nil {
				return err
			}
			if colon.Kind != TokenColon {
				return fmt.Errorf("expected colon")
			}
			value, err := t.Next()
			if err != nil {
				return err
			}
			if idx >= 0 {
				if seen[idx] {
					return fmt.Errorf("duplicate struct field")
				}
				seen[idx] = true
				d.path[depth] = typ.Fields[idx].Name
				if d.relevant(depth + 1) {
					if err := d.decodeValue(t, value, typ.Fields[idx].Type, plan.children[idx], children[idx], row, depth+1); err != nil {
						return err
					}
				} else if err := SkipValue(t, value); err != nil {
					return err
				}
			} else if err := SkipValue(t, value); err != nil {
				return err
			}
			sep, err := t.Next()
			if err != nil {
				return err
			}
			if sep.Kind == TokenRBrace {
				return nil
			}
			if sep.Kind != TokenComma {
				return fmt.Errorf("expected comma or }")
			}
			key, err = t.Next()
			if err != nil {
				return err
			}
		}
	case types.List:
		if first.Kind != TokenLBracket {
			return fmt.Errorf("expected LIST")
		}
		dst.SetValid(row)
		child := dst.Children()[0]
		cp := plan.children[0]
		start := cp.next
		next, err := t.Next()
		if err != nil {
			return err
		}
		if next.Kind == TokenRBracket {
			dst.SetListEntry(row, vector.ListEntry{Offset: uint32(start)})
			return nil
		}
		for {
			idx := cp.next
			if idx >= cp.count {
				return fmt.Errorf("json: LIST count mismatch")
			}
			if err := d.decodeValue(t, next, *typ.Child, cp, child, idx, depth); err != nil {
				return err
			}
			cp.next++
			sep, err := t.Next()
			if err != nil {
				return err
			}
			if sep.Kind == TokenRBracket {
				dst.SetListEntry(row, vector.ListEntry{Offset: uint32(start), Length: uint32(cp.next - start)})
				return nil
			}
			if sep.Kind != TokenComma {
				return fmt.Errorf("expected comma or ]")
			}
			next, err = t.Next()
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported type %d", typ.ID)
	}
}
func ensureEOF(t *Tokenizer) error {
	tok, err := t.Next()
	if err != nil {
		return err
	}
	if tok.Kind != TokenEOF {
		return fmt.Errorf("json: trailing token")
	}
	return nil
}
func parseInt64(raw []byte) (int64, error) {
	if len(raw) == 0 {
		return 0, fmt.Errorf("empty")
	}
	neg := false
	i := 0
	if raw[0] == '-' {
		neg = true
		i++
		if i == len(raw) {
			return 0, fmt.Errorf("invalid")
		}
	}
	var n uint64
	limit := uint64(math.MaxInt64)
	if neg {
		limit++
	}
	for ; i < len(raw); i++ {
		b := raw[i]
		if b < '0' || b > '9' {
			return 0, fmt.Errorf("not integer")
		}
		d := uint64(b - '0')
		if n > (limit-d)/10 {
			return 0, fmt.Errorf("overflow")
		}
		n = n*10 + d
	}
	if neg {
		if n == uint64(math.MaxInt64)+1 {
			return math.MinInt64, nil
		}
		return -int64(n), nil
	}
	return int64(n), nil
}

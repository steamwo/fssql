package expression

import (
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/semantics"
	"github.com/steamwo/fssql/internal/sql/binder"
)

type Kind uint8

const (
	Column Kind = iota
	ConstantInt
	ConstantBool
	Add
	Subtract
	Multiply
	Equal
	LessThan
	And
	Or
	Not
	Cast
	IsNull
	Case
	ScalarCall
	SQLPhase4
)

type BoundExpression struct {
	Kind Kind
	Type types.LogicalType
	ColumnIndex int
	IntValue int64
	BoolValue bool
	Children []*BoundExpression
	SQL *binder.BoundExpression
}

type Executor struct {
	Scope *memory.Scope
	Buffers *memory.BufferManager
	RuntimeConstants map[int]*vector.Vector
	Scalars ScalarRegistry
}

func NewExecutor(scope *memory.Scope, buffers *memory.BufferManager) *Executor { return &Executor{Scope: scope, Buffers: buffers} }

func (e *Executor) Execute(expr *BoundExpression, chunk *vector.DataChunk) (*vector.Vector, error) {
	if expr == nil { return nil, errors.New("nil expression") }
	switch expr.Kind {
	case Column:
		if expr.ColumnIndex < 0 || expr.ColumnIndex >= len(chunk.Columns) { return nil, fmt.Errorf("column %d out of range", expr.ColumnIndex) }
		return vector.NewDictionary(chunk.Columns[expr.ColumnIndex], vector.NewSelectionVector(chunk.Cardinality))
	case ConstantInt:
		v, err := vector.NewConstant(e.Scope, e.Buffers, expr.Type, chunk.Cardinality)
		if err != nil { return nil, err }
		v.SetInt64(0, expr.IntValue)
		return v, nil
	case ConstantBool:
		v, err := vector.NewConstant(e.Scope, e.Buffers, types.LogicalType{ID: types.Boolean}, chunk.Cardinality)
		if err != nil { return nil, err }
		v.SetBool(0, expr.BoolValue)
		return v, nil
	case Add, Subtract, Multiply, Equal, LessThan, And, Or:
		return e.binary(expr, chunk)
	case Not:
		a, err := e.Execute(expr.Children[0], chunk)
		if err != nil { return nil, err }
		defer a.Release()
		out, err := vector.NewFlat(e.Scope, e.Buffers, types.LogicalType{ID: types.Boolean}, chunk.Cardinality)
		if err != nil { return nil, err }
		for i := 0; i < chunk.Cardinality; i++ {
			x, ok := a.Bool(i)
			if !ok { out.SetNull(i) } else { out.SetBool(i, !x) }
		}
		return out, nil
	case Cast:
		return e.cast(expr, chunk)
	case IsNull:
		a, err := e.Execute(expr.Children[0], chunk)
		if err != nil { return nil, err }
		defer a.Release()
		out, err := vector.NewFlat(e.Scope, e.Buffers, types.LogicalType{ID: types.Boolean}, chunk.Cardinality)
		if err != nil { return nil, err }
		for i := 0; i < chunk.Cardinality; i++ { out.SetBool(i, !a.IsValid(i)) }
		return out, nil
	case Case:
		return e.caseExpr(expr, chunk)
	case ScalarCall:
		if expr.SQL == nil { return nil, errors.New("scalar call missing SQL expression") }
		if function.IsWindowID(expr.SQL.FunctionID) { return e.executePhase5Window(expr.SQL, chunk) }
		if isPhase4CastFunction(expr.SQL) { return e.executePhase4CastFunction(expr.SQL, chunk) }
		return e.executeScalarFunction(expr.SQL, chunk)
	case SQLPhase4:
		if expr.SQL == nil { return nil, errors.New("phase4 SQL node missing expression") }
		return e.executePhase4SQL(expr.SQL, chunk)
	default:
		return nil, fmt.Errorf("unsupported expression kind %d", expr.Kind)
	}
}

func (e *Executor) binary(expr *BoundExpression, c *vector.DataChunk) (*vector.Vector, error) {
	if len(expr.Children) != 2 { return nil, errors.New("binary expression requires two children") }
	a, err := e.Execute(expr.Children[0], c)
	if err != nil { return nil, err }
	defer a.Release()
	b, err := e.Execute(expr.Children[1], c)
	if err != nil { return nil, err }
	defer b.Release()
	outType := expr.Type
	if expr.Kind == Equal || expr.Kind == LessThan || expr.Kind == And || expr.Kind == Or { outType = types.LogicalType{ID: types.Boolean} }
	out, err := vector.NewFlat(e.Scope, e.Buffers, outType, c.Cardinality)
	if err != nil { return nil, err }
	for i := 0; i < c.Cardinality; i++ {
		switch expr.Kind {
		case And, Or:
			x, xok := a.Bool(i)
			y, yok := b.Bool(i)
			if expr.Kind == And {
				if (xok && !x) || (yok && !y) { out.SetBool(i, false) } else if !xok || !yok { out.SetNull(i) } else { out.SetBool(i, true) }
			} else {
				if (xok && x) || (yok && y) { out.SetBool(i, true) } else if !xok || !yok { out.SetNull(i) } else { out.SetBool(i, false) }
			}
		default:
			x, xok := a.Int64(i)
			y, yok := b.Int64(i)
			if !xok || !yok { out.SetNull(i); continue }
			switch expr.Kind {
			case Add:
				z, er := semantics.AddInt64(x, y); if er != nil { out.Release(); return nil, er }; out.SetInt64(i, z)
			case Subtract:
				z, er := semantics.SubInt64(x, y); if er != nil { out.Release(); return nil, er }; out.SetInt64(i, z)
			case Multiply:
				z, er := semantics.MulInt64(x, y); if er != nil { out.Release(); return nil, er }; out.SetInt64(i, z)
			case Equal:
				out.SetBool(i, semantics.EqualInt64(x, y))
			case LessThan:
				out.SetBool(i, semantics.CompareInt64(x, y) < 0)
			}
		}
	}
	return out, nil
}

func (e *Executor) cast(expr *BoundExpression, c *vector.DataChunk) (*vector.Vector, error) {
	if len(expr.Children) != 1 { return nil, errors.New("cast requires one child") }
	a, err := e.Execute(expr.Children[0], c)
	if err != nil { return nil, err }
	defer a.Release()
	out, err := vector.NewFlat(e.Scope, e.Buffers, expr.Type, c.Cardinality)
	if err != nil { return nil, err }
	for i := 0; i < c.Cardinality; i++ {
		if !a.IsValid(i) { out.SetNull(i); continue }
		switch {
		case a.Type.ID == types.BigInt && expr.Type.ID == types.Double:
			x, _ := a.Int64(i); out.SetFloat64(i, float64(x))
		case a.Type.ID == types.Double && expr.Type.ID == types.BigInt:
			x, _ := a.Float64(i); out.SetInt64(i, int64(x))
		default:
			out.Release(); return nil, fmt.Errorf("unsupported cast %v -> %v", a.Type.ID, expr.Type.ID)
		}
	}
	return out, nil
}

func (e *Executor) caseExpr(expr *BoundExpression, c *vector.DataChunk) (*vector.Vector, error) {
	if len(expr.Children) != 3 { return nil, errors.New("CASE requires condition, then, else") }
	cond, err := e.Execute(expr.Children[0], c)
	if err != nil { return nil, err }
	defer cond.Release()
	tv, err := e.Execute(expr.Children[1], c)
	if err != nil { return nil, err }
	defer tv.Release()
	fv, err := e.Execute(expr.Children[2], c)
	if err != nil { return nil, err }
	defer fv.Release()
	out, err := vector.NewFlat(e.Scope, e.Buffers, expr.Type, c.Cardinality)
	if err != nil { return nil, err }
	for i := 0; i < c.Cardinality; i++ {
		b, ok := cond.Bool(i)
		src := fv
		if ok && b { src = tv }
		x, xok := src.Int64(i)
		if !xok { out.SetNull(i) } else { out.SetInt64(i, x) }
	}
	return out, nil
}

func (e *Executor) Filter(expr *BoundExpression, c *vector.DataChunk) (vector.SelectionVector, error) {
	v, err := e.Execute(expr, c)
	if err != nil { return vector.SelectionVector{}, err }
	defer v.Release()
	idx := make([]uint32, 0, c.Cardinality)
	for i := 0; i < c.Cardinality; i++ {
		b, ok := v.Bool(i)
		if ok && b { idx = append(idx, uint32(i)) }
	}
	return vector.SelectionFrom(idx), nil
}

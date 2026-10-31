package scan

import "github.com/steamwo/fssql/internal/core/types"

type Truth uint8

const (
	AlwaysFalse Truth = iota
	MaybeTrue
	AlwaysTrue
)

func And(a, b Truth) Truth {
	if a == AlwaysFalse || b == AlwaysFalse {
		return AlwaysFalse
	}
	if a == AlwaysTrue && b == AlwaysTrue {
		return AlwaysTrue
	}
	return MaybeTrue
}

type CompareOp uint8

const (
	Equal CompareOp = iota
	NotEqual
	LessThan
	LessEqual
	GreaterThan
	GreaterEqual
	IsNull
	IsNotNull
)

type Literal struct {
	Type    types.LogicalType
	Null    bool
	Bool    bool
	Int64   int64
	Float64 float64
	String  string
}

type Filter struct {
	Column string
	Op     CompareOp
	Value  Literal
}

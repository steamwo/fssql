package expression

import (
	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"testing"
)

func setup(t *testing.T) (*memory.Manager, *memory.Scope, *memory.BufferManager, *vector.DataChunk) {
	m := memory.NewManager(1 << 20)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	v, _ := vector.NewFlat(s, bm, types.LogicalType{ID: types.BigInt}, 4)
	for i, x := range []int64{1, 2, 3, 4} {
		v.SetInt64(i, x)
	}
	v.SetNull(2)
	c, _ := vector.NewDataChunk(v)
	return m, s, bm, c
}
func col() *BoundExpression {
	return &BoundExpression{Kind: Column, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: 0}
}
func TestArithmeticComparisonFilter(t *testing.T) {
	_, s, bm, c := setup(t)
	e := NewExecutor(s, bm)
	add := &BoundExpression{Kind: Add, Type: types.LogicalType{ID: types.BigInt}, Children: []*BoundExpression{col(), {Kind: ConstantInt, Type: types.LogicalType{ID: types.BigInt}, IntValue: 10}}}
	v, err := e.Execute(add, c)
	if err != nil {
		t.Fatal(err)
	}
	if x, _ := v.Int64(1); x != 12 {
		t.Fatal(x)
	}
	if _, ok := v.Int64(2); ok {
		t.Fatal("null")
	}
	lt := &BoundExpression{Kind: LessThan, Children: []*BoundExpression{col(), {Kind: ConstantInt, Type: types.LogicalType{ID: types.BigInt}, IntValue: 4}}}
	sel, err := e.Filter(lt, c)
	if err != nil {
		t.Fatal(err)
	}
	if sel.Len() != 2 || sel.Get(0) != 0 || sel.Get(1) != 1 {
		t.Fatalf("selection %d", sel.Len())
	}
	v.Release()
	c.Release()
}
func TestBooleanNullSemantics(t *testing.T) {
	_, s, bm, c := setup(t)
	e := NewExecutor(s, bm)
	isnull := &BoundExpression{Kind: IsNull, Children: []*BoundExpression{col()}}
	v, err := e.Execute(isnull, c)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := v.Bool(2)
	if !b {
		t.Fatal("is null")
	}
	v.Release()
	c.Release()
}
func TestCastAndCase(t *testing.T) {
	_, s, bm, c := setup(t)
	e := NewExecutor(s, bm)
	cast := &BoundExpression{Kind: Cast, Type: types.LogicalType{ID: types.Double}, Children: []*BoundExpression{col()}}
	v, err := e.Execute(cast, c)
	if err != nil {
		t.Fatal(err)
	}
	x, _ := v.Float64(1)
	if x != 2 {
		t.Fatal(x)
	}
	v.Release()
	casee := &BoundExpression{Kind: Case, Type: types.LogicalType{ID: types.BigInt}, Children: []*BoundExpression{{Kind: LessThan, Children: []*BoundExpression{col(), {Kind: ConstantInt, Type: types.LogicalType{ID: types.BigInt}, IntValue: 3}}}, {Kind: ConstantInt, Type: types.LogicalType{ID: types.BigInt}, IntValue: 7}, {Kind: ConstantInt, Type: types.LogicalType{ID: types.BigInt}, IntValue: 9}}}
	v, err = e.Execute(casee, c)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := v.Int64(0)
	b, _ := v.Int64(3)
	if a != 7 || b != 9 {
		t.Fatalf("%d %d", a, b)
	}
	v.Release()
	c.Release()
}
func TestSelectionExecutionEqualsMaterialized(t *testing.T) {
	_, s, bm, c := setup(t)
	e := NewExecutor(s, bm)
	pred := &BoundExpression{Kind: LessThan, Children: []*BoundExpression{col(), {Kind: ConstantInt, Type: types.LogicalType{ID: types.BigInt}, IntValue: 4}}}
	sel, err := e.Filter(pred, c)
	if err != nil {
		t.Fatal(err)
	}
	got := []int64{}
	for i := 0; i < sel.Len(); i++ {
		x, _ := c.Columns[0].Int64(sel.Get(i))
		got = append(got, x)
	}
	want := []int64{}
	for i := 0; i < c.Cardinality; i++ {
		x, ok := c.Columns[0].Int64(i)
		if ok && x < 4 {
			want = append(want, x)
		}
	}
	if len(got) != len(want) {
		t.Fatal("length")
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatal("mismatch")
		}
	}
	c.Release()
}

func TestNumericKernelAllocationsDoNotScalePerRow(t *testing.T) {
	measure := func(n int) float64 {
		m := memory.NewManager(8 << 20)
		s := m.NewScope()
		bm := memory.NewBufferManager(nil)
		v, _ := vector.NewFlat(s, bm, types.LogicalType{ID: types.BigInt}, n)
		for i := 0; i < n; i++ {
			v.SetInt64(i, int64(i))
		}
		c, _ := vector.NewDataChunk(v)
		e := NewExecutor(s, bm)
		expr := &BoundExpression{Kind: Add, Type: types.LogicalType{ID: types.BigInt}, Children: []*BoundExpression{{Kind: Column, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: 0}, {Kind: ConstantInt, Type: types.LogicalType{ID: types.BigInt}, IntValue: 1}}}
		allocs := testing.AllocsPerRun(50, func() {
			out, err := e.Execute(expr, c)
			if err != nil {
				panic(err)
			}
			out.Release()
		})
		c.Release()
		if m.Used() != 0 {
			t.Fatalf("leak %d", m.Used())
		}
		return allocs
	}
	small := measure(64)
	large := measure(vector.DefaultVectorSize)
	if large > small+2 {
		t.Fatalf("allocations scale with rows: small=%.1f large=%.1f", small, large)
	}
}

func BenchmarkNumericAdd2048(b *testing.B) {
	m := memory.NewManager(64 << 20)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	v, _ := vector.NewFlat(s, bm, types.LogicalType{ID: types.BigInt}, vector.DefaultVectorSize)
	for i := 0; i < v.Count(); i++ {
		v.SetInt64(i, int64(i))
	}
	c, _ := vector.NewDataChunk(v)
	e := NewExecutor(s, bm)
	expr := &BoundExpression{Kind: Add, Type: types.LogicalType{ID: types.BigInt}, Children: []*BoundExpression{{Kind: Column, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: 0}, {Kind: ConstantInt, Type: types.LogicalType{ID: types.BigInt}, IntValue: 1}}}
	b.ReportAllocs()
	b.SetBytes(int64(vector.DefaultVectorSize * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := e.Execute(expr, c)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
	b.StopTimer()
	c.Release()
	if m.Used() != 0 {
		b.Fatalf("leak %d", m.Used())
	}
}

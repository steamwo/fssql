package aggregate

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/semantics"
)

func TestGlobalFixedMatchesScalarStates(t *testing.T) {
	decimal64, _ := types.NewDecimal(12, 2)
	decimal128, _ := types.NewDecimal(38, 4)
	inputTypes := []types.LogicalType{{ID: types.Boolean}, {ID: types.TinyInt}, {ID: types.SmallInt}, {ID: types.Integer}, {ID: types.BigInt}, {ID: types.HugeInt}, {ID: types.Float}, {ID: types.Double}, {ID: types.DateType}, {ID: types.TimestampType}, decimal64, decimal128}
	for _, typ := range inputTypes {
		for _, kind := range []vector.VectorType{vector.Flat, vector.Constant, vector.Dictionary} {
			m, scope, bm := aggEnv(1 << 20)
			var base *vector.Vector
			var err error
			if kind == vector.Constant {
				base, err = vector.NewConstant(scope, bm, typ, 8)
			} else {
				base, err = vector.NewFlat(scope, bm, typ, 8)
			}
			if err != nil {
				t.Fatal(err)
			}
			for row := 0; row < 8; row++ {
				x := int64(row*7 - 20)
				switch typ.ID {
				case types.Boolean:
					base.SetBool(row, row%2 != 0)
				case types.TinyInt:
					base.SetInt8(row, int8(x))
				case types.SmallInt:
					base.SetInt16(row, int16(x))
				case types.Integer, types.DateType:
					base.SetInt32(row, int32(x))
				case types.Float, types.Double:
					value := []float64{math.Copysign(0, -1), 0, -3.25, math.NaN(), math.Inf(1), math.Inf(-1), 5, 7}[row]
					if typ.ID == types.Float {
						base.SetFloat32(row, float32(value))
					} else {
						base.SetFloat64(row, value)
					}
				case types.HugeInt:
					base.SetInt128(row, types.Int128FromInt64(x))
				case types.Decimal:
					if typ.Decimal.Width > 18 {
						base.SetInt128(row, types.Int128FromInt64(x))
					} else {
						base.SetInt64(row, x)
					}
				default:
					base.SetInt64(row, x)
				}
			}
			if kind != vector.Constant {
				base.SetNull(6)
			}
			input := base
			if kind == vector.Dictionary {
				input, err = vector.NewDictionary(base, vector.SelectionFrom([]uint32{6, 4, 3, 2, 0, 7, 1, 5}))
				if err != nil {
					t.Fatal(err)
				}
			}
			chunk := &vector.DataChunk{Columns: []*vector.Vector{input}, Cardinality: 8}
			var a HashAggregate
			for _, aggKind := range []Kind{Count, Sum, Avg, Min, Max} {
				bound, err := Bind([]types.LogicalType{typ}, []Spec{{Kind: aggKind, Input: 0}})
				if err != nil { // SUM/AVG intentionally exclude nonnumeric types.
					continue
				}
				var batch, scalar [aggregateStateWidth]byte
				for iteration := 0; iteration < 2; iteration++ {
					handled, err := a.updateGlobalFixed(context.Background(), batch[:], bound[0], chunk)
					if !handled || err != nil {
						t.Fatalf("type=%v vector=%v agg=%v handled=%v err=%v", typ, kind, aggKind, handled, err)
					}
					for row := 0; row < 8; row++ {
						if err := a.updateState(scalar[:], bound[0], chunk, row); err != nil {
							t.Fatal(err)
						}
					}
					if !bytes.Equal(batch[:], scalar[:]) {
						t.Fatalf("type=%v vector=%v agg=%v batch=%x scalar=%x", typ, kind, aggKind, batch, scalar)
					}
				}
			}
			if input != base {
				input.Release()
			}
			base.Release()
			if m.Used() != 0 {
				t.Fatalf("managed leak=%d", m.Used())
			}
		}
	}
}

func TestGlobalFixedRetainsOverflowAndNullSemantics(t *testing.T) {
	m, scope, bm := aggEnv(1 << 20)
	defer func() {
		if m.Used() != 0 {
			t.Fatalf("managed leak=%d", m.Used())
		}
	}()
	v, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.HugeInt}, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Release()
	v.SetInt128(0, types.Int128FromInt64(1))
	chunk := &vector.DataChunk{Columns: []*vector.Vector{v}, Cardinality: 1}
	var a HashAggregate
	bound, _ := Bind([]types.LogicalType{v.Type}, []Spec{{Kind: Sum, Input: 0}, {Kind: Avg, Input: 0}})
	var state [aggregateStateWidth]byte
	binary.LittleEndian.PutUint64(state[16:24], math.MaxUint64)
	binary.LittleEndian.PutUint64(state[24:32], math.MaxInt64)
	if _, err := a.updateGlobalFixed(context.Background(), state[:], bound[0], chunk); err != semantics.ErrOverflow {
		t.Fatalf("SUM overflow=%v", err)
	}
	state = [aggregateStateWidth]byte{}
	binary.LittleEndian.PutUint64(state[8:16], math.MaxUint64)
	if _, err := a.updateGlobalFixed(context.Background(), state[:], bound[1], chunk); err != semantics.ErrOverflow {
		t.Fatalf("AVG overflow=%v", err)
	}
	state = [aggregateStateWidth]byte{}
	v.SetNull(0)
	if _, err := a.updateGlobalFixed(context.Background(), state[:], bound[0], chunk); err != nil || state[0] != 0 {
		t.Fatalf("all NULL state=%x err=%v", state, err)
	}
}

func BenchmarkGlobalFixedReduction(b *testing.B) {
	_, scope, bm := aggEnv(1 << 20)
	v, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.Double}, vector.DefaultVectorSize)
	if err != nil {
		b.Fatal(err)
	}
	defer v.Release()
	for row := 0; row < v.Count(); row++ {
		v.SetFloat64(row, float64(row)/10)
	}
	chunk := &vector.DataChunk{Columns: []*vector.Vector{v}, Cardinality: v.Count()}
	bound, _ := Bind([]types.LogicalType{v.Type}, []Spec{{Kind: Sum, Input: 0}})
	var a HashAggregate
	for _, batch := range []bool{false, true} {
		name := "scalar"
		if batch {
			name = "batch"
		}
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(v.Count() * 8))
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				var state [aggregateStateWidth]byte
				if batch {
					if _, err := a.updateGlobalFixed(context.Background(), state[:], bound[0], chunk); err != nil {
						b.Fatal(err)
					}
				} else {
					for row := 0; row < v.Count(); row++ {
						if err := a.updateState(state[:], bound[0], chunk, row); err != nil {
							b.Fatal(err)
						}
					}
				}
			}
		})
	}
}

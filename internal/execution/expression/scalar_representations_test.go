package expression

import (
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func TestScalarLengthVectorRepresentations(t *testing.T) {
	varchar := types.LogicalType{ID: types.Varchar}
	bigint := types.LogicalType{ID: types.BigInt}
	expr := &binder.BoundExpression{
		Kind:         binder.BoundFunction,
		Type:         bigint,
		FunctionID:   function.LengthID,
		FunctionKind: function.Scalar,
		Children: []*binder.BoundExpression{{
			Kind:        binder.BoundColumn,
			Type:        varchar,
			ColumnIndex: 0,
		}},
	}

	cases := []struct {
		name string
		makeVector func(*memory.Scope, *memory.BufferManager) (*vector.Vector, *vector.Vector, []int64)
	}{
		{
			name: "flat",
			makeVector: func(scope *memory.Scope, bm *memory.BufferManager) (*vector.Vector, *vector.Vector, []int64) {
				v, err := vector.NewFlat(scope, bm, varchar, 3)
				if err != nil { t.Fatal(err) }
				for i, value := range []string{"A", "ββ", "😊😊😊"} {
					if err := v.SetString(i, value); err != nil { t.Fatal(err) }
				}
				return v, nil, []int64{1, 2, 3}
			},
		},
		{
			name: "constant",
			makeVector: func(scope *memory.Scope, bm *memory.BufferManager) (*vector.Vector, *vector.Vector, []int64) {
				v, err := vector.NewConstant(scope, bm, varchar, 3)
				if err != nil { t.Fatal(err) }
				if err := v.SetString(0, "ββ"); err != nil { t.Fatal(err) }
				return v, nil, []int64{2, 2, 2}
			},
		},
		{
			name: "dictionary",
			makeVector: func(scope *memory.Scope, bm *memory.BufferManager) (*vector.Vector, *vector.Vector, []int64) {
				base, err := vector.NewFlat(scope, bm, varchar, 3)
				if err != nil { t.Fatal(err) }
				for i, value := range []string{"A", "ββ", "😊😊😊"} {
					if err := base.SetString(i, value); err != nil { t.Fatal(err) }
				}
				dict, err := vector.NewDictionary(base, vector.SelectionFrom([]uint32{2, 0, 1}))
				if err != nil { t.Fatal(err) }
				return dict, base, []int64{3, 1, 2}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			manager := memory.NewManager(1 << 20)
			scope := manager.NewScope()
			bm := memory.NewBufferManager(nil)
			input, retainedBase, want := tc.makeVector(scope, bm)
			chunk, err := vector.NewDataChunk(input)
			if err != nil { t.Fatal(err) }
			executor := NewExecutor(scope, bm)
			out, err := executor.ExecuteSQL(expr, chunk)
			if err != nil { t.Fatal(err) }
			for row, expected := range want {
				got, ok := out.Int64(row)
				if !ok || got != expected { t.Fatalf("row %d length=%d valid=%v want=%d", row, got, ok, expected) }
			}
			out.Release()
			chunk.Release()
			if retainedBase != nil { retainedBase.Release() }
			if err := scope.Close(); err != nil { t.Fatal(err) }
			if manager.Used() != 0 { t.Fatalf("managed memory leaked: %d", manager.Used()) }
		})
	}
}

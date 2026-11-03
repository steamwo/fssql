package aggregate

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
)

func TestGlobalBatchAggregateAcrossChunksAndNulls(t *testing.T) {
	m, scope, bm := aggEnv(1 << 20)
	a, err := NewHashAggregate(scope, bm, []types.LogicalType{{ID: types.BigInt}}, nil, []Spec{{Kind: CountStar}, {Kind: Count, Input: 0}, {Kind: Sum, Input: 0}, {Kind: Avg, Input: 0}, {Kind: Min, Input: 0}, {Kind: Max, Input: 0}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		values []int64
		nulls  map[int]bool
	}{{[]int64{1, 2, 3}, nil}, {[]int64{4, 0, 0}, map[int]bool{1: true, 2: true}}} {
		// Reuse the shared fixture's second vector as this aggregate's input.
		chunk := makeBigIntChunk(t, scope, bm, []int64{0, 0, 0}, tc.values, tc.nulls)
		group := chunk.Columns[0]
		chunk.Columns = chunk.Columns[1:]
		if err := a.Consume(context.Background(), chunk); err != nil {
			t.Fatal(err)
		}
		chunk.Release()
		group.Release()
	}
	out, err := a.Finalize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for col, want := range map[int]int64{0: 6, 1: 4, 4: 1, 5: 4} {
		if got, valid := out.Columns[col].Int64(0); !valid || got != want {
			t.Fatalf("column=%d value=%d valid=%v", col, got, valid)
		}
	}
	if got, valid := out.Columns[2].Int128(0); !valid || got != types.Int128FromInt64(10) {
		t.Fatalf("sum=%v", got)
	}
	if got, valid := out.Columns[3].Float64(0); !valid || got != 2.5 {
		t.Fatalf("avg=%v", got)
	}
	out.Release()
	a.Release()
	if m.Used() != 0 {
		t.Fatalf("managed leak=%d", m.Used())
	}
}

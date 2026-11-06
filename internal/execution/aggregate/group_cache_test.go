package aggregate

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

// TestIntegerGroupCacheRetainsExactKeys 验证碰撞、重复键、NULL 与跨批次状态不因缓存改变。
func TestIntegerGroupCacheRetainsExactKeys(t *testing.T) {
	manager, scope, buffers := aggEnv(8 << 20)
	agg, err := NewHashAggregate(scope, buffers, []types.LogicalType{{ID: types.BigInt}}, []int{0}, []Spec{{Kind: CountStar}}, 8)
	if err != nil {
		t.Fatal(err)
	}
	agg.groupCacheBudget = 1 << 20
	want := map[int64]int64{}
	var nulls int64
	for batch := 0; batch < 8; batch++ {
		v, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.BigInt}, 1025)
		if err != nil {
			t.Fatal(err)
		}
		for row := 0; row < 1025; row++ {
			key := int64((row*104729 + batch) % 1025)
			if row%17 == 0 {
				v.SetNull(row)
				nulls++
			} else {
				v.SetInt64(row, key)
				want[key]++
			}
		}
		chunk, err := vector.NewDataChunk(v)
		if err != nil {
			t.Fatal(err)
		}
		err = agg.Consume(context.Background(), chunk)
		chunk.Release()
		if err != nil {
			t.Fatal(err)
		}
	}
	if agg.groupCache == nil {
		t.Fatal("expected integer cache to be used")
	}
	err = agg.Emit(context.Background(), func(chunk *vector.DataChunk) error {
		defer chunk.Release()
		for row := 0; row < chunk.Cardinality; row++ {
			key, valid := chunk.Columns[0].Int64(row)
			count, _ := chunk.Columns[1].Int64(row)
			if valid {
				if count != want[key] {
					t.Fatalf("key %d count=%d want %d", key, count, want[key])
				}
				delete(want, key)
			} else {
				if count != nulls {
					t.Fatalf("NULL count=%d want %d", count, nulls)
				}
				nulls = 0
			}
		}
		return nil
	})
	if err != nil || len(want) != 0 || nulls != 0 {
		t.Fatalf("remaining groups=%d NULL=%d err=%v", len(want), nulls, err)
	}
	agg.Release()
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

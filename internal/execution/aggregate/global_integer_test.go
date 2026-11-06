package aggregate

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/semantics"
)

// TestGlobalFlatIntegerRetainsOverflowAndCancellation 验证新归约内核的极值、溢出与取消原子性。
func TestGlobalFlatIntegerRetainsOverflowAndCancellation(t *testing.T) {
	manager, scope, buffers := aggEnv(1 << 20)
	v, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.BigInt}, 1)
	if err != nil {
		t.Fatal(err)
	}
	v.SetInt64(0, 1)
	chunk := &vector.DataChunk{Columns: []*vector.Vector{v}, Cardinality: 1}
	bound, err := Bind([]types.LogicalType{v.Type}, []Spec{{Kind: Sum, Input: 0}, {Kind: Avg, Input: 0}, {Kind: Count, Input: 0}})
	if err != nil {
		t.Fatal(err)
	}
	var agg HashAggregate
	var state [aggregateStateWidth]byte
	binary.LittleEndian.PutUint64(state[16:24], math.MaxUint64)
	binary.LittleEndian.PutUint64(state[24:32], math.MaxInt64)
	if handled, err := agg.updateGlobalFlatInteger(context.Background(), state[:], bound[0], chunk); !handled || !errors.Is(err, semantics.ErrOverflow) {
		t.Fatalf("SUM handled=%v overflow=%v", handled, err)
	}
	state = [aggregateStateWidth]byte{}
	binary.LittleEndian.PutUint64(state[8:16], math.MaxUint64)
	if _, err := agg.updateGlobalFlatInteger(context.Background(), state[:], bound[1], chunk); !errors.Is(err, semantics.ErrOverflow) {
		t.Fatalf("AVG count overflow=%v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	state = [aggregateStateWidth]byte{}
	if _, err := agg.updateGlobalFlatInteger(canceled, state[:], bound[2], chunk); !errors.Is(err, context.Canceled) || !bytes.Equal(state[:], make([]byte, aggregateStateWidth)) {
		t.Fatalf("canceled COUNT error=%v state=%x", err, state)
	}
	v.Release()
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

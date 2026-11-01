package vector

import (
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
)

func TestPhase1WidthCorrectNumericAccessors(t *testing.T) {
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)

	ints, err := NewFlat(scope, bm, types.LogicalType{ID: types.Integer}, 2)
	if err != nil {
		t.Fatal(err)
	}
	ints.SetInt32(0, -7)
	ints.SetInt32(1, 42)
	if v, ok := ints.Int32(0); !ok || v != -7 {
		t.Fatalf("int[0]=%d ok=%v", v, ok)
	}
	if v, ok := ints.Int32(1); !ok || v != 42 {
		t.Fatalf("int[1]=%d ok=%v", v, ok)
	}

	floats, err := NewFlat(scope, bm, types.LogicalType{ID: types.Float}, 2)
	if err != nil {
		ints.Release()
		t.Fatal(err)
	}
	floats.SetFloat32(0, 1.25)
	floats.SetFloat32(1, -2.5)
	if v, ok := floats.Float32(0); !ok || v != 1.25 {
		t.Fatalf("float[0]=%v ok=%v", v, ok)
	}
	if v, ok := floats.Float32(1); !ok || v != -2.5 {
		t.Fatalf("float[1]=%v ok=%v", v, ok)
	}

	ints.Release()
	floats.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed bytes leaked: %d", scope.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

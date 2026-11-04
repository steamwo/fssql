package vector

import (
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
)

func TestOwnedDictionaryBytesViewLifetime(t *testing.T) {
	m := memory.NewManager(1 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	base, err := NewFlat(scope, bm, types.BlobType(), 3)
	if err != nil {
		t.Fatal(err)
	}
	_ = base.SetStringBytes(0, []byte{'a', 0, 'b'})
	_ = base.SetString(1, "a long external string value")
	base.SetNull(2)
	child, err := NewOwnedDictionary(scope, base, SelectionFrom([]uint32{2, 1, 0}))
	if err != nil {
		t.Fatal(err)
	}
	outer, err := NewOwnedDictionary(scope, child, SelectionFrom([]uint32{2, 1, 0}))
	if err != nil {
		t.Fatal(err)
	}
	view, ok := outer.BorrowBytes()
	if !ok {
		t.Fatal("missing bytes view")
	}
	for row := 0; row < 3; row++ {
		got, valid := view.Value(row)
		want, wantValid := outer.Bytes(row)
		if valid != wantValid || string(got) != string(want) {
			t.Fatalf("row=%d bytes=%q valid=%v", row, got, valid)
		}
	}
	if _, valid := view.Value(-1); valid {
		t.Fatal("negative row accepted")
	}
	outer.Release()
	outer.Release()
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
	if m.Used() != 0 {
		t.Fatalf("leak=%d", m.Used())
	}
}

func TestOwnedDictionaryFailureRetainsChild(t *testing.T) {
	m := memory.NewManager(24)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	base, err := NewFlat(scope, bm, types.LogicalType{ID: types.BigInt}, 3)
	if err != nil {
		t.Fatal(err)
	}
	base.SetInt64(0, 17)
	if _, err := NewOwnedDictionary(scope, base, SelectionFrom([]uint32{0})); !errors.Is(err, memory.ErrLimitExceeded) {
		t.Fatalf("error=%v", err)
	}
	if x, ok := base.Int64(0); !ok || x != 17 {
		t.Fatal("failed transfer released child")
	}
	base.Release()
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

package vector

import (
	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"testing"
)

func TestHugeIntCopyDictionaryAndSerialization(t *testing.T) {
	mm := memory.NewManager(1 << 20)
	s := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	v, err := NewFlat(s, bm, types.LogicalType{ID: types.HugeInt}, 3)
	if err != nil {
		t.Fatal(err)
	}
	vals := []types.Int128{{Lo: 0, Hi: -1}, {Lo: 1 << 63, Hi: 0}, {Lo: ^uint64(0), Hi: 42}}
	for i, x := range vals {
		v.SetInt128(i, x)
	}
	d, err := NewDictionary(v, SelectionFrom([]uint32{2, 0, 1}))
	if err != nil {
		t.Fatal(err)
	}
	dst, err := NewFlat(s, bm, types.LogicalType{ID: types.HugeInt}, 3)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := CopyValue(d, i, dst, i); err != nil {
			t.Fatal(err)
		}
	}
	c, err := NewDataChunk(dst)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := SerializeChunk(c, s, bm)
	if err != nil {
		t.Fatal(err)
	}
	round, err := DeserializeChunk(raw.Bytes(), s, bm)
	if err != nil {
		t.Fatal(err)
	}
	if round.Columns[0].Type.ID != types.HugeInt {
		t.Fatalf("type=%v", round.Columns[0].Type.ID)
	}
	want := []types.Int128{vals[2], vals[0], vals[1]}
	for i, x := range want {
		got, ok := round.Columns[0].Int128(i)
		if !ok || got != x {
			t.Fatalf("row %d=%+v,%v want %+v", i, got, ok, x)
		}
	}
	round.Release()
	raw.Release()
	c.Release()
	d.Release()
	v.Release()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if mm.Used() != 0 {
		t.Fatalf("leak=%d", mm.Used())
	}
}

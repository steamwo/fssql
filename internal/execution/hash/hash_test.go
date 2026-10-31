package hash

import (
	"encoding/binary"
	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/semantics"
	"math/rand"
	"testing"
)

func henv() (*memory.Manager, *memory.Scope, *memory.BufferManager) {
	m := memory.NewManager(8 << 20)
	s := m.NewScope()
	return m, s, memory.NewBufferManager(nil)
}
func TestEqualsImpliesHash(t *testing.T) {
	_, s, bm := henv()
	v, _ := vector.NewFlat(s, bm, types.LogicalType{ID: types.BigInt}, 1000)
	rng := rand.New(rand.NewSource(1))
	vals := make([]int64, 1000)
	for i := range vals {
		vals[i] = int64(rng.Intn(10))
		v.SetInt64(i, vals[i])
	}
	out := make([]uint64, len(vals))
	if err := VectorHash(v, out); err != nil {
		t.Fatal(err)
	}
	for i := range vals {
		for j := 0; j < 20; j++ {
			k := rng.Intn(len(vals))
			if semantics.EqualInt64(vals[i], vals[k]) && out[i] != out[k] {
				t.Fatalf("hash property")
			}
		}
	}
	v.Release()
}
func TestTupleSerializeRoundTrip(t *testing.T) {
	m, s, bm := henv()
	layout, err := NewRowLayout([]types.LogicalType{{ID: types.BigInt}, {ID: types.Boolean}})
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewTupleDataCollection(s, bm, layout.RowWidth, 1)
	if err != nil {
		t.Fatal(err)
	}
	row := make([]byte, layout.RowWidth)
	binary.LittleEndian.PutUint64(row[:8], 42)
	row[8] = 1
	for i := 0; i < 100; i++ {
		if err := c.Append(row); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := c.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	c.Release()
	d, err := Deserialize(raw.Bytes(), s, bm)
	raw.Release()
	if err != nil {
		t.Fatal(err)
	}
	if d.Count() != 100 || binary.LittleEndian.Uint64(d.Row(99)[:8]) != 42 {
		t.Fatal("roundtrip")
	}
	d.Release()
	if m.Used() != 0 {
		t.Fatalf("leak %d", m.Used())
	}
}
func TestHashTablePrimitive(t *testing.T) {
	m, s, bm := henv()
	h, err := NewHashTable(s, bm, 2048)
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 1000; i++ {
		if err := h.Insert(i, i*3); err != nil {
			t.Fatal(err)
		}
	}
	for i := int64(0); i < 1000; i++ {
		v, ok := h.Lookup(i)
		if !ok || v != i*3 {
			t.Fatalf("%d %d %v", i, v, ok)
		}
	}
	if _, ok := h.Lookup(9999); ok {
		t.Fatal("unexpected")
	}
	h.Release()
	if m.Used() != 0 {
		t.Fatalf("leak %d", m.Used())
	}
}
func TestRadixPartitionComplete(t *testing.T) {
	p, err := NewRadixPartitioner(4)
	if err != nil {
		t.Fatal(err)
	}
	seen := make([]bool, p.Count())
	for i := 0; i < 100000; i++ {
		part := p.Partition(semantics.HashInt64(int64(i)))
		if part < 0 || part >= p.Count() {
			t.Fatal(part)
		}
		seen[part] = true
	}
	for i, v := range seen {
		if !v {
			t.Fatalf("partition %d empty", i)
		}
	}
}
func BenchmarkVectorHashInt64(b *testing.B) {
	m, s, bm := henv()
	v, _ := vector.NewFlat(s, bm, types.LogicalType{ID: types.BigInt}, vector.DefaultVectorSize)
	for i := 0; i < v.Count(); i++ {
		v.SetInt64(i, int64(i))
	}
	out := make([]uint64, v.Count())
	b.ReportAllocs()
	b.SetBytes(int64(v.Count() * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := VectorHash(v, out); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	v.Release()
	if m.Used() != 0 {
		b.Fatal("leak")
	}
}
func BenchmarkHashTableLookup(b *testing.B) {
	m, s, bm := henv()
	h, _ := NewHashTable(s, bm, 2048)
	for i := int64(0); i < 1000; i++ {
		_ = h.Insert(i, i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = h.Lookup(int64(i % 1000))
	}
	b.StopTimer()
	h.Release()
	if m.Used() != 0 {
		b.Fatal("leak")
	}
}

func TestVectorHashVarcharNoPerRowAllocation(t *testing.T) {
	m, s, bm := henv()
	v, _ := vector.NewFlat(s, bm, types.LogicalType{ID: types.Varchar}, vector.DefaultVectorSize)
	for i := 0; i < v.Count(); i++ {
		var x string
		if i&1 == 0 {
			x = "short"
		} else {
			x = "this is a long managed string value"
		}
		if err := v.SetString(i, x); err != nil {
			t.Fatal(err)
		}
	}
	out := make([]uint64, v.Count())
	allocs := testing.AllocsPerRun(100, func() {
		if err := VectorHash(v, out); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("varchar hash allocations/op = %.1f", allocs)
	}
	v.Release()
	if m.Used() != 0 {
		t.Fatalf("leak %d", m.Used())
	}
}

func TestVectorHashVarcharDistinguishesInlineValues(t *testing.T) {
	m, s, bm := henv()
	v, err := vector.NewFlat(s, bm, types.LogicalType{ID: types.Varchar}, 3)
	if err != nil {
		t.Fatal(err)
	}
	for i, value := range []string{"alpha", "bravo", "alpha"} {
		if err := v.SetString(i, value); err != nil {
			t.Fatal(err)
		}
	}
	out := make([]uint64, v.Count())
	if err := VectorHash(v, out); err != nil {
		t.Fatal(err)
	}
	if out[0] != out[2] {
		t.Fatal("equal inline strings must hash equally")
	}
	if out[0] == out[1] {
		t.Fatal("distinct inline strings unexpectedly share the Phase 0 test hash")
	}
	v.Release()
	if m.Used() != 0 {
		t.Fatalf("leak %d", m.Used())
	}
}

func BenchmarkVectorHashVarchar(b *testing.B) {
	m, s, bm := henv()
	v, _ := vector.NewFlat(s, bm, types.LogicalType{ID: types.Varchar}, vector.DefaultVectorSize)
	for i := 0; i < v.Count(); i++ {
		var x string
		if i&1 == 0 {
			x = "short"
		} else {
			x = "this is a long managed string value"
		}
		_ = v.SetString(i, x)
	}
	out := make([]uint64, v.Count())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := VectorHash(v, out); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	v.Release()
	if m.Used() != 0 {
		b.Fatal("leak")
	}
}

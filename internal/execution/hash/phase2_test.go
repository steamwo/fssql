package hash

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/semantics"
)

func testEnv(limit int64) (*memory.Manager, *memory.Scope, *memory.BufferManager) {
	m := memory.NewManager(limit)
	s := m.NewScope()
	return m, s, memory.NewBufferManager(nil)
}

func TestVectorHashPhase2ScalarTypes(t *testing.T) {
	m, s, bm := testEnv(1 << 20)
	dec128, _ := types.NewDecimal(38, 4)
	cases := []struct {
		typ  types.LogicalType
		set  func(*vector.Vector)
		want uint64
	}{
		{types.LogicalType{ID: types.Boolean}, func(v *vector.Vector) { v.SetBool(0, true) }, semantics.HashBool(true)},
		{types.LogicalType{ID: types.TinyInt}, func(v *vector.Vector) { v.SetInt8(0, -7) }, semantics.HashInt64(-7)},
		{types.LogicalType{ID: types.SmallInt}, func(v *vector.Vector) { v.SetInt16(0, -300) }, semantics.HashInt64(-300)},
		{types.LogicalType{ID: types.Integer}, func(v *vector.Vector) { v.SetInt32(0, -70000) }, semantics.HashInt64(-70000)},
		{types.LogicalType{ID: types.BigInt}, func(v *vector.Vector) { v.SetInt64(0, -9000000000) }, semantics.HashInt64(-9000000000)},
		{types.LogicalType{ID: types.Float}, func(v *vector.Vector) { v.SetFloat32(0, float32(-0.0)) }, semantics.HashFloat32(float32(-0.0))},
		{types.LogicalType{ID: types.Double}, func(v *vector.Vector) { v.SetFloat64(0, math.Copysign(0, -1)) }, semantics.HashFloat64(math.Copysign(0, -1))},
		{types.LogicalType{ID: types.Decimal, Decimal: types.DecimalInfo{Width: 18, Scale: 2}}, func(v *vector.Vector) { v.SetInt64(0, -12345) }, semantics.HashInt64(-12345)},
		{dec128, func(v *vector.Vector) { v.SetInt128(0, types.Int128{Lo: 9, Hi: -2}) }, semantics.HashInt128(9, -2)},
		{types.LogicalType{ID: types.Varchar}, func(v *vector.Vector) { _ = v.SetString(0, "phase-two") }, semantics.HashBytes([]byte("phase-two"))},
	}
	for _, tc := range cases {
		v, err := vector.NewFlat(s, bm, tc.typ, 2)
		if err != nil {
			t.Fatal(err)
		}
		tc.set(v)
		v.SetNull(1)
		out := make([]uint64, 2)
		if err := VectorHash(v, out); err != nil {
			t.Fatalf("type %v: %v", tc.typ.ID, err)
		}
		if out[0] != tc.want {
			t.Fatalf("type %v hash=%x want=%x", tc.typ.ID, out[0], tc.want)
		}
		if out[1] != semantics.HashNull() {
			t.Fatalf("type %v null hash=%x", tc.typ.ID, out[1])
		}
		v.Release()
	}
	if m.Used() != 0 {
		t.Fatalf("memory leak: %d", m.Used())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestVectorHashConstantDictionaryEquivalent(t *testing.T) {
	m, s, bm := testEnv(1 << 20)
	base, _ := vector.NewFlat(s, bm, types.LogicalType{ID: types.BigInt}, 4)
	for i, x := range []int64{11, 22, 11, 33} {
		base.SetInt64(i, x)
	}
	dict, err := vector.NewDictionary(base, vector.SelectionFrom([]uint32{2, 0, 3, 1}))
	if err != nil {
		t.Fatal(err)
	}
	flatOut := make([]uint64, 4)
	dictOut := make([]uint64, 4)
	if err := VectorHash(base, flatOut); err != nil {
		t.Fatal(err)
	}
	if err := VectorHash(dict, dictOut); err != nil {
		t.Fatal(err)
	}
	for i, source := range []int{2, 0, 3, 1} {
		if dictOut[i] != flatOut[source] {
			t.Fatalf("dictionary hash mismatch at %d", i)
		}
	}
	constant, err := vector.NewConstant(s, bm, types.LogicalType{ID: types.BigInt}, 4)
	if err != nil {
		t.Fatal(err)
	}
	constant.SetInt64(0, 77)
	constOut := make([]uint64, 4)
	if err := VectorHash(constant, constOut); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(constOut); i++ {
		if constOut[i] != constOut[0] {
			t.Fatal("constant hashes differ")
		}
	}
	constant.Release()
	base.Release()
	if m.Used() != 0 {
		t.Fatalf("memory leak: %d", m.Used())
	}
}

func TestHashChunkComposition(t *testing.T) {
	m, s, bm := testEnv(1 << 20)
	ids, _ := vector.NewFlat(s, bm, types.LogicalType{ID: types.BigInt}, 3)
	names, _ := vector.NewFlat(s, bm, types.LogicalType{ID: types.Varchar}, 3)
	for i, id := range []int64{1, 1, 2} {
		ids.SetInt64(i, id)
	}
	for i, name := range []string{"a", "b", "a"} {
		if err := names.SetString(i, name); err != nil {
			t.Fatal(err)
		}
	}
	chunk, _ := vector.NewDataChunk(ids, names)
	out := make([]uint64, 3)
	if err := HashChunk(chunk, []int{0, 1}, out); err != nil {
		t.Fatal(err)
	}
	if out[0] == out[1] || out[0] == out[2] {
		t.Fatal("compound hash failed to distinguish test keys")
	}
	one := make([]uint64, 3)
	if err := HashChunk(chunk, []int{0}, one); err != nil {
		t.Fatal(err)
	}
	ref := make([]uint64, 3)
	if err := VectorHash(ids, ref); err != nil {
		t.Fatal(err)
	}
	for i := range one {
		if one[i] != ref[i] {
			t.Fatal("single-column chunk hash differs from VectorHash")
		}
	}
	ids.Release()
	names.Release()
	if m.Used() != 0 {
		t.Fatalf("memory leak: %d", m.Used())
	}
}

func TestRelocatableRowLayoutAndTupleCollection(t *testing.T) {
	m, s, bm := testEnv(1 << 20)
	layout, err := NewRelocatableRowLayout([]types.LogicalType{{ID: types.BigInt}, {ID: types.Varchar}, {ID: types.Double}})
	if err != nil {
		t.Fatal(err)
	}
	ids, _ := vector.NewFlat(s, bm, types.LogicalType{ID: types.BigInt}, 3)
	names, _ := vector.NewFlat(s, bm, types.LogicalType{ID: types.Varchar}, 3)
	vals, _ := vector.NewFlat(s, bm, types.LogicalType{ID: types.Double}, 3)
	for i, id := range []int64{10, 20, 30} {
		ids.SetInt64(i, id)
	}
	for i, name := range []string{"short", "this is a long string stored in the heap", "ignored"} {
		if err := names.SetString(i, name); err != nil {
			t.Fatal(err)
		}
	}
	for i, x := range []float64{1.5, 2.5, 3.5} {
		vals.SetFloat64(i, x)
	}
	names.SetNull(2)
	chunk, _ := vector.NewDataChunk(ids, names, vals)
	collection, err := NewTupleDataCollectionForLayout(s, bm, layout, 1)
	if err != nil {
		t.Fatal(err)
	}
	for row := 0; row < chunk.Cardinality; row++ {
		if err := collection.AppendChunkRow(chunk, row); err != nil {
			t.Fatal(err)
		}
	}
	if collection.Count() != 3 {
		t.Fatalf("count=%d", collection.Count())
	}
	for row := 0; row < 3; row++ {
		raw := collection.Row(row)
		idBytes, ok, err := layout.ColumnData(raw, 0)
		if err != nil || !ok {
			t.Fatalf("id row %d: %v %v", row, ok, err)
		}
		if got := int64(binary.LittleEndian.Uint64(idBytes)); got != []int64{10, 20, 30}[row] {
			t.Fatalf("id row %d=%d", row, got)
		}
		nameBytes, ok, err := layout.ColumnData(raw, 1)
		if err != nil {
			t.Fatal(err)
		}
		if row == 2 {
			if ok || nameBytes != nil || !layout.IsNull(raw, 1) {
				t.Fatal("NULL VARCHAR not preserved")
			}
		} else if string(nameBytes) != []string{"short", "this is a long string stored in the heap"}[row] {
			t.Fatalf("name row %d=%q", row, nameBytes)
		}
	}
	collection.Release()
	ids.Release()
	names.Release()
	vals.Release()
	if m.Used() != 0 {
		t.Fatalf("managed memory leak: %d", m.Used())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTupleCollectionGrowthFailureRemainsReleasable(t *testing.T) {
	m, s, bm := testEnv(250)
	layout, err := NewRelocatableRowLayout([]types.LogicalType{{ID: types.Varchar}})
	if err != nil {
		t.Fatal(err)
	}
	v, err := vector.NewFlat(s, bm, types.LogicalType{ID: types.Varchar}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := v.SetString(0, "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuv"); err != nil {
		t.Fatal(err)
	}
	chunk, _ := vector.NewDataChunk(v)
	collection, err := NewTupleDataCollectionForLayout(s, bm, layout, 1)
	if err != nil {
		t.Fatal(err)
	}
	err = collection.AppendChunkRow(chunk, 0)
	if err != nil && !errors.Is(err, memory.ErrLimitExceeded) {
		t.Fatalf("append error=%v", err)
	}
	collection.Release()
	v.Release()
	if m.Used() != 0 {
		t.Fatalf("managed memory leak after failure: %d", m.Used())
	}
}

func TestRadixPartitionAtDepth(t *testing.T) {
	p, err := NewRadixPartitioner(4)
	if err != nil {
		t.Fatal(err)
	}
	h := uint64(0x0000000000000a5c)
	if got, _ := p.PartitionAtDepth(h, 0); got != 0xc {
		t.Fatalf("depth0=%x", got)
	}
	if got, _ := p.PartitionAtDepth(h, 1); got != 0x5 {
		t.Fatalf("depth1=%x", got)
	}
	if got, _ := p.PartitionAtDepth(h, 2); got != 0xa {
		t.Fatalf("depth2=%x", got)
	}
	if _, err := p.PartitionAtDepth(h, 16); err == nil {
		t.Fatal("expected depth overflow")
	}
}

func FuzzRelocatableVarcharRow(f *testing.F) {
	f.Add("alpha")
	f.Add("")
	f.Add("this is longer than inline storage")
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 64<<10 {
			t.Skip()
		}
		m, s, bm := testEnv(1 << 20)
		v, err := vector.NewFlat(s, bm, types.LogicalType{ID: types.Varchar}, 1)
		if err != nil {
			t.Fatal(err)
		}
		if err := v.SetString(0, value); err != nil {
			t.Fatal(err)
		}
		chunk, _ := vector.NewDataChunk(v)
		layout, _ := NewRelocatableRowLayout([]types.LogicalType{{ID: types.Varchar}})
		size, err := layout.RowSize(chunk, 0)
		if err != nil {
			t.Fatal(err)
		}
		row := make([]byte, size)
		if _, err := layout.WriteRow(chunk, 0, row); err != nil {
			t.Fatal(err)
		}
		data, ok, err := layout.ColumnData(row, 0)
		if err != nil || !ok {
			t.Fatalf("decode=%v,%v", ok, err)
		}
		if !bytes.Equal(data, []byte(value)) {
			t.Fatal("row round trip mismatch")
		}
		v.Release()
		if m.Used() != 0 {
			t.Fatalf("leak %d", m.Used())
		}
	})
}

func TestLegacyFixedTupleAPIStillRoundTrips(t *testing.T) {
	m, s, bm := testEnv(1 << 20)
	layout, err := NewRowLayout([]types.LogicalType{{ID: types.BigInt}, {ID: types.Boolean}})
	if err != nil {
		t.Fatal(err)
	}
	if layout.RowWidth != 9 || layout.Columns[0].Offset != 0 || layout.Columns[1].Offset != 8 {
		t.Fatalf("legacy layout changed: %+v", layout)
	}
	c, err := NewTupleDataCollection(s, bm, layout.RowWidth, 1)
	if err != nil {
		t.Fatal(err)
	}
	row := make([]byte, layout.RowWidth)
	binary.LittleEndian.PutUint64(row[:8], 42)
	row[8] = 1
	for i := 0; i < 10; i++ {
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
	if d.Count() != 10 || binary.LittleEndian.Uint64(d.Row(9)[:8]) != 42 {
		t.Fatal("legacy tuple round trip failed")
	}
	d.Release()
	if m.Used() != 0 {
		t.Fatalf("leak %d", m.Used())
	}
}

func TestManagedHashTablePrimitiveStillWorks(t *testing.T) {
	m, s, bm := testEnv(1 << 20)
	h, err := NewHashTable(s, bm, 128)
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 64; i++ {
		if err := h.Insert(i, i*7); err != nil {
			t.Fatal(err)
		}
	}
	for i := int64(0); i < 64; i++ {
		v, ok := h.Lookup(i)
		if !ok || v != i*7 {
			t.Fatalf("lookup %d=(%d,%v)", i, v, ok)
		}
	}
	h.Release()
	if m.Used() != 0 {
		t.Fatalf("leak %d", m.Used())
	}
}

func BenchmarkHashChunkBigIntVarchar(b *testing.B) {
	m, s, bm := testEnv(8 << 20)
	ids, _ := vector.NewFlat(s, bm, types.LogicalType{ID: types.BigInt}, vector.DefaultVectorSize)
	names, _ := vector.NewFlat(s, bm, types.LogicalType{ID: types.Varchar}, vector.DefaultVectorSize)
	for i := 0; i < vector.DefaultVectorSize; i++ {
		ids.SetInt64(i, int64(i))
		_ = names.SetString(i, "managed-hash-key-value")
	}
	chunk, _ := vector.NewDataChunk(ids, names)
	out := make([]uint64, chunk.Cardinality)
	b.SetBytes(int64(chunk.Cardinality * (8 + 22)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := HashChunk(chunk, []int{0, 1}, out); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	ids.Release()
	names.Release()
	if m.Used() != 0 {
		b.Fatal("leak")
	}
}

func TestHugeIntHashAndRowEquality(t *testing.T) {
	m, s, bm := testEnv(1 << 20)
	v, err := vector.NewFlat(s, bm, types.LogicalType{ID: types.HugeInt}, 4)
	if err != nil {
		t.Fatal(err)
	}
	vals := []types.Int128{
		types.Int128FromInt64(-1),
		{Lo: 1 << 63, Hi: 0},
		{Lo: 1 << 63, Hi: 0},
		{Lo: ^uint64(0), Hi: int64(^uint64(0) >> 1)},
	}
	for i, x := range vals {
		v.SetInt128(i, x)
	}
	h := make([]uint64, len(vals))
	if err := VectorHash(v, h); err != nil {
		t.Fatal(err)
	}
	if h[1] != h[2] {
		t.Fatal("equal HUGEINT values must hash equally")
	}
	if h[0] == h[1] {
		t.Fatal("test values unexpectedly collide")
	}
	layout, err := NewRelocatableRowLayout([]types.LogicalType{{ID: types.HugeInt}})
	if err != nil {
		t.Fatal(err)
	}
	c := &vector.DataChunk{Columns: []*vector.Vector{v}, Cardinality: v.Count()}
	rows, err := NewTupleDataCollectionForLayout(s, bm, layout, 4)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if err := rows.AppendChunkRow(c, i); err != nil {
			t.Fatal(err)
		}
	}
	eq, err := layout.EqualRows(rows.Row(1), rows.Row(2), NullsEqual)
	if err != nil || !eq {
		t.Fatalf("HUGEINT row equality=%v err=%v", eq, err)
	}
	neq, err := layout.EqualRows(rows.Row(0), rows.Row(1), NullsEqual)
	if err != nil || neq {
		t.Fatalf("HUGEINT row inequality=%v err=%v", neq, err)
	}
	rows.Release()
	v.Release()
	if m.Used() != 0 {
		t.Fatalf("leak %d", m.Used())
	}
}

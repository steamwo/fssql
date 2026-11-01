package parquet

import (
	"encoding/binary"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

func dataPageHeaderBytes() []byte {
	w := &compactWriter{}
	w.i32Field(1, int32(PageData))
	w.i32Field(2, 12)
	w.i32Field(3, 8)
	w.field(5, ctStruct)
	w.b = appendStruct(w.b, func(s *compactWriter) {
		s.i32Field(1, 3)
		s.i32Field(2, int32(EncodingPlain))
		s.i32Field(3, int32(EncodingRLE))
		s.i32Field(4, int32(EncodingRLE))
	})
	w.stop()
	return w.b
}

func TestDecodePageHeader(t *testing.T) {
	b := dataPageHeaderBytes()
	h, used, err := DecodePageHeader(b)
	if err != nil {
		t.Fatal(err)
	}
	if used != len(b) || h.Type != PageData || h.CompressedPageSize != 8 || h.Data == nil || h.Data.NumValues != 3 || h.Data.Encoding != EncodingPlain {
		t.Fatalf("header=%+v used=%d len=%d", h, used, len(b))
	}
}

func TestDecodeLevelsRLEAndBitPacked(t *testing.T) {
	v1 := []byte{2, 0, 0, 0, 10, 1}
	vals, used, err := DecodeLevelsV1(v1, 1, 5, EncodingRLE)
	if err != nil {
		t.Fatal(err)
	}
	if used != len(v1) || len(vals) != 5 {
		t.Fatalf("vals=%v used=%d", vals, used)
	}
	for _, v := range vals {
		if v != 1 {
			t.Fatalf("vals=%v", vals)
		}
	}
	vals, used, err = DecodeLevelsV1([]byte{0b00101101}, 1, 5, EncodingBitPacked)
	if err != nil {
		t.Fatal(err)
	}
	want := []int16{1, 0, 1, 1, 0}
	for i := range want {
		if vals[i] != want[i] {
			t.Fatalf("vals=%v", vals)
		}
	}
	if used != 1 {
		t.Fatalf("used=%d", used)
	}
}

func TestDecodeDictionaryIndexes(t *testing.T) {
	idx, err := DecodeDictionaryIndexes([]byte{2, 8, 2}, 4)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range idx {
		if v != 2 {
			t.Fatalf("idx=%v", idx)
		}
	}
}

func TestSnappyLiteralAndCopy(t *testing.T) {
	out, err := Decompress(CodecSnappy, append([]byte{5, 16}, []byte("hello")...), 5)
	if err != nil || string(out) != "hello" {
		t.Fatalf("out=%q err=%v", out, err)
	}
	out, err = Decompress(CodecSnappy, []byte{6, 0, 'a', 5, 1}, 6)
	if err != nil || string(out) != "aaaaaa" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func TestDecodePlainDirectToVector(t *testing.T) {
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	ints, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.Integer}, 2)
	if err != nil {
		t.Fatal(err)
	}
	var raw [8]byte
	neg := int32(-7)
	binary.LittleEndian.PutUint32(raw[0:4], uint32(neg))
	binary.LittleEndian.PutUint32(raw[4:8], 42)
	if used, err := DecodePlainInto(ints, 0, 2, TypeInt32, raw[:]); err != nil || used != 8 {
		t.Fatalf("used=%d err=%v", used, err)
	}
	if v, ok := ints.Int32(0); !ok || v != -7 {
		t.Fatalf("v=%d ok=%v", v, ok)
	}
	strs, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.Varchar}, 2)
	if err != nil {
		ints.Release()
		t.Fatal(err)
	}
	data := make([]byte, 0, 32)
	for _, s := range []string{"x", "longer-string-value"} {
		var l [4]byte
		binary.LittleEndian.PutUint32(l[:], uint32(len(s)))
		data = append(data, l[:]...)
		data = append(data, s...)
	}
	if _, err := DecodePlainInto(strs, 0, 2, TypeByteArray, data); err != nil {
		t.Fatal(err)
	}
	if s, ok := strs.String(1); !ok || s != "longer-string-value" {
		t.Fatalf("s=%q ok=%v", s, ok)
	}
	ints.Release()
	strs.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed leak=%d", scope.Used())
	}
}

func TestCopyDictionaryIndexes(t *testing.T) {
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	dict, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.Varchar}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := dict.SetStringBytes(0, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := dict.SetStringBytes(1, []byte("b")); err != nil {
		t.Fatal(err)
	}
	out, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.Varchar}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := CopyDictionaryIndexes(out, 0, dict, []uint32{1, 0, 1}); err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"b", "a", "b"} {
		got, ok := out.String(i)
		if !ok || got != want {
			t.Fatalf("row=%d got=%q ok=%v", i, got, ok)
		}
	}
	dict.Release()
	out.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed leak=%d", scope.Used())
	}
}

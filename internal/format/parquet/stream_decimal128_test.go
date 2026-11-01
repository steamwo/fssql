package parquet

import (
	"encoding/binary"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	coretypes "github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

func encodeInt128BigEndian(value coretypes.Int128) []byte {
	var raw [16]byte
	binary.BigEndian.PutUint64(raw[:8], uint64(value.Hi))
	binary.BigEndian.PutUint64(raw[8:], value.Lo)
	return raw[:]
}

func negateUnsigned128Magnitude(lo uint64) coretypes.Int128 {
	return coretypes.Int128{Lo: ^lo + 1, Hi: -1}
}

func TestValueStreamDecimal128ByteArrayAndFixedLen(t *testing.T) {
	decimal, err := coretypes.NewDecimal(20, 4)
	if err != nil {
		t.Fatal(err)
	}
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)

	positive := coretypes.Int128{Lo: 12345678901234567890, Hi: 0}
	negative := negateUnsigned128Magnitude(12345678901234567890)
	values := []coretypes.Int128{positive, negative}

	var payload []byte
	for _, value := range values {
		raw := encodeInt128BigEndian(value)
		var n [4]byte
		binary.LittleEndian.PutUint32(n[:], uint32(len(raw)))
		payload = append(payload, n[:]...)
		payload = append(payload, raw...)
	}
	byteArray, err := vector.NewFlat(scope, bm, decimal, len(values))
	if err != nil {
		t.Fatal(err)
	}
	stream := newValueStream(payload, TypeByteArray, &SchemaNode{Logical: decimal})
	for i := range values {
		if err := stream.NextInto(byteArray, i); err != nil {
			byteArray.Release()
			t.Fatal(err)
		}
		got, ok := byteArray.Int128(i)
		if !ok || got != values[i] {
			byteArray.Release()
			t.Fatalf("byte-array decimal[%d]=%+v valid=%v, want %+v", i, got, ok, values[i])
		}
	}
	byteArray.Release()

	fixedLen := int32(16)
	physical := TypeFixedLenByteArray
	fixedLeaf := &SchemaNode{Element: SchemaElement{Type: &physical, TypeLength: &fixedLen}, Logical: decimal}
	fixed, err := vector.NewFlat(scope, bm, decimal, len(values))
	if err != nil {
		t.Fatal(err)
	}
	fixedPayload := append(encodeInt128BigEndian(positive), encodeInt128BigEndian(negative)...)
	stream = newValueStream(fixedPayload, TypeFixedLenByteArray, fixedLeaf)
	for i := range values {
		if err := stream.NextInto(fixed, i); err != nil {
			fixed.Release()
			t.Fatal(err)
		}
		got, ok := fixed.Int128(i)
		if !ok || got != values[i] {
			fixed.Release()
			t.Fatalf("fixed decimal[%d]=%+v valid=%v, want %+v", i, got, ok, values[i])
		}
	}
	fixed.Release()

	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}

func TestDecimal128PrecisionOverflowRejected(t *testing.T) {
	decimal, err := coretypes.NewDecimal(19, 0)
	if err != nil {
		t.Fatal(err)
	}
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	v, err := vector.NewFlat(scope, bm, decimal, 1)
	if err != nil {
		t.Fatal(err)
	}

	// 10^19 has 20 decimal digits, so it does not fit DECIMAL(19,0).
	value := coretypes.Int128{Lo: 10000000000000000000, Hi: 0}
	raw := encodeInt128BigEndian(value)
	var n [4]byte
	binary.LittleEndian.PutUint32(n[:], uint32(len(raw)))
	payload := append(n[:], raw...)
	if err := newValueStream(payload, TypeByteArray, &SchemaNode{Logical: decimal}).NextInto(v, 0); err == nil {
		v.Release()
		t.Fatal("expected declared-precision overflow")
	}
	v.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}

func TestDecimalHighPrecisionRejectsIntPhysicalEncoding(t *testing.T) {
	decimal19, err := coretypes.NewDecimal(19, 0)
	if err != nil {
		t.Fatal(err)
	}
	decimal10, err := coretypes.NewDecimal(10, 0)
	if err != nil {
		t.Fatal(err)
	}
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)

	v64, err := vector.NewFlat(scope, bm, decimal19, 1)
	if err != nil {
		t.Fatal(err)
	}
	var raw64 [8]byte
	binary.LittleEndian.PutUint64(raw64[:], 1)
	if err := newValueStream(raw64[:], TypeInt64, &SchemaNode{Logical: decimal19}).NextInto(v64, 0); err == nil {
		v64.Release()
		t.Fatal("DECIMAL(19) should reject INT64 physical encoding")
	}
	v64.Release()

	v32, err := vector.NewFlat(scope, bm, decimal10, 1)
	if err != nil {
		t.Fatal(err)
	}
	var raw32 [4]byte
	binary.LittleEndian.PutUint32(raw32[:], 1)
	if err := newValueStream(raw32[:], TypeInt32, &SchemaNode{Logical: decimal10}).NextInto(v32, 0); err == nil {
		v32.Release()
		t.Fatal("DECIMAL(10) should reject INT32 physical encoding")
	}
	v32.Release()

	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}

func TestTimestampMillisOverflowRejected(t *testing.T) {
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	timestamp := coretypes.LogicalType{ID: coretypes.TimestampType}
	physical := TypeInt64
	var raw [8]byte
	binary.LittleEndian.PutUint64(raw[:], uint64(int64(^uint64(0)>>1)))
	leaf := &SchemaNode{Element: SchemaElement{Type: &physical, Logical: &LogicalType{Kind: LogicalTimestamp, Timestamp: &TimestampLogical{Unit: UnitMillis}}}, Logical: timestamp}
	v, err := vector.NewFlat(scope, bm, timestamp, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := newValueStream(raw[:], TypeInt64, leaf).NextInto(v, 0); err == nil {
		v.Release()
		t.Fatal("expected timestamp microsecond overflow")
	}
	v.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}

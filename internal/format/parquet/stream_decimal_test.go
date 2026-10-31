package parquet

import (
	"encoding/binary"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	coretypes "github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

func TestValueStreamDecimal64ByteArrayAndFixedLen(t *testing.T) {
	decimal, err := coretypes.NewDecimal(18, 2)
	if err != nil {
		t.Fatal(err)
	}
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)

	byteArray, err := vector.NewFlat(scope, bm, decimal, 2)
	if err != nil {
		t.Fatal(err)
	}
	var payload []byte
	for _, raw := range [][]byte{{0x01, 0xe2, 0x40}, {0xff, 0xff, 0x9c, 0x40}} { // 123456, -25536
		var n [4]byte
		binary.LittleEndian.PutUint32(n[:], uint32(len(raw)))
		payload = append(payload, n[:]...)
		payload = append(payload, raw...)
	}
	leaf := &SchemaNode{Logical: decimal}
	stream := newValueStream(payload, TypeByteArray, leaf)
	for i := 0; i < 2; i++ {
		if err := stream.NextInto(byteArray, i); err != nil {
			t.Fatal(err)
		}
	}
	if got, ok := byteArray.Int64(0); !ok || got != 123456 {
		t.Fatalf("byte-array decimal[0]=%d valid=%v", got, ok)
	}
	if got, ok := byteArray.Int64(1); !ok || got != -25536 {
		t.Fatalf("byte-array decimal[1]=%d valid=%v", got, ok)
	}
	byteArray.Release()

	fixedLen := int32(4)
	physical := TypeFixedLenByteArray
	fixedLeaf := &SchemaNode{Element: SchemaElement{Type: &physical, TypeLength: &fixedLen}, Logical: decimal}
	fixed, err := vector.NewFlat(scope, bm, decimal, 2)
	if err != nil {
		t.Fatal(err)
	}
	stream = newValueStream([]byte{0x00, 0x01, 0xe2, 0x40, 0xff, 0xff, 0x9c, 0x40}, TypeFixedLenByteArray, fixedLeaf)
	for i := 0; i < 2; i++ {
		if err := stream.NextInto(fixed, i); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := fixed.Int64(0); got != 123456 {
		t.Fatalf("fixed decimal[0]=%d", got)
	}
	if got, _ := fixed.Int64(1); got != -25536 {
		t.Fatalf("fixed decimal[1]=%d", got)
	}
	fixed.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}

func TestDecodeDecimal64BigEndianRejectsDecimal128(t *testing.T) {
	decimal, err := coretypes.NewDecimal(19, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeDecimal64BigEndian([]byte{1}, decimal); err == nil {
		t.Fatal("expected Decimal128 requirement error")
	}
}

func TestValueStreamTimestampUnitsAndINT96(t *testing.T) {
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	timestamp := coretypes.LogicalType{ID: coretypes.TimestampType}

	physical := TypeInt64
	millis := int64(1700000000123)
	var raw [8]byte
	binary.LittleEndian.PutUint64(raw[:], uint64(millis))
	leaf := &SchemaNode{Element: SchemaElement{Type: &physical, Logical: &LogicalType{Kind: LogicalTimestamp, Timestamp: &TimestampLogical{Unit: UnitMillis}}}, Logical: timestamp}
	v, err := vector.NewFlat(scope, bm, timestamp, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := newValueStream(raw[:], TypeInt64, leaf).NextInto(v, 0); err != nil {
		t.Fatal(err)
	}
	if got, _ := v.Int64(0); got != millis*1000 {
		t.Fatalf("millis timestamp=%d", got)
	}
	v.Release()

	var int96 [12]byte
	binary.LittleEndian.PutUint64(int96[:8], uint64(1_234_567_000))
	binary.LittleEndian.PutUint32(int96[8:], uint32(2440588))
	v, err = vector.NewFlat(scope, bm, timestamp, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := newValueStream(int96[:], TypeInt96, &SchemaNode{Logical: timestamp}).NextInto(v, 0); err != nil {
		t.Fatal(err)
	}
	if got, _ := v.Int64(0); got != 1_234_567 {
		t.Fatalf("INT96 timestamp=%d", got)
	}
	v.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}

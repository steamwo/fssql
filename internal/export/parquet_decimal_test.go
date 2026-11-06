package export

import (
	"encoding/binary"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/format/parquet"
)

func TestParquetDecimalPhysicalEncoding(t *testing.T) {
	for _, width := range []uint8{1, 9, 10, 18, 19, 38} {
		typ, _ := types.NewDecimal(width, 0)
		e, err := parquetSchemaElement(Column{Name: "d", Type: typ})
		if err != nil {
			t.Fatal(err)
		}
		physical, size := parquet.TypeFixedLenByteArray, 16
		if width <= 9 {
			physical, size = parquet.TypeInt32, 4
		} else if width <= 18 {
			physical, size = parquet.TypeInt64, 8
		}
		if *e.Type != physical || e.Logical.Decimal.Precision != int32(width) {
			t.Fatalf("width %d schema=%+v", width, e)
		}
		mgr := memory.NewManager(1 << 20)
		scope := mgr.NewScope()
		bm := memory.NewBufferManager(nil)
		v, err := vector.NewFlat(scope, bm, typ, 3)
		if err != nil {
			t.Fatal(err)
		}
		if width <= 18 {
			v.SetInt64(0, -9)
			v.SetInt64(2, 9)
		} else {
			v.SetInt128(0, types.Int128FromInt64(-9))
			v.SetInt128(2, types.Int128FromInt64(9))
		}
		v.SetNull(1)
		out, err := newParquetPageBuffer(scope, bm, 64)
		if err != nil {
			t.Fatal(err)
		}
		if err := encodeParquetPlain(out, v, typ, 0, 3); err != nil {
			t.Fatal(err)
		}
		if out.Len() != 2*size {
			t.Fatalf("width %d bytes=%d want=%d", width, out.Len(), 2*size)
		}
		if size == 4 && (int32(binary.LittleEndian.Uint32(out.Bytes()[:4])) != -9 || binary.LittleEndian.Uint32(out.Bytes()[4:]) != 9) {
			t.Fatalf("invalid DECIMAL32: %x", out.Bytes())
		}
		out.Release()
		v.Release()
		if mgr.Used() != 0 {
			t.Fatalf("memory leak=%d", mgr.Used())
		}
	}
}

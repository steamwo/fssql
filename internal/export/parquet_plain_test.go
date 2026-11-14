package export

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

func TestParquetPlainNativeBytes(t *testing.T) {
	decimal10, _ := types.NewDecimal(10, 2)
	decimal18, _ := types.NewDecimal(18, 4)
	for _, typ := range []types.LogicalType{
		{ID: types.Integer}, types.UIntegerType(), {ID: types.DateType},
		{ID: types.BigInt}, types.UBigIntType(), {ID: types.TimestampType},
		types.TimestampNSType(), types.TimestampTZType(),
		{ID: types.Float}, {ID: types.Double}, decimal10, decimal18,
	} {
		for _, nulls := range []string{"none", "mixed", "all"} {
			for _, kind := range []string{"flat", "constant", "dictionary"} {
				t.Run(fmt.Sprintf("%v/%s/%s", typ, nulls, kind), func(t *testing.T) {
					scope := memory.NewManager(1 << 20).NewScope()
					bm := memory.NewBufferManager(nil)
					const rows = 137 // Cross validity words and leave a partial final word.
					base := newVector(t, scope, bm, typ, rows)
					data, _, _ := base.MutableFixedWidth()
					width, _ := typ.FixedSize()
					bits := []uint64{0, 1, math.MaxUint64, 1 << 63, math.MaxInt64, 0x7ff8000000000123, 0x7ff0000000000000, 0xfff0000000000000}
					if width == 4 {
						bits = []uint64{0, 1, math.MaxUint32, 1 << 31, math.MaxInt32, 0x7fc00123, 0x7f800000, 0xff800000}
					}
					for row := 0; row < rows; row++ {
						if width == 4 {
							binary.LittleEndian.PutUint32(data[row*width:], uint32(bits[row%len(bits)]))
						} else {
							binary.LittleEndian.PutUint64(data[row*width:], bits[row%len(bits)])
						}
						if nulls == "all" || nulls == "mixed" && row%5 == 0 {
							base.SetNull(row)
						}
					}
					source := base
					indices := make([]uint32, rows)
					for row := range indices {
						indices[row] = uint32(row)
					}
					if kind == "constant" {
						var err error
						source, err = vector.NewConstant(scope, bm, typ, rows)
						if err != nil {
							t.Fatal(err)
						}
						if err := vector.CopyValue(base, 3, source, 0); err != nil {
							t.Fatal(err)
						}
						for row := range indices {
							indices[row] = 3
						}
					} else if kind == "dictionary" {
						for row := range indices {
							indices[row] = uint32(rows - 1 - row)
						}
						first, err := vector.NewOwnedDictionary(scope, base, vector.SelectionFrom(indices))
						if err != nil {
							t.Fatal(err)
						}
						rotation := make([]uint32, rows)
						for row := range rotation {
							rotation[row] = uint32((row + 3) % rows)
						}
						source, err = vector.NewOwnedDictionary(scope, first, vector.SelectionFrom(rotation))
						if err != nil {
							t.Fatal(err)
						}
						for row := range indices {
							indices[row] = uint32(rows - 1 - int(rotation[row]))
						}
					}
					page, err := newParquetPageBuffer(scope, bm, 8)
					if err != nil {
						t.Fatal(err)
					}
					for _, span := range [][2]int{{0, 0}, {0, rows}, {1, 63}, {65, 64}, {136, 1}, {rows, 0}} {
						page.used = 0
						prefix := []byte{0x12, 0x34, 0x56}
						if err := page.Append(prefix); err != nil {
							t.Fatal(err)
						}
						want := append([]byte(nil), prefix...)
						for row := span[0]; row < span[0]+span[1]; row++ {
							index := int(indices[row])
							if nulls != "all" && !(nulls == "mixed" && index%5 == 0) {
								want = append(want, data[index*width:(index+1)*width]...)
							}
						}
						if err := encodeParquetPlain(page, source, typ, span[0], span[1]); err != nil {
							t.Fatal(err)
						}
						if !bytes.Equal(page.Bytes(), want) {
							t.Fatalf("span=%v: got %x want %x", span, page.Bytes(), want)
						}
					}
					page.Release()
					source.Release()
					if kind == "constant" {
						base.Release()
					}
					if scope.Used() != 0 {
						t.Fatalf("managed leak=%d", scope.Used())
					}
				})
			}
		}
	}
}

func TestParquetPlainNativeBudget(t *testing.T) {
	for _, validRows := range []int{0, 1, 8} {
		t.Run(fmt.Sprint(validRows), func(t *testing.T) {
			const limit = 4096
			scope := memory.NewManager(limit).NewScope()
			bm := memory.NewBufferManager(nil)
			typ := types.LogicalType{ID: types.BigInt}
			v := newVector(t, scope, bm, typ, 8)
			for row := 0; row < 8; row++ {
				v.SetInt64(row, int64(row+1))
				if row < 8-validRows {
					v.SetNull(row)
				}
			}
			page, err := newParquetPageBuffer(scope, bm, 16)
			if err != nil {
				t.Fatal(err)
			}
			if err := page.Append([]byte{1, 2, 3}); err != nil {
				t.Fatal(err)
			}
			hold, err := scope.Reserve(memory.TagMetadata, limit-scope.Used())
			if err != nil {
				t.Fatal(err)
			}
			err = encodeParquetPlain(page, v, typ, 0, 8)
			if validRows < 2 {
				if err != nil || page.Len() != 3+validRows*8 {
					t.Fatalf("sparse page len=%d err=%v", page.Len(), err)
				}
			} else if !errors.Is(err, memory.ErrLimitExceeded) {
				t.Fatalf("expected memory limit, got %v", err)
			}
			hold.Release()
			page.used = 3
			if err := encodeParquetPlain(page, v, typ, 0, 8); err != nil {
				t.Fatal(err)
			}
			if page.Len() != 3+validRows*8 {
				t.Fatalf("retry len=%d", page.Len())
			}
			page.Release()
			v.Release()
			if scope.Used() != 0 {
				t.Fatalf("managed leak=%d", scope.Used())
			}
		})
	}
}

func BenchmarkParquetPlainNative(b *testing.B) {
	for _, tc := range []struct {
		name  string
		typ   types.LogicalType
		nulls int
	}{
		{"bigint/valid", types.LogicalType{ID: types.BigInt}, 0},
		{"bigint/null10", types.LogicalType{ID: types.BigInt}, 10},
		{"bigint/null90", types.LogicalType{ID: types.BigInt}, 90},
		{"double/valid", types.LogicalType{ID: types.Double}, 0},
	} {
		b.Run(tc.name, func(b *testing.B) {
			scope := memory.NewManager(1 << 20).NewScope()
			bm := memory.NewBufferManager(nil)
			v, err := vector.NewFlat(scope, bm, tc.typ, 2048)
			if err != nil {
				b.Fatal(err)
			}
			for row := 0; row < v.Count(); row++ {
				if tc.typ.ID == types.Double {
					v.SetFloat64(row, float64(row)/4)
				} else {
					v.SetInt64(row, int64(row))
				}
				if row%100 < tc.nulls {
					v.SetNull(row)
				}
			}
			page, err := newParquetPageBuffer(scope, bm, 64<<10)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				page.used = 0
				if err := encodeParquetPlain(page, v, tc.typ, (i%4)*512, 512); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			page.Release()
			v.Release()
			if scope.Used() != 0 {
				b.Fatalf("managed leak=%d", scope.Used())
			}
		})
	}
}

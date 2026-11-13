package export

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

// Compare scalar and batch materialization at the byte level, including NULLs
// and floating-point bits, after the input storage has been released.
func TestParquetBatchInputOwnership(t *testing.T) {
	decimal, _ := types.NewDecimal(38, 4)
	for _, typ := range []types.LogicalType{
		{ID: types.Boolean}, {ID: types.TinyInt}, {ID: types.SmallInt},
		{ID: types.Integer}, {ID: types.BigInt}, {ID: types.BigInt, Unsigned: true},
		{ID: types.Double}, decimal, {ID: types.Varchar}, {ID: types.Varchar, Binary: true},
	} {
		for _, kind := range []string{"flat", "constant", "dictionary"} {
			t.Run(fmt.Sprintf("%v/%s", typ, kind), func(t *testing.T) {
				scope := memory.NewManager(4 << 20).NewScope()
				bm := memory.NewBufferManager(nil)
				base := newVector(t, scope, bm, typ, 5)
				for row := 0; row < 5; row++ {
					switch typ.ID {
					case types.Boolean:
						base.SetBool(row, row%2 == 0)
					case types.TinyInt:
						base.SetInt8(row, int8(row-3))
					case types.SmallInt:
						base.SetInt16(row, int16(row-3))
					case types.Integer:
						base.SetInt32(row, int32(row-3))
					case types.BigInt:
						base.SetUint64(row, math.MaxUint64-uint64(row))
					case types.Double:
						base.SetFloat64(row, []float64{math.Copysign(0, -1), math.NaN(), math.Inf(1), -1.5, 3}[row])
					case types.Decimal:
						base.SetInt128(row, types.Int128{Hi: -1, Lo: math.MaxUint64 - uint64(row)})
					case types.Varchar:
						if err := base.SetString(row, []string{"", "短", "long string exceeding inline storage", "\x00\xff", "last"}[row]); err != nil {
							t.Fatal(err)
						}
					}
				}
				base.SetNull(4)
				source := base
				if kind == "constant" {
					constant, err := vector.NewConstant(scope, bm, typ, 5)
					if err != nil {
						t.Fatal(err)
					}
					if err := vector.CopyValue(base, 2, constant, 0); err != nil {
						t.Fatal(err)
					}
					source = constant
				} else if kind == "dictionary" {
					first, err := vector.NewOwnedDictionary(scope, base, vector.SelectionFrom([]uint32{4, 2, 0, 3, 1}))
					if err != nil {
						t.Fatal(err)
					}
					source, err = vector.NewOwnedDictionary(scope, first, vector.SelectionFrom([]uint32{2, 0, 4, 1, 3}))
					if err != nil {
						t.Fatal(err)
					}
				}
				want := newVector(t, scope, bm, typ, 5)
				for row := 0; row < 5; row++ {
					if err := copyParquetScalar(want, row, source, row, typ); err != nil {
						t.Fatal(err)
					}
				}
				writer, err := NewParquetWriter([]Column{{Name: "v", Type: typ}}, ParquetWriteOptions{RowGroupRows: 7, PageRows: 2, PageBufferSize: 32}, scope, bm, &captureSink{})
				if err != nil {
					t.Fatal(err)
				}
				chunk, err := vector.NewDataChunk(source)
				if err != nil {
					t.Fatal(err)
				}
				if err := writer.WriteChunk(context.Background(), chunk); err != nil {
					t.Fatal(err)
				}
				chunk.Release()
				if kind == "constant" {
					base.Release()
				}
				var encoded [2][]byte
				for i, v := range []*vector.Vector{want, writer.rows[0]} {
					page, err := newParquetPageBuffer(scope, bm, 32)
					if err != nil {
						t.Fatal(err)
					}
					if err := encodeDefinitionLevels(page, v, 0, 5); err != nil {
						t.Fatal(err)
					}
					if err := encodeParquetPlain(page, v, typ, 0, 5); err != nil {
						t.Fatal(err)
					}
					encoded[i] = append([]byte(nil), page.Bytes()...)
					page.Release()
				}
				if !bytes.Equal(encoded[0], encoded[1]) {
					t.Fatalf("materialized bytes differ: %x != %x", encoded[0], encoded[1])
				}
				want.Release()
				if _, err := writer.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				if scope.Used() != 0 {
					t.Fatalf("managed leak=%d", scope.Used())
				}
			})
		}
	}
}

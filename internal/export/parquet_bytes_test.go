package export

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

func TestParquetPlainBorrowedBytes(t *testing.T) {
	values := []string{"", "short", "\x00\xff\x80", "中文\x00", strings.Repeat("long-value", 512)}
	for _, binaryValue := range []bool{false, true} {
		for _, kind := range []string{"flat", "constant", "constant_null", "dictionary"} {
			t.Run(fmt.Sprintf("binary=%v/%s", binaryValue, kind), func(t *testing.T) {
				scope := memory.NewManager(1 << 20).NewScope()
				buffers := memory.NewBufferManager(nil)
				typ := types.LogicalType{ID: types.Varchar, Binary: binaryValue}
				base, err := vector.NewFlat(scope, buffers, typ, len(values)+1)
				if err != nil {
					t.Fatal(err)
				}
				defer base.Release()
				for i, value := range values {
					if err := base.SetString(i, value); err != nil {
						t.Fatal(err)
					}
				}
				base.SetNull(len(values))
				input := base
				indices := []int{0, 1, 2, 3, 4, 5}
				if kind == "constant" || kind == "constant_null" {
					input, err = vector.NewConstant(scope, buffers, typ, 6)
					if err == nil {
						err = input.SetString(0, values[4])
					}
					indices = []int{4, 4, 4, 4, 4, 4}
					if kind == "constant_null" && err == nil {
						input.SetNull(0)
						indices = []int{5, 5, 5, 5, 5, 5}
					}
				} else if kind == "dictionary" {
					first, e := vector.NewDictionary(base, vector.SelectionFrom([]uint32{5, 4, 2, 0, 3, 1}))
					if e != nil {
						t.Fatal(e)
					}
					defer first.Release()
					input, err = vector.NewDictionary(first, vector.SelectionFrom([]uint32{1, 0, 3, 2, 5, 4}))
					indices = []int{4, 5, 0, 2, 1, 3}
				}
				if err != nil {
					t.Fatal(err)
				}
				if input != base {
					defer input.Release()
				}
				for _, span := range [][2]int{{0, 6}, {1, 4}, {5, 1}, {6, 0}} {
					page, err := newParquetPageBuffer(scope, buffers, 8)
					if err != nil {
						t.Fatal(err)
					}
					var want bytes.Buffer
					for _, index := range indices[span[0] : span[0]+span[1]] {
						if index == len(values) {
							continue
						}
						var length [4]byte
						binary.LittleEndian.PutUint32(length[:], uint32(len(values[index])))
						want.Write(length[:])
						want.WriteString(values[index])
					}
					if err := encodeParquetPlain(page, input, typ, span[0], span[1]); err != nil || !bytes.Equal(page.Bytes(), want.Bytes()) {
						t.Fatalf("span=%v bytes differ: err=%v", span, err)
					}
					page.Release()
				}
				// Release in dependency order, then ensure no managed ownership remains.
				if input != base {
					input.Release()
				}
				base.Release()
				if scope.Used() != 0 {
					t.Fatalf("managed leak=%d", scope.Used())
				}
			})
		}
	}
}

func TestParquetPlainBytesExactBudgetAndRetry(t *testing.T) {
	buffers := memory.NewBufferManager(nil)
	inputScope := memory.NewManager(1 << 20).NewScope()
	typ := types.LogicalType{ID: types.Varchar, Binary: true}
	input, err := vector.NewFlat(inputScope, buffers, typ, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Release()
	if err := input.SetString(0, strings.Repeat("a", 20)); err != nil {
		t.Fatal(err)
	}
	input.SetNull(1)
	if err := input.SetString(2, strings.Repeat("b", 20)); err != nil {
		t.Fatal(err)
	}
	// Three existing prefix bytes plus two length-prefixed, non-NULL values.
	scope := memory.NewManager(51).NewScope()
	page, err := newParquetPageBuffer(scope, buffers, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer page.Release()
	prefix := []byte{9, 8, 7}
	if err := page.Append(prefix); err != nil {
		t.Fatal(err)
	}
	hold, err := scope.Reserve(memory.TagMetadata, 1)
	if err != nil {
		t.Fatal(err)
	}
	err = encodeParquetPlain(page, input, typ, 0, 3)
	if !errors.Is(err, memory.ErrLimitExceeded) || !bytes.Equal(page.Bytes(), prefix) {
		t.Fatalf("budget failure err=%v page=%x", err, page.Bytes())
	}
	hold.Release()
	if err := encodeParquetPlain(page, input, typ, 0, 3); err != nil {
		t.Fatal(err)
	}
	input.Release()
	want := append([]byte(nil), prefix...)
	for _, value := range []string{strings.Repeat("a", 20), strings.Repeat("b", 20)} {
		var length [4]byte
		binary.LittleEndian.PutUint32(length[:], uint32(len(value)))
		want = append(want, length[:]...)
		want = append(want, value...)
	}
	if !bytes.Equal(page.Bytes(), want) || scope.Used() != 51 || inputScope.Used() != 0 {
		t.Fatalf("page bytes=%d scope=%d input=%d", page.Len(), scope.Used(), inputScope.Used())
	}
	page.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed leak=%d", scope.Used())
	}
}

func BenchmarkParquetPlainBytes(b *testing.B) {
	scope := memory.NewManager(1 << 20).NewScope()
	buffers := memory.NewBufferManager(nil)
	typ := types.LogicalType{ID: types.Varchar}
	input, err := vector.NewFlat(scope, buffers, typ, 2048)
	if err != nil {
		b.Fatal(err)
	}
	for row := 0; row < input.Count(); row++ {
		if err := input.SetString(row, fmt.Sprintf("row-%08d-long-value", row)); err != nil {
			b.Fatal(err)
		}
		if row%10 == 0 {
			input.SetNull(row)
		}
	}
	page, err := newParquetPageBuffer(scope, buffers, 64<<10)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		page.used = 0
		if err := encodeParquetPlain(page, input, typ, (i%4)*512, 512); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	page.Release()
	input.Release()
	if scope.Used() != 0 {
		b.Fatalf("managed leak=%d", scope.Used())
	}
}

package parquet

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func TestDictionaryReadMatchesNextAcrossRunsAndWidths(t *testing.T) {
	for width := 0; width <= 32; width++ {
		mask := uint32((uint64(1) << uint(width)) - 1)
		value := uint32(0xa5a5a5a5) & mask
		data := []byte{byte(width), 10} // Five RLE values.
		for b := 0; b < (width+7)/8; b++ {
			data = append(data, byte(value>>uint(8*b)))
		}
		data = append(data, 3) // Eight packed values.
		packed := make([]byte, width)
		for row := 0; row < 8; row++ {
			x := (uint32(row)*7919 + 17) & mask
			for bit := 0; bit < width; bit++ {
				if x&(1<<uint(bit)) != 0 {
					position := row*width + bit
					packed[position>>3] |= 1 << uint(position&7)
				}
			}
		}
		data = append(data, packed...)
		data = append(data, 6) // Three RLE values.
		for b := 0; b < (width+7)/8; b++ {
			data = append(data, byte(value>>uint(8*b)))
		}
		for _, sizes := range [][]int{{16}, {1, 3, 7, 5}, {4, 4, 4, 4}} {
			batch, scalar := newDictionaryIndexStream(data), newDictionaryIndexStream(data)
			for _, n := range sizes {
				out := make([]uint32, n)
				if err := batch.Read(out); err != nil {
					t.Fatalf("width=%d sizes=%v err=%v", width, sizes, err)
				}
				for i, got := range out {
					want, err := scalar.Next()
					if err != nil || got != want {
						t.Fatalf("width=%d row=%d got=%d want=%d err=%v", width, i, got, want, err)
					}
				}
			}
			if err := batch.Read(make([]uint32, 1)); err == nil {
				t.Fatal("exhausted dictionary accepted")
			}
		}
	}
	for _, data := range [][]byte{nil, {33}, {8, 3}, {8, 0}} {
		if err := newDictionaryIndexStream(data).Read(make([]uint32, 2)); err == nil {
			t.Fatalf("malformed dictionary accepted: %x", data)
		}
	}
}

func TestScalarBatchPackedDefinitionsAndNullRuns(t *testing.T) {
	for _, definitions := range [][]byte{{3, 0xf5}, {6, 0, 10, 1}} {
		levels, err := newLevelsV2(definitions, 1)
		if err != nil {
			t.Fatal(err)
		}
		var validity [8]bool
		body := make([]byte, 4)
		binary.LittleEndian.PutUint32(body, uint32(len(definitions)))
		body = append(body, definitions...)
		values := int64(0)
		for row := 0; row < 8; row++ {
			def, err := levels.Next()
			if err != nil {
				t.Fatal(err)
			}
			validity[row] = def == 1
			if def == 1 {
				var raw [4]byte
				binary.LittleEndian.PutUint32(raw[:], uint32(100+row))
				body = append(body, raw[:]...)
				values++
			}
		}
		data := append(cursorDataPageHeader(8, len(body), EncodingPlain), body...)
		driver := &recordingDriver{data: data}
		uri, _ := vfs.ParseURI("mem://x/mixed-batch")
		m := memory.NewManager(1 << 20)
		scope := m.NewScope()
		bm := memory.NewBufferManager(nil)
		cursor, err := NewColumnCursor(driver, uri, vfs.ObjectIdentity{ETag: "e", Size: int64(len(data))}, &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 8, TotalCompressedSize: int64(len(data))}, optionalInt32Leaf(), scope, bm)
		if err != nil {
			t.Fatal(err)
		}
		for base := 0; base < 8; {
			out, err := cursor.ReadScalar(context.Background(), 3)
			if err != nil {
				t.Fatal(err)
			}
			for row := 0; row < out.Count(); row++ {
				x, valid := out.Int32(row)
				if valid != validity[base+row] || valid && x != int32(100+base+row) {
					t.Fatalf("defs=%x row=%d value=%d valid=%v", definitions, base+row, x, valid)
				}
			}
			base += out.Count()
			out.Release()
		}
		cursor.Close()
		if cursor.Metrics().DataValues != values || cursor.Consumed() != 8 {
			t.Fatalf("values=%d consumed=%d", cursor.Metrics().DataValues, cursor.Consumed())
		}
		if m.Used() != 0 {
			t.Fatalf("managed leak=%d", m.Used())
		}
	}
}

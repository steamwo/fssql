package parquet

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func TestPackedWordMatchesBitReference(t *testing.T) {
	for width := 0; width <= 32; width++ {
		data := make([]byte, (width*64+7)/8)
		for i := range data {
			data[i] = byte(i*71 + 19)
		}
		for row := 0; row < 64; row++ {
			var want uint32
			for bit := 0; bit < width; bit++ {
				position := row*width + bit
				if data[position>>3]&(1<<uint(position&7)) != 0 {
					want |= 1 << uint(bit)
				}
			}
			if got := packedWord(data, row*width, width); got != want {
				t.Fatalf("width=%d row=%d got=%d want=%d", width, row, got, want)
			}
		}
	}
}

func TestScalarBatchRequiredAndOptionalAcrossPages(t *testing.T) {
	for _, optional := range []bool{false, true} {
		var data []byte
		for page := 0; page < 2; page++ {
			var body []byte
			if optional {
				body = make([]byte, 6)
				binary.LittleEndian.PutUint32(body[:4], 2)
				body[4] = 80
				body[5] = 1
			} // 40 defined rows.
			for row := 0; row < 40; row++ {
				var value [4]byte
				binary.LittleEndian.PutUint32(value[:], uint32(page*40+row))
				body = append(body, value[:]...)
			}
			data = append(data, cursorDataPageHeader(40, len(body), EncodingPlain)...)
			data = append(data, body...)
		}
		driver := &recordingDriver{data: data}
		uri, _ := vfs.ParseURI("mem://x/batch")
		m := memory.NewManager(1 << 20)
		scope := m.NewScope()
		bm := memory.NewBufferManager(nil)
		leaf := requiredInt32Leaf()
		if optional {
			leaf = optionalInt32Leaf()
		}
		cursor, err := NewColumnCursor(driver, uri, vfs.ObjectIdentity{ETag: "e", Size: int64(len(data))}, &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 80, TotalCompressedSize: int64(len(data))}, leaf, scope, bm)
		if err != nil {
			t.Fatal(err)
		}
		for base := 0; base < 80; {
			out, err := cursor.ReadScalar(context.Background(), 23)
			if err != nil {
				t.Fatal(err)
			}
			for row := 0; row < out.Count(); row++ {
				if x, ok := out.Int32(row); !ok || x != int32(base+row) {
					t.Fatalf("optional=%v row=%d value=%d valid=%v", optional, base+row, x, ok)
				}
			}
			base += out.Count()
			out.Release()
		}
		cursor.Close()
		if cursor.Metrics().DataValues != 80 {
			t.Fatal("bad value metrics")
		}
		if m.Used() != 0 {
			t.Fatalf("managed leak=%d", m.Used())
		}
	}
}

func TestFixedBatchPreservesConversionFallbackAndTruncation(t *testing.T) {
	m := memory.NewManager(1 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	v, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.TimestampType}, 2)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &SchemaNode{Element: SchemaElement{Logical: &LogicalType{Kind: LogicalTimestamp, Timestamp: &TimestampLogical{Unit: UnitMillis}}}}
	s := newValueStream(make([]byte, 8), TypeInt64, leaf)
	if handled, err := s.writeFixedBatch(v, 0, 1); err != nil || handled || s.off != 0 {
		t.Fatalf("conversion handled=%v err=%v", handled, err)
	}
	s.leaf = nil
	if handled, err := s.writeFixedBatch(v, 0, 2); !handled || err == nil {
		t.Fatal("truncated batch accepted")
	}
	v.Release()
	if m.Used() != 0 {
		t.Fatalf("managed leak=%d", m.Used())
	}
}

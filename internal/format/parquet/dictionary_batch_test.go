package parquet

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/bits"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type dictionaryBatchTestPage struct {
	indices []int // -1 denotes NULL
	plain   bool
}

func dictionaryBatchCursor(t *testing.T, dictionary []string, pages []dictionaryBatchTestPage, blob bool, bm *memory.BufferManager) (*ColumnCursor, *memory.Manager) {
	t.Helper()
	var dictionaryBody []byte
	for _, value := range dictionary {
		dictionaryBody = append(dictionaryBody, byteArrayValue(value)...)
	}
	data := append(cursorDictionaryPageHeader(len(dictionary), len(dictionaryBody)), dictionaryBody...)
	dataOffset := int64(len(data))
	total := 0
	for _, page := range pages {
		levels := make([]int, len(page.indices))
		for row, index := range page.indices {
			if index >= 0 {
				levels[row] = 1
			}
		}
		defs := rleLevels(levels)
		body := make([]byte, 4)
		binary.LittleEndian.PutUint32(body, uint32(len(defs)))
		body = append(body, defs...)
		encoding := EncodingPlain
		if !page.plain {
			encoding = EncodingRLEDictionary
			width := max(1, bits.Len(uint(len(dictionary)-1)))
			body = append(body, byte(width))
			for _, index := range page.indices {
				if index < 0 {
					continue
				}
				body = append(body, 2) // one-value RLE run
				for byteIndex := 0; byteIndex < (width+7)/8; byteIndex++ {
					body = append(body, byte(uint(index)>>uint(8*byteIndex)))
				}
			}
		} else {
			for _, index := range page.indices {
				if index >= 0 {
					body = append(body, byteArrayValue(dictionary[index])...)
				}
			}
		}
		data = append(data, cursorDataPageHeader(len(page.indices), len(body), encoding)...)
		data = append(data, body...)
		total += len(page.indices)
	}
	manager := memory.NewManager(4 << 20)
	scope := manager.NewScope()
	if bm == nil {
		bm = memory.NewBufferManager(nil)
	}
	leaf := optionalVarcharLeaf()
	leaf.Logical.Binary = blob
	uri, _ := vfs.ParseURI("mem://dictionary/batches")
	dictOffset := int64(0)
	cursor, err := NewColumnCursor(&recordingDriver{data: data}, uri, vfs.ObjectIdentity{ETag: "dictionary", Size: int64(len(data))}, &ColumnMetaData{Type: TypeByteArray, Codec: CodecUncompressed, NumValues: int64(total), TotalCompressedSize: int64(len(data)), DataPageOffset: dataOffset, DictionaryPageOffset: &dictOffset}, leaf, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cursor.Close()
		if err := scope.Close(); err != nil {
			t.Fatal(err)
		}
		if manager.Used() != 0 {
			t.Fatalf("managed leak=%d", manager.Used())
		}
	})
	return cursor, manager
}

func assertDictionaryBatchValues(t *testing.T, out *vector.Vector, dictionary []string, indices []int) {
	t.Helper()
	if out.Count() != len(indices) {
		t.Fatalf("count=%d want=%d", out.Count(), len(indices))
	}
	for row, index := range indices {
		got, valid := out.Bytes(row)
		if valid != (index >= 0) || valid && string(got) != dictionary[index] {
			t.Fatalf("row %d value=%q valid=%v index=%d", row, got, valid, index)
		}
	}
}

func TestOwnedDictionaryBatchSurvivesCursorClose(t *testing.T) {
	for _, blob := range []bool{false, true} {
		t.Run(fmt.Sprintf("blob%v", blob), func(t *testing.T) {
			dictionary := []string{"short", "long dictionary value with \x00 bytes"}
			indices := []int{0, 1, -1, 1, 0, -1, 0, 1, 1, 0, -1, 1}
			cursor, manager := dictionaryBatchCursor(t, dictionary, []dictionaryBatchTestPage{{indices: indices}}, blob, nil)
			out, err := cursor.ReadScalar(context.Background(), len(indices))
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			if out.Kind != vector.Dictionary || out.Type.Binary != blob {
				t.Fatalf("kind=%v blob=%v", out.Kind, out.Type.Binary)
			}
			cursor.Close()
			assertDictionaryBatchValues(t, out, dictionary, indices)
			if cursor.Consumed() != int64(len(indices)) || cursor.Metrics().DataValues != 9 {
				t.Fatalf("consumed=%d values=%d", cursor.Consumed(), cursor.Metrics().DataValues)
			}
			out.Release()
			if manager.Used() != 0 {
				t.Fatalf("owned output leak=%d", manager.Used())
			}
		})
	}
}

func TestDictionaryBatchFlatFallbackBeforeConsumption(t *testing.T) {
	for _, tc := range []struct {
		name       string
		dictionary []string
		pages      []dictionaryBatchTestPage
	}{
		{name: "page-boundary", dictionary: []string{"a", "b"}, pages: []dictionaryBatchTestPage{{indices: []int{0, 1, -1, 0}}, {indices: []int{1, 0, -1, 1, 0, 1, 1, 0}, plain: true}}},
		{name: "plain", dictionary: []string{"a", "b"}, pages: []dictionaryBatchTestPage{{indices: []int{0, 1, -1, 0, 1, 0, -1, 1}, plain: true}}},
		{name: "long-dictionary", dictionary: []string{strings.Repeat("x", maxBatchDictionaryBytes+1)}, pages: []dictionaryBatchTestPage{{indices: []int{0, 0, 0, 0, 0, 0, 0, 0}}}},
		{name: "large-dictionary", dictionary: func() []string {
			values := make([]string, 257)
			for i := range values {
				values[i] = fmt.Sprint(i)
			}
			return values
		}(), pages: []dictionaryBatchTestPage{{indices: []int{0, 256, 0, 256, 0, 256, 0, 256}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cursor, _ := dictionaryBatchCursor(t, tc.dictionary, tc.pages, false, nil)
			var indices []int
			for _, page := range tc.pages {
				indices = append(indices, page.indices...)
			}
			out, err := cursor.ReadScalar(context.Background(), len(indices))
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			if out.Kind != vector.Flat {
				t.Fatalf("fallback kind=%v", out.Kind)
			}
			assertDictionaryBatchValues(t, out, tc.dictionary, indices)
		})
	}
}

func TestDictionaryBatchAllocationFallbackDoesNotConsumeLevels(t *testing.T) {
	fail := false
	bm := memory.NewBufferManager(func(n int) ([]byte, error) {
		if fail {
			fail = false
			return nil, memory.ErrLimitExceeded
		}
		return make([]byte, n), nil
	})
	dictionary := []string{"a", "b"}
	indices := []int{0, -1, 1, 0, -1, 1, 1, 0}
	cursor, _ := dictionaryBatchCursor(t, dictionary, []dictionaryBatchTestPage{{indices: indices}}, false, bm)
	if err := cursor.loadPage(context.Background()); err != nil {
		t.Fatal(err)
	}
	fail = true
	out, err := cursor.ReadScalar(context.Background(), len(indices))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Kind != vector.Flat {
		t.Fatalf("allocation fallback kind=%v", out.Kind)
	}
	assertDictionaryBatchValues(t, out, dictionary, indices)
	if cursor.Consumed() != 8 || cursor.Metrics().DataValues != 6 {
		t.Fatalf("consumed=%d values=%d", cursor.Consumed(), cursor.Metrics().DataValues)
	}
}

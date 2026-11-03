package parquet

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
)

func TestSnappyBatchCopyMatchesOverlappingByteReference(t *testing.T) {
	for offset := 1; offset <= 64; offset++ {
		for length := 0; length <= 512; length += 7 {
			got := make([]byte, 64+length+8)
			for i := range got {
				got[i] = byte(i*31 + 19)
			}
			want := append([]byte(nil), got...)
			for i := 0; i < length; i++ {
				want[64+i] = want[64+i-offset]
			}
			copySnappyMatch(got, 64, offset, length)
			if !bytes.Equal(got, want) {
				t.Fatalf("offset=%d length=%d", offset, length)
			}
			// The legacy helper must also retain its append/grow behavior.
			legacy, err := snappyCopy(append([]byte(nil), want[:64]...), offset, length, 64+length)
			if err != nil || !bytes.Equal(legacy, want[:64+length]) {
				t.Fatalf("legacy offset=%d length=%d err=%v", offset, length, err)
			}
		}
	}
}

func TestManagedSnappyBatchCopyAllTagForms(t *testing.T) {
	m := memory.NewManager(1 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	for _, form := range []byte{1, 2, 3} {
		for offset := 1; offset <= 32; offset++ {
			length := 64
			if form == 1 {
				length = 11
			}
			prefix := []byte("0123456789abcdefghijklmnopqrstuv")
			var header [10]byte
			n := binary.PutUvarint(header[:], uint64(len(prefix)+length))
			data := append([]byte(nil), header[:n]...)
			data = append(data, byte((len(prefix)-1)<<2))
			data = append(data, prefix...)
			switch form {
			case 1:
				data = append(data, byte((length-4)<<2)|1, byte(offset))
			case 2:
				data = append(data, byte((length-1)<<2)|2, byte(offset), 0)
			case 3:
				data = append(data, byte((length-1)<<2)|3, byte(offset), 0, 0, 0)
			}
			want := append([]byte(nil), prefix...)
			for i := 0; i < length; i++ {
				want = append(want, want[len(want)-offset])
			}
			got, err := DecompressManaged(scope, bm, CodecSnappy, data, len(want))
			if err != nil {
				t.Fatalf("form=%d offset=%d err=%v", form, offset, err)
			}
			if !bytes.Equal(got.Bytes(), want) {
				t.Fatalf("form=%d offset=%d value mismatch", form, offset)
			}
			got.Release()
		}
	}
	for _, data := range [][]byte{{6, 0, 'a', 5, 0}, {6, 0, 'a', 5, 2}, {5, 0, 'a', 5, 1}} {
		if out, err := DecompressManaged(scope, bm, CodecSnappy, data, int(data[0])); err == nil {
			out.Release()
			t.Fatalf("invalid snappy copy accepted: %x", data)
		}
	}
	if m.Used() != 0 {
		t.Fatalf("managed leak=%d", m.Used())
	}
}

func BenchmarkSnappyCopyMatch(b *testing.B) {
	for _, batch := range []bool{false, true} {
		name := "byte"
		if batch {
			name = "batch"
		}
		b.Run(name, func(b *testing.B) {
			var dst [128]byte
			dst[0] = 'x'
			b.SetBytes(64)
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				if batch {
					copySnappyMatch(dst[:], 1, 1, 64)
				} else {
					for i := 1; i <= 64; i++ {
						dst[i] = dst[i-1]
					}
				}
			}
		})
	}
}

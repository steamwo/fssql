package compression

import (
	"bytes"
	"compress/gzip"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/format/scan"
)

func env(t *testing.T) (*memory.Manager, *memory.Scope, *memory.BufferManager) {
	t.Helper()
	m := memory.NewManager(16 << 20)
	return m, m.NewScope(), memory.NewBufferManager(nil)
}

func gzipEncode(p []byte) ([]byte, error) {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	if _, err := w.Write(p); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func gzipBytes(t testing.TB, p []byte) []byte {
	t.Helper()
	b, err := gzipEncode(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestGzipRoundTripAndCorruption(t *testing.T) {
	m, s, bm := env(t)
	plain := bytes.Repeat([]byte("row,1\n"), 10000)
	out, err := (Gzip{}).Decode(gzipBytes(t, plain), s, bm)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), plain) {
		t.Fatal("mismatch")
	}
	out.Release()
	bad := gzipBytes(t, plain)
	bad[len(bad)-3] ^= 0xff
	if _, err = (Gzip{}).Decode(bad, s, bm); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err=%v", err)
	}
	if m.Used() != 0 {
		t.Fatalf("leak=%d", m.Used())
	}
}

func TestCodecCapabilities(t *testing.T) {
	if (Gzip{}).Capability() != scan.CompressionSequential || (Zstd{}).Capability() != scan.CompressionSequential || (Identity{}).Capability() != scan.CompressionIdentity {
		t.Fatal("capability mismatch")
	}
}

func TestZstdBaseline(t *testing.T) {
	if !ZstdAvailable() {
		t.Skip("zstd backend unavailable")
	}
	m, s, bm := env(t)
	frame := []byte{40, 181, 47, 253, 36, 11, 89, 0, 0, 104, 101, 108, 108, 111, 32, 122, 115, 116, 100, 10, 108, 87, 249, 81}
	out, err := (Zstd{}).Decode(frame, s, bm)
	if err != nil {
		t.Fatal(err)
	}
	if string(out.Bytes()) != "hello zstd\n" {
		t.Fatalf("%q", out.Bytes())
	}
	out.Release()
	frame[10] ^= 0xff
	if _, err = (Zstd{}).Decode(frame, s, bm); err == nil {
		t.Fatal("expected corrupt stream")
	}
	if m.Used() != 0 {
		t.Fatalf("leak=%d", m.Used())
	}
}

func TestAllocationFailureNoLeak(t *testing.T) {
	m := memory.NewManager(8)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	_, err := (Gzip{}).Decode(gzipBytes(t, []byte("a lot more than eight bytes")), s, bm)
	if err == nil {
		t.Fatal("expected limit")
	}
	if m.Used() != 0 {
		t.Fatalf("leak=%d", m.Used())
	}
}

func BenchmarkGzipDecode(b *testing.B) {
	plain := bytes.Repeat([]byte("1234567890,abcdefghij\n"), 50000)
	src, err := gzipEncode(plain)
	if err != nil {
		b.Fatal(err)
	}
	m := memory.NewManager(1 << 30)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	b.SetBytes(int64(len(plain)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out, err := (Gzip{}).Decode(src, s, bm)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}

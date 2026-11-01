package compression_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/format/compression"
	"github.com/steamwo/fssql/internal/format/csv"
	"github.com/steamwo/fssql/internal/format/scan"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func gzipBytes(t *testing.T, plain []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	zw := gzip.NewWriter(&b)
	if _, err := zw.Write(plain); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
func decompress(t *testing.T, c compression.Codec, src []byte, max int64) ([]byte, int64) {
	t.Helper()
	m := memory.NewManager(32 << 20)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	b, err := c.Decompress(context.Background(), src, max, s, bm)
	if err != nil {
		t.Fatal(err)
	}
	out := append([]byte(nil), b.Bytes()...)
	b.Release()
	if s.Used() != 0 {
		t.Fatalf("managed leak %d", s.Used())
	}
	return out, m.Peak()
}
func TestGzipAndZstdLogicalResultEqualsPlain(t *testing.T) {
	plain := []byte("1,alpha\n2,beta\n")
	gz := gzipBytes(t, plain)
	zhex := "28b52ffd240f790000312c616c7068610a322c626574610acb559942"
	zsrc, _ := hex.DecodeString(zhex)
	gout, _ := decompress(t, compression.Gzip(), gz, 1<<20)
	zout, _ := decompress(t, compression.Zstd(), zsrc, 1<<20)
	if !bytes.Equal(gout, plain) || !bytes.Equal(zout, plain) {
		t.Fatalf("logical bytes differ gzip=%q zstd=%q", gout, zout)
	}
	schema := []types.LogicalType{{ID: types.BigInt}, {ID: types.Varchar}}
	for name, data := range map[string][]byte{"plain": plain, "gzip": gout, "zstd": zout} {
		m := memory.NewManager(1 << 20)
		scope := m.NewScope()
		bm := memory.NewBufferManager(nil)
		chunk, _, err := csv.DecodeChunk(data, schema, csv.DecodeOptions{Lex: csv.DefaultLexConfig(), Final: true}, scope, bm)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if v, _ := chunk.Columns[0].Int64(1); v != 2 {
			t.Fatalf("%s id=%d", name, v)
		}
		chunk.Release()
		if scope.Used() != 0 {
			t.Fatalf("%s leak=%d", name, scope.Used())
		}
	}
}

func TestGzipCorruptionAndLimitReleaseMemory(t *testing.T) {
	plain := bytes.Repeat([]byte("abcdef"), 1000)
	gz := gzipBytes(t, plain)
	m := memory.NewManager(32 << 20)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	bad := append([]byte(nil), gz...)
	bad[len(bad)-1] ^= 0xff
	if b, err := compression.Gzip().Decompress(context.Background(), bad, 1<<20, s, bm); err == nil {
		if b != nil {
			b.Release()
		}
		t.Fatal("expected gzip corruption")
	}
	if s.Used() != 0 {
		t.Fatalf("corruption leak=%d", s.Used())
	}
	if b, err := compression.Gzip().Decompress(context.Background(), gz, 128, s, bm); err == nil {
		if b != nil {
			b.Release()
		}
		t.Fatal("expected output limit")
	}
	if s.Used() != 0 {
		t.Fatalf("limit leak=%d", s.Used())
	}
}
func TestCompressionCapabilityComposition(t *testing.T) {
	caps := vfs.Capabilities(vfs.RandomRead, vfs.ParallelRandomRead)
	if got := scan.ResolveScanCapability(caps, compression.Gzip().Capability(), scan.FormatParallelBoundary); got.Mode != scan.ScanSequential || got.Blocker != scan.BlockerCompression {
		t.Fatalf("gzip decision=%+v", got)
	}
	if got := scan.ResolveScanCapability(caps, compression.Identity().Capability(), scan.FormatParallelBoundary); got.Mode != scan.ScanParallelRange {
		t.Fatalf("identity decision=%+v", got)
	}
	if compression.Zstd().Capability() != scan.CompressionSequential {
		t.Fatal("zstd baseline must be sequential")
	}
}

func BenchmarkGzipDecode(b *testing.B) {
	plain := bytes.Repeat([]byte("id,name,description\n1,alpha,abcdefghijklmnopqrstuvwxyz\n"), 2048)
	gz := gzipBytes(&testing.T{}, plain)
	m := memory.NewManager(256 << 20)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	b.ReportAllocs()
	b.SetBytes(int64(len(plain)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := compression.Gzip().Decompress(context.Background(), gz, 64<<20, s, bm)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}
func BenchmarkZstdDecode(b *testing.B) {
	zhex := "28b52ffd240f790000312c616c7068610a322c626574610acb559942"
	frame, _ := hex.DecodeString(zhex)
	src := bytes.Repeat(frame, 4096)
	m := memory.NewManager(128 << 20)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	b.ReportAllocs()
	b.SetBytes(15 * 4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := compression.Zstd().Decompress(context.Background(), src, 1<<20, s, bm)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}

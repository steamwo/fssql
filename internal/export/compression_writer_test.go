package export

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	phasecompression "github.com/steamwo/fssql/internal/format/compression"
)

func TestGzipCompressionSinkLogicalEquality(t *testing.T) {
	mm := memory.NewManager(4 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	sink := &captureSink{}
	z, err := NewCompressionSink("gzip", scope, bm, sink)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("row,hello\n"), 4096)
	if n, err := z.Write(context.Background(), payload); err != nil || n != len(payload) {
		t.Fatalf("write n=%d err=%v", n, err)
	}
	if _, err := z.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(sink.buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	_ = zr.Close()
	if !bytes.Equal(got, payload) {
		t.Fatal("gzip logical result mismatch")
	}
	if scope.Used() != 0 {
		t.Fatalf("gzip reservation leak: %d", scope.Used())
	}
}

func TestZstdRawCompressionSinkInteropWithPhase3Decoder(t *testing.T) {
	mm := memory.NewManager(4 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	sink := &captureSink{}
	z, err := NewCompressionSink("zstd", scope, bm, sink)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("zstd-phase4-"), 20000)
	if n, err := z.Write(context.Background(), payload); err != nil || n != len(payload) {
		t.Fatalf("write n=%d err=%v", n, err)
	}
	if _, err := z.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	decoded, err := phasecompression.Zstd().Decompress(context.Background(), sink.buf.Bytes(), int64(len(payload)+1), scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded.Bytes(), payload) {
		t.Fatal("zstd logical result mismatch")
	}
	decoded.Release()
	if scope.Used() != 0 {
		t.Fatalf("zstd buffer leak: %d", scope.Used())
	}
}

func TestCompressionSinkAllocationAndCancellation(t *testing.T) {
	bm := memory.NewBufferManager(nil)
	mm := memory.NewManager(32 << 10)
	scope := mm.NewScope()
	if _, err := NewCompressionSink("gzip", scope, bm, &captureSink{}); err == nil {
		t.Fatal("expected gzip reservation failure")
	}
	if scope.Used() != 0 {
		t.Fatalf("gzip failure leak=%d", scope.Used())
	}
	if _, err := NewCompressionSink("zstd", scope, bm, &captureSink{}); err == nil {
		t.Fatal("expected zstd buffer allocation failure")
	}
	if scope.Used() != 0 {
		t.Fatalf("zstd failure leak=%d", scope.Used())
	}

	mm2 := memory.NewManager(2 << 20)
	scope2 := mm2.NewScope()
	z, err := NewCompressionSink("gzip", scope2, bm, &captureSink{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := z.Write(ctx, []byte("x")); err != context.Canceled {
		t.Fatalf("expected cancel, got %v", err)
	}
	if err := z.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if scope2.Used() != 0 {
		t.Fatalf("cancel leak=%d", scope2.Used())
	}
}

func TestZstdFlushProducesValidContinuation(t *testing.T) {
	mm := memory.NewManager(2 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	sink := &captureSink{}
	z, err := NewCompressionSink("zstd", scope, bm, sink)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = z.Write(context.Background(), []byte("first"))
	if err := z.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, _ = z.Write(context.Background(), []byte("second"))
	if _, err := z.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	decoded, err := phasecompression.Zstd().Decompress(context.Background(), sink.buf.Bytes(), 64, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(decoded.Bytes()); got != "firstsecond" {
		t.Fatalf("got %q", got)
	}
	decoded.Release()
	if scope.Used() != 0 {
		t.Fatalf("leak=%d", scope.Used())
	}
}

func BenchmarkGzipCompressionSink(b *testing.B) {
	payload := bytes.Repeat([]byte("abcdefghij"), 4096)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mm := memory.NewManager(4 << 20)
		scope := mm.NewScope()
		bm := memory.NewBufferManager(nil)
		sink := &captureSink{}
		z, _ := NewCompressionSink("gzip", scope, bm, sink)
		_, _ = z.Write(context.Background(), payload)
		_, _ = z.Close(context.Background())
	}
}

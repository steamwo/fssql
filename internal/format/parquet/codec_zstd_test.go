package parquet

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
)

// zstd frame produced by libzstd for "hello zstd parquet fixture" x4.
const zstdFixtureHex = "28b52ffd20680d0100d068656c6c6f207a7374642070617271756574206669787475726501006ea3730c"

var zstdFixtureWant = bytes.Repeat([]byte("hello zstd parquet fixture"), 4)

func TestDecompressZstd(t *testing.T) {
	fixture, err := hex.DecodeString(zstdFixtureHex)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decompress(CodecZSTD, fixture, len(zstdFixtureWant))
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(got, zstdFixtureWant) {
		t.Fatalf("value mismatch")
	}
	if _, err := Decompress(CodecZSTD, fixture, len(zstdFixtureWant)+1); err == nil {
		t.Fatalf("expected size mismatch not rejected")
	}
}

func TestDecompressManagedZstd(t *testing.T) {
	fixture, err := hex.DecodeString(zstdFixtureHex)
	if err != nil {
		t.Fatal(err)
	}
	m := memory.NewManager(1 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	got, err := DecompressManaged(scope, bm, CodecZSTD, fixture, len(zstdFixtureWant))
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(got.Bytes(), zstdFixtureWant) {
		t.Fatalf("value mismatch")
	}
	got.Release()
	if _, err := DecompressManaged(scope, bm, CodecZSTD, fixture, len(zstdFixtureWant)+1); err == nil {
		t.Fatalf("expected size mismatch not rejected")
	}
	if _, err := DecompressManaged(scope, bm, CodecZSTD, fixture[:8], len(zstdFixtureWant)); err == nil {
		t.Fatalf("truncated frame not rejected")
	}
	if m.Used() != 0 {
		t.Fatalf("managed leak=%d", m.Used())
	}
}

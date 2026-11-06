package csv

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func TestCSVLimitAvoidsParallelBoundaryPrescan(t *testing.T) {
	driver := &trackingCSVDriver{memoryCSVDriver: newMemoryCSVDriver(t, makeParallelFixture(10000), vfs.Capabilities(vfs.RandomRead, vfs.ParallelRandomRead))}
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	m, scope, bm := testMemory(8 << 20)
	binder, err := NewSourceBinder(registry, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := binder.Bind(context.Background(), driver.uri.String(), SourceBindOptions{Inference: InferenceOptions{Header: true}})
	if err != nil {
		t.Fatal(err)
	}
	driver.reset(0)
	source, err := NewSource(context.Background(), registry, binding, ScanOptions{EnableParallel: true, TargetRangeBytes: 4096, Limit: 7, Projection: []int{0}}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	if reads, _ := driver.stats(); reads != 0 {
		t.Fatalf("constructor read %d ranges", reads)
	}
	ids := collectIDs(t, source, 4)
	if len(ids) != 7 || ids[0] != 0 || ids[6] != 6 {
		t.Fatalf("ids=%v", ids)
	}
	if total := driver.totalBytes(); total > 4096 {
		t.Fatalf("limit read %d bytes", total)
	}
	if m.Used() != 0 {
		t.Fatalf("managed leak=%d", m.Used())
	}
}

func TestCSVSerialWindowServesMultipleChunksAndCloseReleasesIt(t *testing.T) {
	driver := &trackingCSVDriver{memoryCSVDriver: newMemoryCSVDriver(t, makeParallelFixture(10000), vfs.Capabilities(vfs.RandomRead))}
	registry, _ := vfs.NewRegistry(driver)
	m, scope, bm := testMemory(8 << 20)
	binder, _ := NewSourceBinder(registry, scope, bm)
	binding, err := binder.Bind(context.Background(), driver.uri.String(), SourceBindOptions{Inference: InferenceOptions{Header: true}})
	if err != nil {
		t.Fatal(err)
	}
	driver.reset(0)
	source, err := NewSource(context.Background(), registry, binding, ScanOptions{}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		chunk, ok, err := source.Next(context.Background())
		if err != nil || !ok {
			t.Fatalf("next=%v %v", ok, err)
		}
		chunk.Release()
	}
	if reads, _ := driver.stats(); reads != 1 {
		t.Fatalf("two batches used %d requests", reads)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if m.Used() != 0 {
		t.Fatalf("managed leak=%d", m.Used())
	}
}

func TestCSVProjectedDecodeRetainsLexicalAndFieldSemantics(t *testing.T) {
	for _, input := range []string{"7,ignored,\"a\"\"b\"\r\n8,ignored,plain", "7,ignored,\"a\"\"b\"\n8,ignored,plain\n"} {
		m, scope, bm := testMemory(1 << 20)
		chunk, n, err := DecodeChunk([]byte(input), []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}, {ID: types.Varchar}}, DecodeOptions{Final: true, Projection: []int{0, 2}}, scope, bm)
		if err != nil {
			t.Fatal(err)
		}
		if n != len(input) || chunk.Cardinality != 2 {
			t.Fatalf("n=%d rows=%d", n, chunk.Cardinality)
		}
		if chunk.Columns[1].IsValid(0) {
			t.Fatal("unread column is valid")
		}
		if got, _ := chunk.Columns[2].String(0); got != "a\"b" {
			t.Fatalf("quoted=%q", got)
		}
		if got, _ := chunk.Columns[2].String(1); got != "plain" {
			t.Fatalf("unquoted=%q", got)
		}
		chunk.Release()
		if m.Used() != 0 {
			t.Fatalf("managed leak=%d", m.Used())
		}
	}
}

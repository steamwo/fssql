package parquet

import (
	"context"
	"math"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func TestExactMetadataRows(t *testing.T) {
	for _, tc := range []struct {
		meta FileMetaData
		want int64
		bad  bool
	}{
		{FileMetaData{}, 0, false},
		{FileMetaData{NumRows: 7, RowGroups: []RowGroup{{NumRows: 3}, {NumRows: 4}}}, 7, false},
		{FileMetaData{NumRows: 8, RowGroups: []RowGroup{{NumRows: 7}}}, 0, true},
		{FileMetaData{NumRows: -1}, 0, true},
		{FileMetaData{RowGroups: []RowGroup{{NumRows: -1}}}, 0, true},
		{FileMetaData{RowGroups: []RowGroup{{NumRows: math.MaxInt64}, {NumRows: 1}}}, 0, true},
	} {
		got, err := exactMetadataRows(tc.meta)
		if (err != nil) != tc.bad || (!tc.bad && got != tc.want) {
			t.Fatalf("rows=%d err=%v meta=%+v", got, err, tc.meta)
		}
	}
}

func TestMetadataCountReadsNoDataPages(t *testing.T) {
	// The row group deliberately has no readable column pages: COUNT must
	// consume only its exact row count, never instantiate a page scanner.
	meta, err := DecodeFileMetaData(minimalMetadata())
	if err != nil {
		t.Fatal(err)
	}
	meta.NumRows = 17
	meta.RowGroups = []RowGroup{{NumRows: 17}}
	encoded, err := EncodeFileMetaData(meta)
	if err != nil {
		t.Fatal(err)
	}
	uri, _ := vfs.ParseURI("mem://x/count.parquet")
	driver := &runtimeBindDriver{recordingDriver: &recordingDriver{data: parquetFile(encoded)}, uri: uri}
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := NewMetadataCache(4)
	if err != nil {
		t.Fatal(err)
	}
	m := memory.NewManager(1 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	binder, err := NewSourceBinder(registry, cache, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := binder.Bind(context.Background(), uri.String(), SourceBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	reads := len(driver.ranges)
	source, err := NewMetadataCountSource(context.Background(), RuntimeSourceConfig{Binder: binder, Pattern: uri.String(), ExpectedFingerprint: binding.Fingerprint()})
	if err != nil {
		t.Fatal(err)
	}
	chunk, ok, err := source.Next(context.Background())
	if err != nil || !ok {
		t.Fatalf("next=%v %v", ok, err)
	}
	if n, valid := chunk.Columns[0].Int64(0); !valid || n != 17 {
		t.Fatalf("count=%d valid=%v", n, valid)
	}
	chunk.Release()
	if len(driver.ranges) != reads {
		t.Fatalf("metadata cache count issued data reads: %v", driver.ranges[reads:])
	}
	if _, ok, err := source.Next(context.Background()); err != nil || ok {
		t.Fatalf("terminal=%v %v", ok, err)
	}
	if m.Used() != 0 {
		t.Fatalf("managed leak=%d", m.Used())
	}
	if _, err := NewMetadataCountSource(context.Background(), RuntimeSourceConfig{Binder: binder, Pattern: uri.String(), ExpectedFingerprint: "changed"}); err == nil {
		t.Fatal("snapshot mismatch accepted")
	}
}

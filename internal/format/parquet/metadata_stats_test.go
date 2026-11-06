package parquet

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func metadataStatsFixture(t *testing.T, nullOnly bool) (*runtimeBindDriver, *SourceBinder, string, string, *memory.Manager, func()) {
	t.Helper()
	meta, err := DecodeFileMetaData(minimalMetadata())
	if err != nil {
		t.Fatal(err)
	}
	meta.NumRows = 10
	zero := int64(0)
	stats := &Statistics{MinValue: i64b(3), MaxValue: i64b(9), NullCount: &zero}
	if nullOnly {
		ten := int64(10)
		stats = &Statistics{NullCount: &ten}
	}
	meta.RowGroups = []RowGroup{{NumRows: 10, Columns: []ColumnChunk{{MetaData: &ColumnMetaData{Type: TypeInt64, Encodings: []Encoding{EncodingPlain}, NumValues: 10, PathInSchema: []string{"id"}, Statistics: stats}}}}}
	encoded, err := EncodeFileMetaData(meta)
	if err != nil {
		t.Fatal(err)
	}
	uri, _ := vfs.ParseURI("mem://x/stats.parquet")
	driver := &runtimeBindDriver{recordingDriver: &recordingDriver{data: parquetFile(encoded)}, uri: uri}
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := NewMetadataCache(4)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	binder, err := NewSourceBinder(registry, cache, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	binding, err := binder.Bind(context.Background(), uri.String(), SourceBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return driver, binder, uri.String(), binding.Fingerprint(), manager, func() {
		if err := scope.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMetadataStatsSourceAnswersMinMaxFromFooter(t *testing.T) {
	driver, binder, pattern, fingerprint, manager, cleanup := metadataStatsFixture(t, false)
	defer cleanup()
	reads := len(driver.ranges)
	bigint := types.LogicalType{ID: types.BigInt}
	source, accepted, err := NewMetadataStatsSource(context.Background(), RuntimeSourceConfig{Binder: binder, Pattern: pattern, ExpectedFingerprint: fingerprint}, []StatsRequest{
		{ColumnIndex: 0, Kind: StatsMin, OutputType: bigint},
		{ColumnIndex: 0, Kind: StatsMax, OutputType: bigint},
	})
	if err != nil || !accepted {
		t.Fatalf("accepted=%v err=%v", accepted, err)
	}
	chunk, ok, err := source.Next(context.Background())
	if err != nil || !ok {
		t.Fatalf("next=%v err=%v", ok, err)
	}
	if value, valid := chunk.Columns[0].Int64(0); !valid || value != 3 {
		t.Fatalf("min=%d valid=%v", value, valid)
	}
	if value, valid := chunk.Columns[1].Int64(0); !valid || value != 9 {
		t.Fatalf("max=%d valid=%v", value, valid)
	}
	chunk.Release()
	if len(driver.ranges) != reads {
		t.Fatalf("stats source issued data reads: %v", driver.ranges[reads:])
	}
	if _, ok, err := source.Next(context.Background()); err != nil || ok {
		t.Fatalf("terminal=%v err=%v", ok, err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
}

func TestMetadataStatsSourceFallsBackWithoutStats(t *testing.T) {
	meta, err := DecodeFileMetaData(minimalMetadata())
	if err != nil {
		t.Fatal(err)
	}
	meta.NumRows = 10
	meta.RowGroups = []RowGroup{{NumRows: 10, Columns: []ColumnChunk{{MetaData: &ColumnMetaData{Type: TypeInt64, Encodings: []Encoding{EncodingPlain}, NumValues: 10, PathInSchema: []string{"id"}}}}}}
	encoded, err := EncodeFileMetaData(meta)
	if err != nil {
		t.Fatal(err)
	}
	uri, _ := vfs.ParseURI("mem://x/nostats.parquet")
	driver := &runtimeBindDriver{recordingDriver: &recordingDriver{data: parquetFile(encoded)}, uri: uri}
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := NewMetadataCache(4)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	defer func() {
		if err := scope.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	binder, err := NewSourceBinder(registry, cache, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	binding, err := binder.Bind(context.Background(), uri.String(), SourceBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, accepted, err := NewMetadataStatsSource(context.Background(), RuntimeSourceConfig{Binder: binder, Pattern: uri.String(), ExpectedFingerprint: binding.Fingerprint()}, []StatsRequest{
		{ColumnIndex: 0, Kind: StatsMin, OutputType: types.LogicalType{ID: types.BigInt}},
	})
	if err != nil || accepted {
		t.Fatalf("accepted=%v err=%v", accepted, err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
}

func TestMetadataStatsSourceAllNull(t *testing.T) {
	_, binder, pattern, fingerprint, manager, cleanup := metadataStatsFixture(t, true)
	defer cleanup()
	source, accepted, err := NewMetadataStatsSource(context.Background(), RuntimeSourceConfig{Binder: binder, Pattern: pattern, ExpectedFingerprint: fingerprint}, []StatsRequest{
		{ColumnIndex: 0, Kind: StatsMin, OutputType: types.LogicalType{ID: types.BigInt}},
	})
	if err != nil || !accepted {
		t.Fatalf("accepted=%v err=%v", accepted, err)
	}
	chunk, ok, err := source.Next(context.Background())
	if err != nil || !ok {
		t.Fatalf("next=%v err=%v", ok, err)
	}
	if chunk.Columns[0].IsValid(0) {
		t.Fatal("all-NULL MIN must be NULL")
	}
	chunk.Release()
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
}

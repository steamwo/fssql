package export

import (
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// TestFileWriterFactoryFormatCompressionMatrix 固定 P4-D1 的格式×压缩组合，避免后续实现便利改变公开能力矩阵。
func TestFileWriterFactoryFormatCompressionMatrix(t *testing.T) {
	cases := []struct {
		// name 是子测试稳定名称。
		name string
		// format/compression 是待验证的两个正交维度。
		format      FileFormat
		compression string
		// allowed 表示该组合是否属于 Phase 4 已授权能力。
		allowed         bool
		wantCompression string
	}{
		{name: "csv_default", format: FormatCSV, allowed: true, wantCompression: "identity"},
		{name: "json_default", format: FormatJSON, allowed: true, wantCompression: "identity"},
		{name: "ndjson_default", format: FormatNDJSON, allowed: true, wantCompression: "identity"},
		{name: "parquet_default", format: FormatParquet, allowed: true, wantCompression: "snappy"},
		{name: "parquet_blank", format: FormatParquet, compression: "  ", allowed: true, wantCompression: "snappy"},
		{name: "parquet_none", format: FormatParquet, compression: "none", allowed: true, wantCompression: "identity"},
		{name: "parquet_snappy", format: FormatParquet, compression: "snappy", allowed: true},
		{name: "csv_identity", format: FormatCSV, compression: "identity", allowed: true},
		{name: "csv_gzip", format: FormatCSV, compression: "gzip", allowed: true},
		{name: "csv_zstd", format: FormatCSV, compression: "zstd", allowed: true},
		{name: "json_identity", format: FormatJSON, compression: "identity", allowed: true},
		{name: "json_gzip", format: FormatJSON, compression: "gzip", allowed: true},
		{name: "json_zstd", format: FormatJSON, compression: "zstd", allowed: true},
		{name: "ndjson_identity", format: FormatNDJSON, compression: "identity", allowed: true},
		{name: "ndjson_gzip", format: FormatNDJSON, compression: "gzip", allowed: true},
		{name: "ndjson_zstd", format: FormatNDJSON, compression: "zstd", allowed: true},
		{name: "parquet_identity", format: FormatParquet, compression: "identity", allowed: true},
		{name: "parquet_gzip", format: FormatParquet, compression: "gzip", allowed: false},
		{name: "parquet_zstd", format: FormatParquet, compression: "zstd", allowed: false},
		{name: "csv_unknown", format: FormatCSV, compression: "brotli", allowed: false},
		{name: "json_unknown", format: FormatJSON, compression: "brotli", allowed: false},
		{name: "ndjson_unknown", format: FormatNDJSON, compression: "brotli", allowed: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			manager := memory.NewManager(4 << 20)
			scope := manager.NewScope()
			driver := &factoryTestDriver{caps: vfs.Capabilities(vfs.StreamingWrite)}
			registry, err := vfs.NewRegistry(driver)
			if err != nil {
				t.Fatal(err)
			}
			pool, err := NewWriterPool(1, 4096, scope)
			if err != nil {
				t.Fatal(err)
			}
			factory, err := NewFileWriterFactory(FileWriterConfig{
				Schema:      []Column{{Name: "id", Type: types.LogicalType{ID: types.Integer}}},
				Format:      tc.format,
				Compression: tc.compression,
			}, registry, scope, memory.NewBufferManager(nil), pool)
			if tc.allowed && err != nil {
				t.Fatalf("allowed combination rejected: %v", err)
			}
			if !tc.allowed && err == nil {
				t.Fatal("disallowed combination accepted")
			}
			if tc.wantCompression != "" && factory != nil && factory.config.Compression != tc.wantCompression {
				t.Fatalf("compression=%q, want %q", factory.config.Compression, tc.wantCompression)
			}
			if driver.opens != 0 || pool.Active() != 0 || scope.Used() != 0 {
				t.Fatalf("constructor side effect opens=%d active=%d memory=%d", driver.opens, pool.Active(), scope.Used())
			}
		})
	}
}

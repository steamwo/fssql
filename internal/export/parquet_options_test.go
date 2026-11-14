package export

import (
	"errors"
	"math"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func TestFileWriterFactoryRejectsParquetLayoutBeforeOpen(t *testing.T) {
	for _, tc := range []struct {
		name   string
		format FileFormat
		opts   ParquetWriteOptions
	}{
		{"negative_group", FormatParquet, ParquetWriteOptions{RowGroupRows: -1}},
		{"negative_page", FormatParquet, ParquetWriteOptions{PageRows: -1}},
		{"negative_buffer", FormatParquet, ParquetWriteOptions{PageBufferSize: -1}},
		{"page_exceeds_group", FormatParquet, ParquetWriteOptions{RowGroupRows: 3, PageRows: 4}},
		{"page_exceeds_default_group", FormatParquet, ParquetWriteOptions{PageRows: 2049}},
		{"group_overflow", FormatParquet, ParquetWriteOptions{RowGroupRows: math.MaxInt}},
		{"csv_layout", FormatCSV, ParquetWriteOptions{RowGroupRows: 8192}},
		{"json_layout", FormatJSON, ParquetWriteOptions{PageRows: 2048}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scope := memory.NewManager(1 << 20).NewScope()
			driver := &factoryTestDriver{caps: vfs.Capabilities(vfs.StreamingWrite)}
			registry, _ := vfs.NewRegistry(driver)
			pool, _ := NewWriterPool(1, 64, scope)
			_, err := NewFileWriterFactory(FileWriterConfig{
				Schema: []Column{{Name: "id", Type: types.LogicalType{ID: types.BigInt}}},
				Format: tc.format, Parquet: tc.opts,
			}, registry, scope, memory.NewBufferManager(nil), pool)
			if err == nil || driver.opens != 0 || pool.Active() != 0 || scope.Used() != 0 {
				t.Fatalf("err=%v opens=%d active=%d used=%d", err, driver.opens, pool.Active(), scope.Used())
			}
		})
	}
}

func TestParquetLargeRowGroupBudgetBeforeAllocation(t *testing.T) {
	scope := memory.NewManager(1 << 20).NewScope()
	allocations := 0
	buffers := memory.NewBufferManager(func(n int) ([]byte, error) {
		allocations++
		return make([]byte, n), nil
	})
	_, err := NewParquetWriter([]Column{{Name: "id", Type: types.LogicalType{ID: types.BigInt}}},
		ParquetWriteOptions{RowGroupRows: 1 << 24}, scope, buffers, &captureSink{})
	if !errors.Is(err, memory.ErrLimitExceeded) || allocations != 0 || scope.Used() != 0 {
		t.Fatalf("err=%v allocations=%d used=%d", err, allocations, scope.Used())
	}
}

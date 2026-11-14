package export

import (
	"errors"
	"fmt"
	"math"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
)

// NormalizeParquetLayout applies defaults and validates row-group/page sizes.
// Compression and encryption are validated separately by the writer/factory.
func NormalizeParquetLayout(opts ParquetWriteOptions) (ParquetWriteOptions, error) {
	if opts.RowGroupRows < 0 || opts.PageRows < 0 || opts.PageBufferSize < 0 {
		return opts, errors.New("export: Parquet layout sizes must be non-negative")
	}
	if opts.RowGroupRows == 0 {
		opts.RowGroupRows = DefaultParquetRowGroupRows
	}
	// Varchar slots are the widest supported row-group payload (24 bytes).
	// Also leaves room for the validity bitmap's count+63 calculation.
	if opts.RowGroupRows > math.MaxInt/24 {
		return opts, errors.New("export: Parquet row-group rows exceed vector size limit")
	}
	if opts.PageRows == 0 {
		opts.PageRows = min(DefaultParquetPageRows, opts.RowGroupRows)
	}
	if opts.PageRows > opts.RowGroupRows {
		return opts, fmt.Errorf("export: parquet page rows %d exceed row-group rows %d", opts.PageRows, opts.RowGroupRows)
	}
	if opts.PageRows > math.MaxInt32 {
		return opts, errors.New("export: Parquet page rows exceed int32 limit")
	}
	if opts.PageBufferSize == 0 {
		opts.PageBufferSize = defaultParquetPageBuffer
	}
	return opts, nil
}

func checkParquetRowGroupBudget(schema []Column, rows int, scope *memory.Scope) error {
	// Check the complete fixed payload before NewFlat creates Go-heap validity
	// bitmaps. The actual buffers keep their own reservations after this check.
	var total int64
	for _, column := range schema {
		width, _ := column.Type.FixedSize()
		if column.Type.ID == types.Varchar {
			width = 24
		}
		size := int64(rows) * int64(width)
		if size > math.MaxInt64-total {
			return errors.New("export: Parquet row-group size overflows int64")
		}
		total += size
	}
	reservation, err := scope.Reserve(memory.TagVector, total)
	if err != nil {
		return err
	}
	reservation.Release()
	return nil
}

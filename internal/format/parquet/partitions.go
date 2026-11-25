package parquet

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/format/scan"
	"github.com/steamwo/fssql/internal/storage/fileset"
	"github.com/steamwo/fssql/internal/tablefunc"
)

func parsePartitionSource(source string) (string, string, []types.StructField, error) {
	pattern, options, encoded, err := tablefunc.Decode(source)
	if err != nil || !encoded {
		return pattern, "", nil, err
	}
	spec, ok := options["partition_columns"]
	if !ok || len(options) != 1 {
		return "", "", nil, fmt.Errorf("parquet: unsupported reader options")
	}
	fields, err := tablefunc.ParseColumns(spec)
	if err != nil {
		return "", "", nil, fmt.Errorf("parquet: partition_columns: %w", err)
	}
	for _, field := range fields {
		if field.Type.ID != types.Varchar && (field.Type.ID != types.BigInt || field.Type.IsUnsignedInteger()) {
			return "", "", nil, fmt.Errorf("parquet: partition column %q requires BIGINT or VARCHAR", field.Name)
		}
	}
	return pattern, spec, fields, nil
}

func validatePartitions(ctx context.Context, physical, partitions []types.StructField, files []fileset.FileSnapshot) error {
	for _, field := range partitions {
		for _, column := range physical {
			if strings.EqualFold(field.Name, column.Name) {
				return fmt.Errorf("parquet: partition column %q conflicts with a file column", field.Name)
			}
		}
		if field.Type.ID != types.BigInt {
			continue
		}
		for _, file := range files {
			if err := ctx.Err(); err != nil {
				return err
			}
			if text, ok := file.Partitions[field.Name]; ok {
				if _, err := strconv.ParseInt(text, 10, 64); err != nil {
					return fmt.Errorf("parquet: file %s has invalid BIGINT partition %q=%q", file.URI, field.Name, text)
				}
			}
		}
	}
	return ctx.Err()
}

func partitionValue(file fileset.FileSnapshot, field types.StructField) scan.Literal {
	text, ok := file.Partitions[field.Name]
	value := scan.Literal{Type: field.Type, Null: !ok}
	if field.Type.ID == types.BigInt {
		// Bind validated exact BIGINT conversion for the frozen file set.
		value.Int64, _ = strconv.ParseInt(text, 10, 64)
	} else {
		value.String = text
	}
	return value
}

func partitionValues(file fileset.FileSnapshot, fields []types.StructField) []scan.Literal {
	values := make([]scan.Literal, len(fields))
	for i, field := range fields {
		values[i] = partitionValue(file, field)
	}
	return values
}

func splitPartitionPushdown(input scan.Pushdown, physicalWidth int) (scan.Pushdown, []scan.IndexedFilter) {
	physical := scan.Pushdown{Limit: input.Limit}
	for _, col := range input.Projection {
		if col < physicalWidth {
			physical.Projection = append(physical.Projection, col)
		}
	}
	var partitions []scan.IndexedFilter
	for _, filter := range input.Filters {
		if filter.ColumnIndex < physicalWidth {
			physical.Filters = append(physical.Filters, filter)
		} else {
			filter.ColumnIndex -= physicalWidth
			partitions = append(partitions, filter)
		}
	}
	return physical, partitions
}

func prunePartitionFiles(files []fileset.FileSnapshot, fields []types.StructField, filters []scan.IndexedFilter) []fileset.FileSnapshot {
	kept := files[:0]
	for _, file := range files {
		keep := true
		for _, filter := range filters {
			if filter.ColumnIndex < len(fields) && partitionCannotMatch(partitionValue(file, fields[filter.ColumnIndex]), filter) {
				keep = false
				break
			}
		}
		if keep {
			kept = append(kept, file)
		}
	}
	clear(files[len(kept):])
	return kept
}

func partitionCannotMatch(value scan.Literal, filter scan.IndexedFilter) bool {
	switch filter.Op {
	case scan.IsNull:
		return !value.Null
	case scan.IsNotNull:
		return value.Null
	case scan.Equal:
		if value.Null || filter.Value.Null {
			return true
		}
		if value.Type.ID == types.BigInt && filter.Value.Type.ID == types.BigInt && !filter.Value.Type.IsUnsignedInteger() {
			return value.Int64 != filter.Value.Int64
		}
		if value.Type.ID == types.Varchar && filter.Value.Type.ID == types.Varchar {
			return value.String != filter.Value.String
		}
	}
	return false
}

type partitionScanner struct {
	runtimeRowGroupScanner
	values  []scan.Literal
	scope   *memory.Scope
	buffers *memory.BufferManager
}

func (s *partitionScanner) Next(ctx context.Context) (*vector.DataChunk, error) {
	chunk, err := s.runtimeRowGroupScanner.Next(ctx)
	if err != nil || chunk == nil {
		return chunk, err
	}
	for _, value := range s.values {
		column, err := vector.NewConstant(s.scope, s.buffers, value.Type, chunk.Cardinality)
		if err != nil {
			chunk.Release()
			return nil, err
		}
		chunk.Columns = append(chunk.Columns, column)
		switch {
		case value.Null:
			column.SetNull(0)
		case value.Type.ID == types.BigInt:
			column.SetInt64(0, value.Int64)
		default:
			if err := column.SetString(0, value.String); err != nil {
				chunk.Release()
				return nil, err
			}
		}
	}
	return chunk, nil
}

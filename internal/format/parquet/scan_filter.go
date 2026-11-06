package parquet

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/format/scan"
	"github.com/steamwo/fssql/internal/semantics"
)

type scanBatchFilter struct {
	column     int
	selectRows func(*vector.Vector, []int) []int
	source     scan.Filter
}

func (s *RowGroupScanner) initFilters(filters []scan.Filter, scope *memory.Scope, bm *memory.BufferManager) {
	s.scope, s.buffers = scope, bm
	for _, filter := range filters {
		for column, reader := range s.readers {
			scalar, ok := reader.(*scalarColumnReader)
			if !ok || strings.Join(scalar.cursor.leaf.Path, ".") != filter.Column {
				continue
			}
			if selectRows := compileScanFilter(scalar.cursor.leaf.Logical, filter); selectRows != nil {
				s.filters = append(s.filters, scanBatchFilter{column, selectRows, filter})
			}
		}
	}
}

// Unsupported types remain residual. Type and comparison dispatch happen once
// for the scanner, and borrowed buffers are acquired once per filter batch.
func compileScanFilter(t types.LogicalType, filter scan.Filter) func(*vector.Vector, []int) []int {
	if filter.Op == scan.IsNull || filter.Op == scan.IsNotNull {
		wantValid := filter.Op == scan.IsNotNull
		return func(v *vector.Vector, rows []int) []int {
			out := rows[:0]
			for _, row := range rows {
				if v.IsValid(row) == wantValid {
					out = append(out, row)
				}
			}
			return out
		}
	}
	var predicate func(int) bool
	switch filter.Op {
	case scan.Equal:
		predicate = func(c int) bool { return c == 0 }
	case scan.NotEqual:
		predicate = func(c int) bool { return c != 0 }
	case scan.LessThan:
		predicate = func(c int) bool { return c < 0 }
	case scan.LessEqual:
		predicate = func(c int) bool { return c <= 0 }
	case scan.GreaterThan:
		predicate = func(c int) bool { return c > 0 }
	case scan.GreaterEqual:
		predicate = func(c int) bool { return c >= 0 }
	default:
		return nil
	}
	if filter.Value.Null {
		return nil
	}
	literal := filter.Value
	timestampLiteral := t.ID == types.TimestampType && literal.Type.ID == types.TimestampType
	if !t.Same(literal.Type) && !(t.ID == types.Integer && literal.Type.ID == types.BigInt && !literal.Type.IsUnsignedInteger()) && !(t.ID == types.Float && literal.Type.ID == types.Double) && !timestampLiteral {
		return nil
	}
	if t.IsUnsignedInteger() || t.IsBlob() {
		return nil
	}
	if t.ID == types.Varchar {
		value := []byte(literal.String)
		return func(v *vector.Vector, rows []int) []int {
			view, _ := v.BorrowBytes()
			out := rows[:0]
			for _, row := range rows {
				data, valid := view.Value(row)
				if valid && predicate(bytes.Compare(data, value)) {
					out = append(out, row)
				}
			}
			return out
		}
	}
	var compare func([]byte) int
	switch t.ID {
	case types.Boolean:
		value := byte(0)
		if literal.Bool {
			value = 1
		}
		compare = func(data []byte) int { return int(data[0]) - int(value) }
	case types.Integer:
		compare = func(data []byte) int {
			return semantics.CompareInt64(int64(int32(binary.LittleEndian.Uint32(data))), literal.Int64)
		}
	case types.DateType:
		value := literal.Int64
		compare = func(data []byte) int {
			return semantics.CompareInt64(int64(int32(binary.LittleEndian.Uint32(data))), value)
		}
	case types.BigInt:
		compare = func(data []byte) int {
			return semantics.CompareInt64(int64(binary.LittleEndian.Uint64(data)), literal.Int64)
		}
	case types.TimestampType:
		value, err := semantics.ConvertTimestampUnit(literal.Int64, literal.Type.Timestamp.Unit, t.Timestamp.Unit)
		if err != nil {
			return nil
		}
		compare = func(data []byte) int {
			return semantics.CompareInt64(int64(binary.LittleEndian.Uint64(data)), value)
		}
	case types.Float:
		compare = func(data []byte) int {
			return semantics.CompareFloat64(float64(math.Float32frombits(binary.LittleEndian.Uint32(data))), literal.Float64)
		}
	case types.Double:
		compare = func(data []byte) int {
			return semantics.CompareFloat64(math.Float64frombits(binary.LittleEndian.Uint64(data)), literal.Float64)
		}
	default:
		return nil
	}
	return func(v *vector.Vector, rows []int) []int {
		view, _ := v.BorrowFixedWidth()
		out := rows[:0]
		for _, row := range rows {
			data, valid := view.Value(row)
			if valid && predicate(compare(data)) {
				out = append(out, row)
			}
		}
		return out
	}
}

func (s *RowGroupScanner) nextFiltered(ctx context.Context) (*vector.DataChunk, error) {
	for s.position < s.rowGroup.NumRows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		rows := int(min(int64(s.batchSize), s.rowGroup.NumRows-s.position))
		first := s.filters[0]
		cursor := s.readers[first.column].(*scalarColumnReader).cursor
		var skipped, available int64
		for _, filter := range s.filters {
			if filter.column != first.column {
				continue
			}
			var err error
			skipped, available, err = cursor.pruneFilterPage(ctx, filter.source)
			if err != nil {
				return nil, err
			}
			if skipped > 0 {
				break
			}
		}
		if skipped > 0 {
			s.position += skipped
			continue
		}
		if available > 0 {
			rows = int(min(int64(rows), available))
		}
		columns := make([]*vector.Vector, len(s.readers))
		var selected [vector.DefaultVectorSize]int
		for i := 0; i < rows; i++ {
			selected[i] = i
		}
		selection := selected[:rows]
		fail := func(err error) (*vector.DataChunk, error) { releaseRuntimeColumns(columns); return nil, err }
		for _, filter := range s.filters {
			if columns[filter.column] == nil {
				reader := s.readers[filter.column].(*scalarColumnReader)
				if err := reader.cursor.skipScalar(ctx, s.position-reader.RowPosition()); err != nil {
					return fail(err)
				}
				col, err := reader.Read(ctx, rows)
				if err != nil {
					return fail(err)
				}
				columns[filter.column] = col
			}
			selection = filter.selectRows(columns[filter.column], selection)
			if len(selection) == 0 {
				break
			}
		}
		if len(selection) == 0 {
			releaseRuntimeColumns(columns)
			s.position += int64(rows)
			continue // Payload readers stay unopened until a later batch matches.
		}
		for column, reader := range s.readers {
			var col *vector.Vector
			var err error
			if columns[column] != nil {
				if len(selection) == rows {
					continue
				}
				col, err = vector.CopySelection(s.scope, s.buffers, columns[column], selection)
				columns[column].Release()
				columns[column] = nil
			} else if scalar, ok := reader.(*scalarColumnReader); ok {
				if err = scalar.cursor.skipScalar(ctx, s.position-reader.RowPosition()); err == nil {
					col, err = scalar.cursor.readSelected(ctx, rows, selection)
				}
			} else {
				// Nested reconstruction retains its checked reader contract.
				for reader.RowPosition() < s.position && err == nil {
					var discarded *vector.Vector
					discarded, err = reader.Read(ctx, int(min(int64(s.batchSize), s.position-reader.RowPosition())))
					if discarded != nil {
						discarded.Release()
					}
				}
				if err == nil {
					var full *vector.Vector
					full, err = reader.Read(ctx, rows)
					if err == nil {
						if len(selection) == rows {
							col = full
						} else {
							col, err = vector.NewOwnedDictionary(s.scope, full, vector.SelectionFromRows(selection))
						}
						if err == nil {
							full = nil
						}
					}
					if full != nil {
						full.Release()
					}
				}
			}
			if err != nil {
				return fail(err)
			}
			columns[column] = col
		}
		chunk, err := vector.NewDataChunk(columns...)
		if err != nil {
			return fail(err)
		}
		s.position += int64(rows)
		s.rowsProduced += int64(len(selection))
		for _, reader := range s.readers {
			if reader.RowPosition() != s.position {
				chunk.Release()
				return nil, fmt.Errorf("parquet: filtered columns lost row alignment")
			}
		}
		return chunk, nil
	}
	return nil, io.EOF
}

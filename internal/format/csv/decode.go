package csv

import (
	"bytes"
	"fmt"
	"math"
	"strconv"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

// DecodeOptions controls lexical and field-level decoding. Final indicates
// that input ends at EOF; when false, a trailing partial record is left
// unconsumed for the caller to prepend to the next input block.
type DecodeOptions struct {
	Lex         LexConfig
	Final       bool
	NullLiteral []byte
}

// DecodeChunk decodes at most one engine vector-size batch directly into the
// engine's managed Vector/DataChunk ABI. It returns the number of input bytes
// consumed so callers can retain an incomplete trailing record without copying
// the whole input.
func DecodeChunk(input []byte, schema []types.LogicalType, opts DecodeOptions, scope *memory.Scope, bm *memory.BufferManager) (*vector.DataChunk, int, error) {
	if scope == nil || bm == nil {
		return nil, 0, fmt.Errorf("csv: managed memory scope and buffer manager are required")
	}
	if len(schema) == 0 {
		return nil, 0, fmt.Errorf("csv: schema must contain at least one column")
	}
	if opts.Lex == (LexConfig{}) {
		opts.Lex = DefaultLexConfig()
	}

	rows, consumed, err := countChunkRows(input, opts.Lex, opts.Final, vector.DefaultVectorSize)
	if err != nil {
		return nil, 0, err
	}
	if rows == 0 {
		return nil, consumed, nil
	}

	cols := make([]*vector.Vector, 0, len(schema))
	cleanup := func() {
		for _, col := range cols {
			col.Release()
		}
	}
	for _, typ := range schema {
		col, err := vector.NewFlat(scope, bm, typ, rows)
		if err != nil {
			cleanup()
			return nil, 0, err
		}
		cols = append(cols, col)
	}

	chunk, err := vector.NewDataChunk(cols...)
	if err != nil {
		cleanup()
		return nil, 0, err
	}
	cols = nil

	if err := decodeRows(input[:consumed], schema, opts, chunk, scope, bm); err != nil {
		chunk.Release()
		return nil, 0, err
	}
	return chunk, consumed, nil
}

func countChunkRows(input []byte, cfg LexConfig, final bool, maxRows int) (rows, consumed int, err error) {
	m, err := NewMachine(cfg)
	if err != nil {
		return 0, 0, err
	}
	lastRecordStart := 0
	for i := 0; i < len(input); i++ {
		event, stepErr := m.Step(input[i])
		if stepErr != nil {
			return 0, 0, stepErr
		}
		if event.Has(EventRecordEnd) {
			rows++
			consumed = i + 1
			if input[i] == '\r' && i+1 < len(input) && input[i+1] == '\n' {
				consumed++
			}
			lastRecordStart = consumed
			if rows == maxRows {
				return rows, consumed, nil
			}
		}
	}
	if !final {
		return rows, consumed, nil
	}
	if err := m.Finish(); err != nil {
		return 0, 0, err
	}
	if lastRecordStart < len(input) {
		rows++
		consumed = len(input)
	}
	return rows, consumed, nil
}

func decodeRows(input []byte, schema []types.LogicalType, opts DecodeOptions, chunk *vector.DataChunk, scope *memory.Scope, bm *memory.BufferManager) error {
	m, err := NewMachine(opts.Lex)
	if err != nil {
		return err
	}
	initial := 256
	if len(input) < initial {
		initial = len(input)
	}
	if initial < 1 {
		initial = 1
	}
	scratch, err := bm.Allocate(scope, memory.TagCSVDecode, initial)
	if err != nil {
		return err
	}
	defer scratch.Release()
	scratchBytes := scratch.Bytes()
	used := 0
	row := 0
	col := 0

	appendByte := func(b byte) error {
		if used == len(scratchBytes) {
			newCap := len(scratchBytes) * 2
			if newCap < 64 {
				newCap = 64
			}
			if err := scratch.Resize(newCap, nil); err != nil {
				return err
			}
			scratchBytes = scratch.Bytes()
		}
		scratchBytes[used] = b
		used++
		return nil
	}
	finishField := func() error {
		if row >= chunk.Cardinality {
			return fmt.Errorf("csv: decoded more rows than planned")
		}
		if col >= len(schema) {
			return fmt.Errorf("csv: row %d has more than %d columns", row+1, len(schema))
		}
		if err := decodeField(chunk.Columns[col], schema[col], row, scratchBytes[:used], opts.NullLiteral); err != nil {
			return fmt.Errorf("csv: row %d column %d: %w", row+1, col+1, err)
		}
		used = 0
		col++
		return nil
	}
	finishRecord := func() error {
		if col != len(schema) {
			return fmt.Errorf("csv: row %d has %d columns, want %d", row+1, col, len(schema))
		}
		row++
		col = 0
		return nil
	}

	for i := 0; i < len(input); i++ {
		event, stepErr := m.Step(input[i])
		if stepErr != nil {
			return stepErr
		}
		if event.Has(EventData) {
			if err := appendByte(input[i]); err != nil {
				return err
			}
		}
		if event.Has(EventFieldEnd) {
			if err := finishField(); err != nil {
				return err
			}
		}
		if event.Has(EventRecordEnd) {
			if err := finishRecord(); err != nil {
				return err
			}
		}
	}
	if err := m.Finish(); err != nil {
		return err
	}
	if row < chunk.Cardinality {
		if err := finishField(); err != nil {
			return err
		}
		if err := finishRecord(); err != nil {
			return err
		}
	}
	if row != chunk.Cardinality {
		return fmt.Errorf("csv: decoded %d rows, planned %d", row, chunk.Cardinality)
	}
	return nil
}

func decodeField(dst *vector.Vector, typ types.LogicalType, row int, raw, nullLiteral []byte) error {
	if nullLiteral != nil && bytes.Equal(raw, nullLiteral) {
		dst.SetNull(row)
		return nil
	}
	switch typ.ID {
	case types.Boolean:
		value, ok := parseBool(raw)
		if !ok {
			return fmt.Errorf("invalid BOOLEAN %q", raw)
		}
		dst.SetBool(row, value)
	case types.Integer:
		value, err := strconv.ParseInt(string(raw), 10, 32)
		if err != nil {
			return fmt.Errorf("invalid INTEGER %q", raw)
		}
		dst.SetInt32(row, int32(value))
	case types.BigInt:
		value, err := strconv.ParseInt(string(raw), 10, 64)
		if err != nil {
			return fmt.Errorf("invalid BIGINT %q", raw)
		}
		dst.SetInt64(row, value)
	case types.Float:
		value, err := strconv.ParseFloat(string(raw), 32)
		if err != nil || math.IsInf(value, 0) {
			return fmt.Errorf("invalid FLOAT %q", raw)
		}
		dst.SetFloat32(row, float32(value))
	case types.Double:
		value, err := strconv.ParseFloat(string(raw), 64)
		if err != nil || math.IsInf(value, 0) {
			return fmt.Errorf("invalid DOUBLE %q", raw)
		}
		dst.SetFloat64(row, value)
	case types.Varchar:
		if err := dst.SetStringBytes(row, raw); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported decode type %d", typ.ID)
	}
	return nil
}

func parseBool(raw []byte) (bool, bool) {
	if bytes.EqualFold(raw, []byte("true")) {
		return true, true
	}
	if bytes.EqualFold(raw, []byte("false")) {
		return false, true
	}
	return false, false
}

package csv

import (
	"bytes"
	"fmt"
	"math"
	"strconv"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
)

// InferenceOptions bounds CSV schema inference. MaxRows counts data rows, not
// the optional header row. Inference is intentionally sampled; callers surface
// that fact through the source-introspection confidence contract.
type InferenceOptions struct {
	Lex         LexConfig
	Header      bool
	Final       bool
	NullLiteral []byte
	MaxRows     int
}

// InferredSchema is the single CSV inference result intended to be embedded in
// CSV SourceBinding. Introspection must consume that binding rather than call a
// separate inference path.
type InferredSchema struct {
	Fields     []types.StructField
	SampleRows int
}

type inferredKind uint8

const (
	kindUnknown inferredKind = iota
	kindBoolean
	kindBigInt
	kindDouble
	kindVarchar
)

// InferSchema uses the same quote-aware Machine as execution decoding and keeps
// field scratch space in managed memory. The returned schema metadata is small
// control-plane state and may live on the Go heap.
func InferSchema(input []byte, opts InferenceOptions, scope *memory.Scope, bm *memory.BufferManager) (InferredSchema, error) {
	if scope == nil || bm == nil {
		return InferredSchema{}, fmt.Errorf("csv: managed memory scope and buffer manager are required")
	}
	if opts.Lex == (LexConfig{}) {
		opts.Lex = DefaultLexConfig()
	}
	if opts.MaxRows <= 0 {
		opts.MaxRows = 2048
	}
	m, err := NewMachine(opts.Lex)
	if err != nil {
		return InferredSchema{}, err
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
		return InferredSchema{}, err
	}
	defer scratch.Release()
	scratchBytes := scratch.Bytes()
	used := 0

	var names []string
	var kinds []inferredKind
	row := 0
	col := 0
	dataRows := 0
	stop := false

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
		raw := scratchBytes[:used]
		if row == 0 {
			if opts.Header {
				names = append(names, string(raw))
				kinds = append(kinds, kindUnknown)
			} else {
				kinds = append(kinds, observeKind(raw, opts.NullLiteral))
			}
		} else {
			if col >= len(kinds) {
				return fmt.Errorf("csv: row %d has more than %d columns", row+1, len(kinds))
			}
			kinds[col] = mergeKind(kinds[col], observeKind(raw, opts.NullLiteral))
		}
		used = 0
		col++
		return nil
	}
	finishRecord := func() error {
		if row == 0 {
			if len(kinds) == 0 {
				return fmt.Errorf("csv: first row contains no columns")
			}
		} else if col != len(kinds) {
			return fmt.Errorf("csv: row %d has %d columns, want %d", row+1, col, len(kinds))
		}
		if row > 0 || !opts.Header {
			dataRows++
		}
		row++
		col = 0
		if dataRows >= opts.MaxRows {
			stop = true
		}
		return nil
	}

	for i := 0; i < len(input) && !stop; i++ {
		event, stepErr := m.Step(input[i])
		if stepErr != nil {
			return InferredSchema{}, stepErr
		}
		if event.Has(EventData) {
			if err := appendByte(input[i]); err != nil {
				return InferredSchema{}, err
			}
		}
		if event.Has(EventFieldEnd) {
			if err := finishField(); err != nil {
				return InferredSchema{}, err
			}
		}
		if event.Has(EventRecordEnd) {
			if err := finishRecord(); err != nil {
				return InferredSchema{}, err
			}
		}
	}

	if !stop && opts.Final {
		if err := m.Finish(); err != nil {
			return InferredSchema{}, err
		}
		if col > 0 || used > 0 || (row == 0 && len(input) > 0) {
			if err := finishField(); err != nil {
				return InferredSchema{}, err
			}
			if err := finishRecord(); err != nil {
				return InferredSchema{}, err
			}
		}
	}
	if len(kinds) == 0 {
		return InferredSchema{}, fmt.Errorf("csv: no complete rows available for schema inference")
	}
	if opts.Header {
		names = normalizeHeaderNames(names)
	} else {
		names = make([]string, len(kinds))
		for i := range names {
			names[i] = fmt.Sprintf("column%d", i)
		}
	}
	fields := make([]types.StructField, len(kinds))
	for i := range kinds {
		fields[i] = types.StructField{Name: names[i], Type: kindLogicalType(kinds[i])}
	}
	return InferredSchema{Fields: fields, SampleRows: dataRows}, nil
}

func observeKind(raw, nullLiteral []byte) inferredKind {
	if nullLiteral != nil && bytes.Equal(raw, nullLiteral) {
		return kindUnknown
	}
	if _, ok := parseBool(raw); ok {
		return kindBoolean
	}
	if _, err := strconv.ParseInt(string(raw), 10, 64); err == nil {
		return kindBigInt
	}
	if value, err := strconv.ParseFloat(string(raw), 64); err == nil && !math.IsInf(value, 0) {
		return kindDouble
	}
	return kindVarchar
}

func mergeKind(current, observed inferredKind) inferredKind {
	if observed == kindUnknown {
		return current
	}
	if current == kindUnknown || current == observed {
		return observed
	}
	if (current == kindBigInt && observed == kindDouble) || (current == kindDouble && observed == kindBigInt) {
		return kindDouble
	}
	return kindVarchar
}

func kindLogicalType(kind inferredKind) types.LogicalType {
	switch kind {
	case kindBoolean:
		return types.LogicalType{ID: types.Boolean}
	case kindBigInt:
		return types.LogicalType{ID: types.BigInt}
	case kindDouble:
		return types.LogicalType{ID: types.Double}
	default:
		return types.LogicalType{ID: types.Varchar}
	}
}

func normalizeHeaderNames(names []string) []string {
	out := make([]string, len(names))
	seen := make(map[string]int, len(names))
	for i, name := range names {
		if name == "" {
			name = fmt.Sprintf("column%d", i)
		}
		base := name
		if count := seen[base]; count > 0 {
			for suffix := count; ; suffix++ {
				candidate := fmt.Sprintf("%s_%d", base, suffix)
				if _, exists := seen[candidate]; !exists {
					name = candidate
					seen[base] = suffix + 1
					break
				}
			}
		}
		seen[name]++
		out[i] = name
	}
	return out
}

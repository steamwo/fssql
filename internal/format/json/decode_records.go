package json

import (
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

type cell struct {
	raw     []byte
	present bool
}

type cellBudget struct {
	res   *memory.Reservation
	count int
}

func newCellBudget(scope *memory.Scope) (*cellBudget, error) {
	r, err := scope.Reserve(memory.TagJSONDecode, 0)
	if err != nil {
		return nil, err
	}
	return &cellBudget{res: r}, nil
}

func (b *cellBudget) grow(n int) error {
	b.count += n
	return b.res.Resize(int64(b.count * 32))
}

func (b *cellBudget) release() { b.res.Release() }

// DecodeRecords decodes object records directly into the engine Vector/DataChunk ABI.
// Raw input bytes remain caller-owned; bounded cell metadata is accounted as a managed
// reservation and vector/string payloads use BufferManager.
func DecodeRecords(records [][]byte, fields []types.StructField, projected *ProjectedPathSet, scope *memory.Scope, bm *memory.BufferManager) (*vector.DataChunk, error) {
	if scope == nil || bm == nil {
		return nil, fmt.Errorf("json: managed memory scope and buffer manager are required")
	}
	if len(records) > vector.DefaultVectorSize {
		return nil, fmt.Errorf("json: record count %d exceeds vector size %d", len(records), vector.DefaultVectorSize)
	}
	selected := make([]types.StructField, 0, len(fields))
	for _, f := range fields {
		if projected == nil || projected.Includes(f.Name) {
			selected = append(selected, f)
		}
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("json: projection selects no fields")
	}
	budget, err := newCellBudget(scope)
	if err != nil {
		return nil, err
	}
	defer budget.release()
	if err := budget.grow(len(records) * len(selected)); err != nil {
		return nil, err
	}
	matrix := make([][]cell, len(selected))
	for i := range matrix {
		matrix[i] = make([]cell, len(records))
	}
	index := make(map[string]int, len(selected))
	for i, f := range selected {
		index[f.Name] = i
	}
	for r, raw := range records {
		cells, err := extractObject(raw, index, len(selected))
		if err != nil {
			return nil, fmt.Errorf("json: record %d: %w", r+1, err)
		}
		for c := range selected {
			matrix[c][r] = cells[c]
		}
	}
	cols := make([]*vector.Vector, 0, len(selected))
	cleanup := func() {
		for _, v := range cols {
			v.Release()
		}
	}
	for i, f := range selected {
		v, err := buildVector(f.Type, matrix[i], budget, scope, bm)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("json: field %s: %w", f.Name, err)
		}
		cols = append(cols, v)
	}
	chunk, err := vector.NewDataChunk(cols...)
	if err != nil {
		cleanup()
		return nil, err
	}
	return chunk, nil
}

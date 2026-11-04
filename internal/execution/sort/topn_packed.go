package sort

// 本文件实现固定宽度 TopN 的紧凑行区版本，参照 DuckDB TOP_N 的行区 + 索引堆模型。
// 候选行连续存放，堆只保存槽位，避免为每个候选分配独立 memory.Buffer。

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	exechash "github.com/steamwo/fssql/internal/execution/hash"
)

// PackedTopNConfig 描述固定宽度 TopN 的输入、排序键与预算。
type PackedTopNConfig struct {
	InputTypes   []types.LogicalType
	Keys         []Key
	N            int
	MemoryBudget int64
	Scope        *memory.Scope
	Buffers      *memory.BufferManager
}

// PackedTopN 用连续行区保存最多 N 个固定宽度候选，并按索引堆维护最差行。
type PackedTopN struct {
	cfg       PackedTopNConfig
	layout    exechash.RowLayout
	prepared  []preparedSortKey
	rows      *memory.Buffer
	heap      *managedIndices
	scratch   *memory.Buffer
	rowWidth  int
	count     int
	heapReady bool
	finalized bool
}

// NewPackedTopN 在构造阶段一次性预留行区、索引堆与编码 scratch，Consume 不再扩容。
func NewPackedTopN(cfg PackedTopNConfig) (*PackedTopN, error) {
	if cfg.Scope == nil || cfg.Buffers == nil {
		return nil, errors.New("packed TopN requires scope and buffers")
	}
	if cfg.N < 0 {
		return nil, errors.New("packed TopN N must be non-negative")
	}
	if cfg.MemoryBudget <= 0 {
		return nil, errors.New("packed TopN memory budget must be positive")
	}
	if len(cfg.Keys) == 0 {
		return nil, errors.New("packed TopN requires sort keys")
	}
	for _, key := range cfg.Keys {
		if key.Column < 0 || key.Column >= len(cfg.InputTypes) {
			return nil, fmt.Errorf("packed TopN key %d out of range", key.Column)
		}
	}
	layout, err := exechash.NewRelocatableRowLayout(cfg.InputTypes)
	if err != nil {
		return nil, err
	}
	if layout.VariableColumns != 0 || layout.FixedWidth <= 0 {
		return nil, errors.New("packed TopN requires fixed-width input")
	}
	if cfg.N > math.MaxInt/4 || (cfg.N > 0 && int64(cfg.N) > math.MaxInt64/int64(layout.FixedWidth)) {
		return nil, errors.New("packed TopN size overflow")
	}
	rowBytes := int64(cfg.N) * int64(layout.FixedWidth)
	heapBytes := int64(cfg.N) * 4
	scratchBytes := int64(layout.FixedWidth)
	maxInt := int64(int(^uint(0) >> 1))
	if rowBytes > maxInt || heapBytes > maxInt {
		return nil, errors.New("packed TopN size exceeds int range")
	}
	if rowBytes+heapBytes+scratchBytes > cfg.MemoryBudget {
		return nil, memory.ErrLimitExceeded
	}
	if cfg.N == 0 {
		scratch, err := cfg.Buffers.Allocate(cfg.Scope, memory.TagSort, int(scratchBytes))
		if err != nil {
			return nil, err
		}
		return &PackedTopN{cfg: cfg, layout: layout, prepared: preparePackedKeys(layout, cfg.Keys), scratch: scratch, rowWidth: layout.FixedWidth}, nil
	}
	rows, err := cfg.Buffers.Allocate(cfg.Scope, memory.TagSort, int(rowBytes))
	if err != nil {
		return nil, err
	}
	heap, err := newManagedIndices(cfg.Scope, cfg.Buffers, cfg.N)
	if err != nil {
		rows.Release()
		return nil, err
	}
	scratch, err := cfg.Buffers.Allocate(cfg.Scope, memory.TagSort, int(scratchBytes))
	if err != nil {
		heap.release()
		rows.Release()
		return nil, err
	}
	return &PackedTopN{
		cfg: cfg, layout: layout, prepared: preparePackedKeys(layout, cfg.Keys),
		rows: rows, heap: heap, scratch: scratch, rowWidth: layout.FixedWidth,
	}, nil
}

// preparePackedKeys 冻结列偏移与宽度，避免堆比较时重复解析 RowLayout。
func preparePackedKeys(layout exechash.RowLayout, keys []Key) []preparedSortKey {
	prepared := make([]preparedSortKey, len(keys))
	for i, key := range keys {
		prepared[i] = preparedSortKey{key: key, column: layout.Columns[key.Column], fixedWidth: layout.FixedWidth}
	}
	return prepared
}

// row 返回指定槽位的编码行。
func (p *PackedTopN) row(slot int) []byte {
	start := slot * p.rowWidth
	return p.rows.Bytes()[start : start+p.rowWidth]
}

// worse 判断槽位 a 的行是否严格差于槽位 b，用于维护“堆顶最差”的最大堆。
func (p *PackedTopN) worse(a, b int) (bool, error) {
	cmp, err := comparePreparedRows(p.prepared, p.row(a), p.row(b))
	return cmp > 0, err
}

// down 从根向下恢复最大堆，范围由当前候选数决定。
func (p *PackedTopN) down(i int) error {
	for {
		left := 2*i + 1
		if left >= p.count {
			return nil
		}
		best := left
		if left+1 < p.count {
			w, err := p.worse(p.heap.get(left+1), p.heap.get(left))
			if err != nil {
				return err
			}
			if w {
				best = left + 1
			}
		}
		w, err := p.worse(p.heap.get(best), p.heap.get(i))
		if err != nil {
			return err
		}
		if !w {
			return nil
		}
		p.heap.swap(i, best)
		i = best
	}
}

// heapify 用自底向上方式把已填满的槽位整理为最大堆，避免逐行 up 的 O(N log N) 填充成本。
func (p *PackedTopN) heapify() error {
	for i := p.count/2 - 1; i >= 0; i-- {
		if err := p.down(i); err != nil {
			return err
		}
	}
	p.heapReady = true
	return nil
}

// Consume 只序列化可能进入候选集的行；已满时先与堆顶行比较再写入行区。
func (p *PackedTopN) Consume(ctx context.Context, chunk *vector.DataChunk) error {
	if p.finalized {
		return errors.New("packed TopN finalized")
	}
	if chunk == nil {
		return errors.New("nil chunk")
	}
	if len(chunk.Columns) != len(p.cfg.InputTypes) {
		return errors.New("packed TopN input column count mismatch")
	}
	if p.cfg.N == 0 {
		return ctx.Err()
	}
	firstKey := p.cfg.Keys[0]
	firstView, fixedFirst := chunk.Columns[firstKey.Column].BorrowFixedWidth()
	var firstData []byte
	var firstValid bool
	rootFirstReady := false
	scratch := p.scratch.Bytes()
	for row := 0; row < chunk.Cardinality; row++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if p.count == p.cfg.N {
			if !p.heapReady {
				if err := p.heapify(); err != nil {
					return err
				}
				rootFirstReady = false
			}
			root := p.heap.get(0)
			var cmp int
			var err error
			if fixedFirst {
				if !rootFirstReady {
					if firstData, firstValid, err = p.layout.ColumnData(p.row(root), firstKey.Column); err != nil {
						return err
					}
					rootFirstReady = true
				}
				data, valid := firstView.Value(row)
				cmp, err = compareFixedInputKey(p.cfg.InputTypes[firstKey.Column], firstKey, data, valid, firstData, firstValid)
				if err == nil && cmp == 0 {
					cmp, err = compareInputRow(p.layout, p.cfg.Keys[1:], chunk, row, p.row(root))
				}
			} else {
				cmp, err = compareInputRow(p.layout, p.cfg.Keys, chunk, row, p.row(root))
			}
			if err != nil {
				return err
			}
			if cmp >= 0 {
				continue
			}
		}
		if _, err := p.layout.WriteRow(chunk, row, scratch); err != nil {
			return err
		}
		if p.count < p.cfg.N {
			slot := p.count
			copy(p.row(slot), scratch)
			p.heap.set(slot, slot)
			p.count++
			continue
		}
		copy(p.row(p.heap.get(0)), scratch)
		if err := p.down(0); err != nil {
			return err
		}
		rootFirstReady = false
	}
	return nil
}

// sortedSlots 把当前最大堆原地变为升序槽位序列，避免额外索引分配。
func (p *PackedTopN) sortedSlots() error {
	if !p.heapReady {
		if err := p.heapify(); err != nil {
			return err
		}
	}
	for end := p.count - 1; end > 0; end-- {
		p.heap.swap(0, end)
		if err := p.siftDown(0, end); err != nil {
			return err
		}
	}
	return nil
}

// siftDown 在 [0,end) 范围内维护最大堆，供 heapsort 使用。
func (p *PackedTopN) siftDown(root, end int) error {
	for {
		left := 2*root + 1
		if left >= end {
			return nil
		}
		best := left
		if left+1 < end {
			w, err := p.worse(p.heap.get(left+1), p.heap.get(left))
			if err != nil {
				return err
			}
			if w {
				best = left + 1
			}
		}
		w, err := p.worse(p.heap.get(best), p.heap.get(root))
		if err != nil {
			return err
		}
		if !w {
			return nil
		}
		p.heap.swap(root, best)
		root = best
	}
}

// Emit 按升序输出候选行，并复用外部排序的 chunk 物化路径。
func (p *PackedTopN) Emit(ctx context.Context, emit func(*vector.DataChunk) error) error {
	if p.finalized {
		return errors.New("packed TopN finalized")
	}
	if emit == nil {
		return errors.New("packed TopN emit callback required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.sortedSlots(); err != nil {
		return err
	}
	shim := &ExternalSort{
		cfg:    Config{InputTypes: p.cfg.InputTypes, Keys: p.cfg.Keys, Scope: p.cfg.Scope, Buffers: p.cfg.Buffers},
		layout: p.layout, emitLimit: p.count,
	}
	err := shim.emitRows(ctx, func(consume func([]byte) error) error {
		for i := 0; i < p.count; i++ {
			if err := consume(p.row(p.heap.get(i))); err != nil {
				return err
			}
		}
		return nil
	}, emit)
	if err != nil {
		return err
	}
	p.finalized = true
	return nil
}

// Release 释放行区、索引堆与 scratch；重复调用安全。
func (p *PackedTopN) Release() {
	if p == nil {
		return
	}
	if p.rows != nil {
		p.rows.Release()
		p.rows = nil
	}
	if p.heap != nil {
		p.heap.release()
		p.heap = nil
	}
	if p.scratch != nil {
		p.scratch.Release()
		p.scratch = nil
	}
}

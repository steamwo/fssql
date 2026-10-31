package parquet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/format/scan"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type RowGroupScanMetrics struct {
	RowGroupsPruned  int64
	RowsProduced     int64
	Columns          int
	Requests         int64
	PlannedBytes     int64
	TransferredBytes int64
	PagesRead        int64
	DataValues       int64
}

type RowGroupScanner struct {
	tree         *SchemaTree
	rowGroup     RowGroup
	plan         ProjectionPlan
	prune        PruneResult
	cursors      []*ColumnCursor
	leaves       []*SchemaNode
	batchSize    int
	position     int64
	rowsProduced int64
	skipped      bool
	closed       bool
}

func NewRowGroupScanner(driver vfs.Driver, uri vfs.URI, identity vfs.ObjectIdentity, tree *SchemaTree, rg RowGroup, requested []string, filters []scan.Filter, scope *memory.Scope, bm *memory.BufferManager, batchSize int) (*RowGroupScanner, error) {
	if driver == nil || tree == nil || scope == nil || bm == nil {
		return nil, errors.New("parquet: row group scanner requires driver, schema, scope, and buffer manager")
	}
	if rg.NumRows < 0 {
		return nil, errors.New("parquet: negative row group row count")
	}
	if batchSize <= 0 || batchSize > vector.DefaultVectorSize {
		return nil, fmt.Errorf("parquet: invalid row group batch size %d", batchSize)
	}
	plan, err := PlanProjection(tree, rg, requested)
	if err != nil {
		return nil, err
	}
	prune := EvaluateRowGroup(tree, rg, filters)
	s := &RowGroupScanner{tree: tree, rowGroup: rg, plan: plan, prune: prune, batchSize: batchSize}
	if prune.Truth == scan.AlwaysFalse {
		s.skipped = true
		return s, nil
	}
	for _, idx := range plan.ColumnIndexes {
		if idx < 0 || idx >= len(rg.Columns) || rg.Columns[idx].MetaData == nil {
			s.Close()
			return nil, fmt.Errorf("parquet: projected column index %d has no metadata", idx)
		}
		meta := rg.Columns[idx].MetaData
		leaf := leafForPath(tree, meta.PathInSchema)
		if leaf == nil {
			s.Close()
			return nil, fmt.Errorf("parquet: schema leaf %q not found", strings.Join(meta.PathInSchema, "."))
		}
		if leaf.MaxRep != 0 {
			s.Close()
			return nil, ErrRepeatedColumnRequiresLevelReader
		}
		if meta.NumValues != rg.NumRows {
			s.Close()
			return nil, fmt.Errorf("parquet: scalar column %q has %d values for %d rows", strings.Join(meta.PathInSchema, "."), meta.NumValues, rg.NumRows)
		}
		cursor, err := NewColumnCursor(driver, uri, identity, meta, leaf, scope, bm)
		if err != nil {
			s.Close()
			return nil, err
		}
		s.cursors = append(s.cursors, cursor)
		s.leaves = append(s.leaves, leaf)
	}
	return s, nil
}

func leafForPath(tree *SchemaTree, path []string) *SchemaNode {
	if tree == nil {
		return nil
	}
	want := strings.Join(path, ".")
	for _, leaf := range tree.Leaves {
		if strings.Join(leaf.Path, ".") == want {
			return leaf
		}
	}
	return nil
}

func (s *RowGroupScanner) Projection() ProjectionPlan { return s.plan }
func (s *RowGroupScanner) PruneResult() PruneResult   { return s.prune }
func (s *RowGroupScanner) Position() int64            { return s.position }

func (s *RowGroupScanner) Next(ctx context.Context) (*vector.DataChunk, error) {
	if s == nil || s.closed {
		return nil, errors.New("parquet: row group scanner is closed")
	}
	if s.skipped || s.position >= s.rowGroup.NumRows {
		return nil, io.EOF
	}
	remaining := s.rowGroup.NumRows - s.position
	rows := s.batchSize
	if int64(rows) > remaining {
		rows = int(remaining)
	}
	cols := make([]*vector.Vector, 0, len(s.cursors))
	for _, cursor := range s.cursors {
		col, err := cursor.ReadScalar(ctx, rows)
		if err != nil {
			for _, v := range cols {
				v.Release()
			}
			return nil, err
		}
		cols = append(cols, col)
	}
	chunk, err := vector.NewDataChunk(cols...)
	if err != nil {
		for _, v := range cols {
			v.Release()
		}
		return nil, err
	}
	s.position += int64(rows)
	s.rowsProduced += int64(rows)
	for _, cursor := range s.cursors {
		if cursor.Consumed() != s.position {
			chunk.Release()
			return nil, errors.New("parquet: projected columns lost row alignment")
		}
	}
	return chunk, nil
}

func (s *RowGroupScanner) Metrics() RowGroupScanMetrics {
	var m RowGroupScanMetrics
	if s == nil {
		return m
	}
	if s.skipped {
		m.RowGroupsPruned = 1
	}
	m.RowsProduced = s.rowsProduced
	m.Columns = len(s.cursors)
	for _, cursor := range s.cursors {
		cm := cursor.Metrics()
		m.Requests += cm.Requests
		m.PlannedBytes += cm.PlannedBytes
		m.TransferredBytes += cm.TransferredBytes
		m.PagesRead += cm.PagesRead
		m.DataValues += cm.DataValues
	}
	return m
}

func (s *RowGroupScanner) Close() {
	if s == nil || s.closed {
		return
	}
	s.closed = true
	for _, cursor := range s.cursors {
		cursor.Close()
	}
	s.cursors = nil
	s.leaves = nil
}

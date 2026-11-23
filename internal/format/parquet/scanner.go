package parquet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/steamwo/fssql/internal/core/memory"
	coretypes "github.com/steamwo/fssql/internal/core/types"
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
	UsefulBytes      int64
	PagesRead        int64
	PagesSkipped     int64
	DataValues       int64
}

type projectedColumnReader interface {
	Read(context.Context, int) (*vector.Vector, error)
	Close()
	Metrics() ColumnCursorMetrics
	RowPosition() int64
}

type scalarColumnReader struct{ cursor *ColumnCursor }

func (r *scalarColumnReader) Read(ctx context.Context, rows int) (*vector.Vector, error) {
	return r.cursor.ReadScalar(ctx, rows)
}
func (r *scalarColumnReader) Close()                       { r.cursor.Close() }
func (r *scalarColumnReader) Metrics() ColumnCursorMetrics { return r.cursor.Metrics() }
func (r *scalarColumnReader) RowPosition() int64           { return r.cursor.Consumed() }

type RowGroupScanner struct {
	tree          *SchemaTree
	rowGroup      RowGroup
	plan          ProjectionPlan
	prune         PruneResult
	readers       []projectedColumnReader
	batchSize     int
	position      int64
	rowsProduced  int64
	skipped       bool
	closed        bool
	finalMetrics  RowGroupScanMetrics
	metricsFrozen bool
	filters       []scanBatchFilter
	scope         *memory.Scope
	buffers       *memory.BufferManager
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
	defer func() { s.initFilters(filters, scope, bm) }()

	explicit := nonEmptyProjectionNames(requested)
	if len(explicit) == 0 && tree.Root != nil {
		for _, child := range tree.Root.Children {
			explicit = append(explicit, child.Name)
		}
	}
	if len(explicit) > 0 {
		for _, name := range explicit {
			node := schemaNodeForPath(tree, name)
			if node == nil {
				s.Close()
				return nil, fmt.Errorf("parquet: projected column %q not found", name)
			}
			reader, err := projectedReaderForNode(driver, uri, identity, tree, rg, node, scope, bm)
			if err != nil {
				s.Close()
				return nil, err
			}
			s.readers = append(s.readers, reader)
		}
		return s, nil
	}

	for _, idx := range plan.ColumnIndexes {
		reader, err := projectedReaderForColumnIndex(driver, uri, identity, tree, rg, idx, scope, bm)
		if err != nil {
			s.Close()
			return nil, err
		}
		s.readers = append(s.readers, reader)
	}
	return s, nil
}

func nonEmptyProjectionNames(requested []string) []string {
	out := make([]string, 0, len(requested))
	for _, raw := range requested {
		if name := strings.TrimSpace(raw); name != "" {
			out = append(out, name)
		}
	}
	return out
}

func schemaNodeForPath(tree *SchemaTree, name string) *SchemaNode {
	if tree == nil || name == "" {
		return nil
	}
	want := strings.TrimSpace(name)
	if tree.Root != nil {
		parts := strings.Split(want, ".")
		node := tree.Root
		found := true
		for _, part := range parts {
			var next *SchemaNode
			for _, child := range node.Children {
				if child.Name == part {
					next = child
					break
				}
			}
			if next == nil {
				found = false
				break
			}
			node = next
		}
		if found {
			return node
		}
	}
	for _, leaf := range tree.Leaves {
		for node := leaf; node != nil; node = node.Parent {
			if strings.Join(node.Path, ".") == want || (node.Parent == nil && node.Name == want) || (len(strings.Split(want, ".")) == 1 && node.Name == want) {
				return node
			}
		}
	}
	return nil
}

func projectedReaderForNode(driver vfs.Driver, uri vfs.URI, identity vfs.ObjectIdentity, tree *SchemaTree, rg RowGroup, node *SchemaNode, scope *memory.Scope, bm *memory.BufferManager) (projectedColumnReader, error) {
	if node == nil {
		return nil, errors.New("parquet: nil projected schema node")
	}
	if node.Logical.ID == coretypes.Struct && node.MaxRep == 0 {
		return NewStructColumnReader(driver, uri, identity, rg, node, scope, bm)
	}
	if node.Logical.ID == coretypes.List && isListAnnotated(node.Element) {
		if _, _, _, err := validateStandardListStruct(node); err == nil {
			return NewListStructColumnReader(driver, uri, identity, rg, node, scope, bm)
		}
		_, leaf, err := validateStandardList(node)
		if err != nil {
			return nil, err
		}
		meta := metadataForPath(rg, leaf.Path)
		if meta == nil {
			return nil, fmt.Errorf("parquet: projected LIST %q has no column metadata", strings.Join(node.Path, "."))
		}
		return NewListColumnReader(driver, uri, identity, meta, node, rg.NumRows, scope, bm)
	}
	if node.Element.Type == nil {
		return nil, ErrUnsupportedStructLayout
	}
	meta := metadataForPath(rg, node.Path)
	if meta == nil {
		return nil, fmt.Errorf("parquet: projected column %q has no metadata", strings.Join(node.Path, "."))
	}
	if node.MaxRep == 0 {
		if meta.NumValues != rg.NumRows {
			return nil, fmt.Errorf("parquet: scalar column %q has %d values for %d rows", strings.Join(meta.PathInSchema, "."), meta.NumValues, rg.NumRows)
		}
		cursor, err := NewColumnCursor(driver, uri, identity, meta, node, scope, bm)
		if err != nil {
			return nil, err
		}
		if meta.crypto == nil {
			for _, chunk := range rg.Columns {
				if chunk.MetaData == meta {
					cursor.configurePageIndex(chunk)
					break
				}
			}
		}
		return &scalarColumnReader{cursor: cursor}, nil
	}
	listNode, err := standardListAncestor(node)
	if err != nil {
		return nil, err
	}
	return NewListColumnReader(driver, uri, identity, meta, listNode, rg.NumRows, scope, bm)
}

func projectedReaderForColumnIndex(driver vfs.Driver, uri vfs.URI, identity vfs.ObjectIdentity, tree *SchemaTree, rg RowGroup, idx int, scope *memory.Scope, bm *memory.BufferManager) (projectedColumnReader, error) {
	if idx < 0 || idx >= len(rg.Columns) || rg.Columns[idx].MetaData == nil {
		return nil, fmt.Errorf("parquet: projected column index %d has no metadata", idx)
	}
	meta := rg.Columns[idx].MetaData
	leaf := leafForPath(tree, meta.PathInSchema)
	if leaf == nil {
		return nil, fmt.Errorf("parquet: schema leaf %q not found", strings.Join(meta.PathInSchema, "."))
	}
	return projectedReaderForNode(driver, uri, identity, tree, rg, leaf, scope, bm)
}

func standardListAncestor(leaf *SchemaNode) (*SchemaNode, error) {
	for n := leaf.Parent; n != nil; n = n.Parent {
		if !isListAnnotated(n.Element) {
			continue
		}
		_, listLeaf, err := validateStandardList(n)
		if err != nil {
			return nil, err
		}
		if listLeaf != leaf {
			return nil, ErrUnsupportedListLayout
		}
		return n, nil
	}
	return nil, ErrRepeatedColumnRequiresLevelReader
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
	if len(s.filters) > 0 {
		return s.nextFiltered(ctx)
	}
	remaining := s.rowGroup.NumRows - s.position
	rows := s.batchSize
	if int64(rows) > remaining {
		rows = int(remaining)
	}
	cols := make([]*vector.Vector, 0, len(s.readers))
	for _, reader := range s.readers {
		col, err := reader.Read(ctx, rows)
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
	for _, reader := range s.readers {
		if reader.RowPosition() != s.position {
			chunk.Release()
			return nil, errors.New("parquet: projected columns lost row alignment")
		}
	}
	return chunk, nil
}

func (s *RowGroupScanner) collectMetrics() RowGroupScanMetrics {
	var m RowGroupScanMetrics
	if s == nil {
		return m
	}
	if s.skipped {
		m.RowGroupsPruned = 1
	}
	m.RowsProduced = s.rowsProduced
	m.Columns = len(s.readers)
	for _, reader := range s.readers {
		cm := reader.Metrics()
		m.Requests += cm.Requests
		m.PlannedBytes += cm.PlannedBytes
		m.TransferredBytes += cm.TransferredBytes
		m.PagesRead += cm.PagesRead
		m.PagesSkipped += cm.PagesSkipped
		m.DataValues += cm.DataValues
		if !s.skipped && s.position >= s.rowGroup.NumRows {
			m.UsefulBytes += cm.UsefulBytes
		}
	}
	return m
}

func (s *RowGroupScanner) Metrics() RowGroupScanMetrics {
	if s == nil {
		return RowGroupScanMetrics{}
	}
	if s.metricsFrozen {
		return s.finalMetrics
	}
	return s.collectMetrics()
}

func (s *RowGroupScanner) Close() {
	if s == nil || s.closed {
		return
	}
	// Freeze observable scan/profile evidence before releasing the readers. The
	// resource lifecycle still closes every reader before terminal return, while
	// EXPLAIN/acceptance callers retain stable post-close metrics.
	s.finalMetrics = s.collectMetrics()
	s.metricsFrozen = true
	s.closed = true
	for _, reader := range s.readers {
		reader.Close()
	}
	s.readers = nil
}

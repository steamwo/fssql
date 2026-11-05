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
	"github.com/steamwo/fssql/internal/storage/vfs"
)

var ErrUnsupportedStructLayout = errors.New("parquet: unsupported STRUCT layout")

type StructColumnReader struct {
	node        *SchemaNode
	leaves      []*SchemaNode
	cursors     []*ColumnCursor
	scope       *memory.Scope
	bm          *memory.BufferManager
	totalRows   int64
	rowPosition int64
	closed      bool
}

func NewStructColumnReader(driver vfs.Driver, uri vfs.URI, identity vfs.ObjectIdentity, rg RowGroup, node *SchemaNode, scope *memory.Scope, bm *memory.BufferManager) (*StructColumnReader, error) {
	if driver == nil || node == nil || scope == nil || bm == nil {
		return nil, errors.New("parquet: struct reader requires driver, schema, scope, and buffer manager")
	}
	if node.Logical.ID != coretypes.Struct || node.MaxRep != 0 || len(node.Children) == 0 {
		return nil, ErrUnsupportedStructLayout
	}
	r := &StructColumnReader{node: node, scope: scope, bm: bm, totalRows: rg.NumRows}
	for _, child := range node.Children {
		if child == nil || child.Element.Type == nil || len(child.Children) != 0 || child.MaxRep != 0 {
			r.Close()
			return nil, ErrUnsupportedStructLayout
		}
		meta := metadataForPath(rg, child.Path)
		if meta == nil {
			r.Close()
			return nil, fmt.Errorf("parquet: STRUCT child %q has no column metadata", strings.Join(child.Path, "."))
		}
		if meta.NumValues != rg.NumRows {
			r.Close()
			return nil, fmt.Errorf("parquet: STRUCT child %q has %d values for %d rows", strings.Join(child.Path, "."), meta.NumValues, rg.NumRows)
		}
		cursor, err := NewColumnCursor(driver, uri, identity, meta, child, scope, bm)
		if err != nil {
			r.Close()
			return nil, err
		}
		r.leaves = append(r.leaves, child)
		r.cursors = append(r.cursors, cursor)
	}
	return r, nil
}

func metadataForPath(rg RowGroup, path []string) *ColumnMetaData {
	want := strings.Join(path, ".")
	for i := range rg.Columns {
		meta := rg.Columns[i].MetaData
		if meta != nil && strings.Join(meta.PathInSchema, ".") == want {
			return meta
		}
	}
	return nil
}

func (r *StructColumnReader) RowPosition() int64 { return r.rowPosition }

func (r *StructColumnReader) Metrics() ColumnCursorMetrics {
	var out ColumnCursorMetrics
	if r == nil {
		return out
	}
	for _, cursor := range r.cursors {
		m := cursor.Metrics()
		out.Requests += m.Requests
		out.PlannedBytes += m.PlannedBytes
		out.TransferredBytes += m.TransferredBytes
		out.PagesRead += m.PagesRead
		out.PagesSkipped += m.PagesSkipped
		out.UsefulBytes += m.UsefulBytes
		out.DataValues += m.DataValues
	}
	return out
}

func (r *StructColumnReader) Close() {
	if r == nil || r.closed {
		return
	}
	r.closed = true
	for _, cursor := range r.cursors {
		cursor.Close()
	}
	r.cursors = nil
}

func (r *StructColumnReader) Read(ctx context.Context, rows int) (*vector.Vector, error) {
	if r == nil || r.closed {
		return nil, errors.New("parquet: struct reader is closed")
	}
	if rows <= 0 || rows > vector.DefaultVectorSize {
		return nil, fmt.Errorf("parquet: invalid struct batch size %d", rows)
	}
	remaining := r.totalRows - r.rowPosition
	if remaining <= 0 {
		return nil, io.EOF
	}
	if int64(rows) > remaining {
		rows = int(remaining)
	}
	out, err := vector.NewFlat(r.scope, r.bm, r.node.Logical, rows)
	if err != nil {
		return nil, err
	}
	children := make([]*vector.Vector, 0, len(r.leaves))
	keep := false
	defer func() {
		if !keep {
			for _, child := range children {
				child.Release()
			}
			out.Release()
		}
	}()
	for _, leaf := range r.leaves {
		child, err := vector.NewFlat(r.scope, r.bm, leaf.Logical, rows)
		if err != nil {
			return nil, err
		}
		children = append(children, child)
	}
	for row := 0; row < rows; row++ {
		parentKnown := false
		parentPresent := true
		for i, cursor := range r.cursors {
			entry, err := cursor.PeekEntry(ctx)
			if err != nil {
				return nil, err
			}
			if entry.Repetition != 0 {
				return nil, ErrUnsupportedStructLayout
			}
			present := entry.Definition >= r.node.MaxDef
			if !parentKnown {
				parentPresent = present
				parentKnown = true
			} else if parentPresent != present {
				return nil, errors.New("parquet: STRUCT child definition levels disagree on parent validity")
			}
			consumed, err := cursor.NextInto(ctx, children[i], row)
			if err != nil {
				return nil, err
			}
			if !consumed.HasValue {
				children[i].SetNull(row)
			}
		}
		if !parentPresent {
			out.SetNull(row)
		}
		r.rowPosition++
	}
	out.SetChildren(children...)
	keep = true
	return out, nil
}

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

// ListStructColumnReader reconstructs the standard three-level LIST encoding
// when its element is a non-repeated STRUCT whose fields are primitive leaves.
// It intentionally rejects legacy/ambiguous LIST layouts rather than guessing.
type ListStructColumnReader struct {
	list        *SchemaNode
	repeated    *SchemaNode
	element     *SchemaNode
	leaves      []*SchemaNode
	cursors     []*ColumnCursor
	scope       *memory.Scope
	bm          *memory.BufferManager
	totalRows   int64
	rowPosition int64
	closed      bool
}

func NewListStructColumnReader(driver vfs.Driver, uri vfs.URI, identity vfs.ObjectIdentity, rg RowGroup, listNode *SchemaNode, scope *memory.Scope, bm *memory.BufferManager) (*ListStructColumnReader, error) {
	if driver == nil || listNode == nil || scope == nil || bm == nil {
		return nil, errors.New("parquet: list-of-struct reader requires driver, schema, scope, and buffer manager")
	}
	if rg.NumRows < 0 {
		return nil, errors.New("parquet: negative list-of-struct row count")
	}
	repeated, element, leaves, err := validateStandardListStruct(listNode)
	if err != nil {
		return nil, err
	}
	r := &ListStructColumnReader{list: listNode, repeated: repeated, element: element, leaves: leaves, scope: scope, bm: bm, totalRows: rg.NumRows}
	for _, leaf := range leaves {
		meta := metadataForPath(rg, leaf.Path)
		if meta == nil {
			r.Close()
			return nil, fmt.Errorf("parquet: LIST<STRUCT> leaf %q has no metadata", strings.Join(leaf.Path, "."))
		}
		cursor, err := NewColumnCursor(driver, uri, identity, meta, leaf, scope, bm)
		if err != nil {
			r.Close()
			return nil, err
		}
		r.cursors = append(r.cursors, cursor)
	}
	return r, nil
}

func validateStandardListStruct(listNode *SchemaNode) (*SchemaNode, *SchemaNode, []*SchemaNode, error) {
	if listNode == nil || !isListAnnotated(listNode.Element) || listNode.Logical.ID != coretypes.List || len(listNode.Children) != 1 {
		return nil, nil, nil, ErrUnsupportedListLayout
	}
	repeated := listNode.Children[0]
	if repeated.Element.Repetition == nil || *repeated.Element.Repetition != Repeated || repeated.Element.Type != nil || len(repeated.Children) != 1 {
		return nil, nil, nil, ErrUnsupportedListLayout
	}
	element := repeated.Children[0]
	if element.Element.Type != nil || element.Logical.ID != coretypes.Struct || element.MaxRep != repeated.MaxRep || len(element.Children) == 0 {
		return nil, nil, nil, ErrUnsupportedListLayout
	}
	if element.Element.Repetition != nil && *element.Element.Repetition == Repeated {
		return nil, nil, nil, ErrUnsupportedListLayout
	}
	leaves := make([]*SchemaNode, 0, len(element.Children))
	for _, child := range element.Children {
		if child == nil || child.Element.Type == nil || len(child.Children) != 0 || child.MaxRep != repeated.MaxRep {
			return nil, nil, nil, ErrUnsupportedListLayout
		}
		leaves = append(leaves, child)
	}
	return repeated, element, leaves, nil
}

func (r *ListStructColumnReader) RowPosition() int64 { return r.rowPosition }

func (r *ListStructColumnReader) Metrics() ColumnCursorMetrics {
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
		out.DataValues += m.DataValues
	}
	return out
}

func (r *ListStructColumnReader) Close() {
	if r == nil || r.closed {
		return
	}
	r.closed = true
	for _, cursor := range r.cursors {
		cursor.Close()
	}
	r.cursors = nil
}

func (r *ListStructColumnReader) Read(ctx context.Context, rows int) (*vector.Vector, error) {
	if r == nil || r.closed {
		return nil, errors.New("parquet: list-of-struct reader is closed")
	}
	if rows <= 0 || rows > vector.DefaultVectorSize {
		return nil, fmt.Errorf("parquet: invalid list-of-struct batch size %d", rows)
	}
	remaining := r.totalRows - r.rowPosition
	if remaining <= 0 {
		return nil, io.EOF
	}
	if int64(rows) > remaining {
		rows = int(remaining)
	}
	out, err := vector.NewFlat(r.scope, r.bm, r.list.Logical, rows)
	if err != nil {
		return nil, err
	}
	keepOut := false
	defer func() {
		if !keepOut {
			out.Release()
		}
	}()

	capHint := 64
	scratch := make([]*vector.Vector, 0, len(r.leaves))
	for _, leaf := range r.leaves {
		v, err := vector.NewFlat(r.scope, r.bm, leaf.Logical, capHint)
		if err != nil {
			for _, current := range scratch {
				current.Release()
			}
			return nil, err
		}
		scratch = append(scratch, v)
	}
	keepScratch := false
	defer func() {
		if !keepScratch {
			for _, current := range scratch {
				current.Release()
			}
		}
	}()

	elementValid := make([]bool, 0, capHint)
	entries := make([]ColumnEntry, len(r.cursors))
	used := 0
	for row := 0; row < rows; row++ {
		first, err := r.cursors[0].PeekEntry(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
		if first.Repetition > r.list.MaxRep {
			return nil, errors.New("parquet: LIST<STRUCT> row does not start at parent repetition level")
		}
		start := used
		listNull := first.Definition < r.list.MaxDef
		firstEntry := true
		for {
			for i, cursor := range r.cursors {
				entry, err := cursor.PeekEntry(ctx)
				if err != nil {
					return nil, err
				}
				entries[i] = entry
				if entry.Repetition != entries[0].Repetition {
					return nil, errors.New("parquet: LIST<STRUCT> leaf repetition levels lost alignment")
				}
				if (entry.Definition >= r.list.MaxDef) != (entries[0].Definition >= r.list.MaxDef) {
					return nil, errors.New("parquet: LIST<STRUCT> leaf definitions disagree on list validity")
				}
				if (entry.Definition >= r.element.MaxDef) != (entries[0].Definition >= r.element.MaxDef) {
					return nil, errors.New("parquet: LIST<STRUCT> leaf definitions disagree on element validity")
				}
			}
			if !firstEntry && entries[0].Repetition <= r.list.MaxRep {
				break
			}
			hasElementSlot := entries[0].Definition >= r.repeated.MaxDef
			if !firstEntry && !hasElementSlot {
				return nil, errors.New("parquet: LIST<STRUCT> empty/null list has repeated continuation")
			}
			for i := range scratch {
				var err error
				scratch[i], err = ensureVectorCapacity(r.scope, r.bm, scratch[i], used, used+1)
				if err != nil {
					return nil, err
				}
			}
			for i, cursor := range r.cursors {
				consumed, err := cursor.NextInto(ctx, scratch[i], used)
				if err != nil {
					return nil, err
				}
				if hasElementSlot && !consumed.HasValue {
					scratch[i].SetNull(used)
				}
			}
			if hasElementSlot {
				elementValid = append(elementValid, entries[0].Definition >= r.element.MaxDef)
				used++
			}
			firstEntry = false
			if r.cursors[0].Consumed() >= r.cursors[0].meta.NumValues {
				break
			}
			next, err := r.cursors[0].PeekEntry(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return nil, err
			}
			if next.Repetition <= r.list.MaxRep {
				break
			}
		}
		out.SetListEntry(row, vector.ListEntry{Offset: uint32(start), Length: uint32(used - start)})
		if listNull {
			out.SetNull(row)
		}
		r.rowPosition++
	}

	structChild, err := vector.NewFlat(r.scope, r.bm, r.element.Logical, used)
	if err != nil {
		return nil, err
	}
	children := make([]*vector.Vector, 0, len(r.leaves))
	for i, leaf := range r.leaves {
		child, err := vector.NewFlat(r.scope, r.bm, leaf.Logical, used)
		if err != nil {
			for _, current := range children {
				current.Release()
			}
			structChild.Release()
			return nil, err
		}
		for row := 0; row < used; row++ {
			if err := copyDictionaryValue(child, row, scratch[i], row); err != nil {
				child.Release()
				for _, current := range children {
					current.Release()
				}
				structChild.Release()
				return nil, err
			}
		}
		children = append(children, child)
	}
	for i, valid := range elementValid {
		if !valid {
			structChild.SetNull(i)
		}
	}
	structChild.SetChildren(children...)
	for _, current := range scratch {
		current.Release()
	}
	keepScratch = true
	out.SetChildren(structChild)
	keepOut = true
	return out, nil
}

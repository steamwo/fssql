package parquet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/steamwo/fssql/internal/core/memory"
	coretypes "github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

var ErrUnsupportedListLayout = errors.New("parquet: unsupported LIST layout")

type ListColumnReader struct {
	cursor      *ColumnCursor
	list        *SchemaNode
	repeated    *SchemaNode
	leaf        *SchemaNode
	scope       *memory.Scope
	bm          *memory.BufferManager
	totalRows   int64
	rowPosition int64
	closed      bool
}

func NewListColumnReader(driver vfs.Driver, uri vfs.URI, identity vfs.ObjectIdentity, meta *ColumnMetaData, listNode *SchemaNode, totalRows int64, scope *memory.Scope, bm *memory.BufferManager) (*ListColumnReader, error) {
	if listNode == nil || scope == nil || bm == nil {
		return nil, errors.New("parquet: list reader requires list schema, scope, and buffer manager")
	}
	if totalRows < 0 {
		return nil, errors.New("parquet: negative list row count")
	}
	repeated, leaf, err := validateStandardList(listNode)
	if err != nil {
		return nil, err
	}
	if meta == nil || strings.Join(meta.PathInSchema, ".") != strings.Join(leaf.Path, ".") {
		return nil, errors.New("parquet: LIST leaf metadata path mismatch")
	}
	cursor, err := NewColumnCursor(driver, uri, identity, meta, leaf, scope, bm)
	if err != nil {
		return nil, err
	}
	return &ListColumnReader{cursor: cursor, list: listNode, repeated: repeated, leaf: leaf, scope: scope, bm: bm, totalRows: totalRows}, nil
}

func validateStandardList(listNode *SchemaNode) (*SchemaNode, *SchemaNode, error) {
	if !isListAnnotated(listNode.Element) || listNode.Logical.ID != coretypes.List || len(listNode.Children) != 1 {
		return nil, nil, ErrUnsupportedListLayout
	}
	repeated := listNode.Children[0]
	if repeated.Element.Repetition == nil || *repeated.Element.Repetition != Repeated || repeated.Element.Type != nil || len(repeated.Children) != 1 {
		return nil, nil, ErrUnsupportedListLayout
	}
	leaf := repeated.Children[0]
	if leaf.Element.Type == nil || len(leaf.Children) != 0 {
		return nil, nil, ErrUnsupportedListLayout
	}
	if repeated.MaxRep != listNode.MaxRep+1 || leaf.MaxRep != repeated.MaxRep {
		return nil, nil, ErrUnsupportedListLayout
	}
	return repeated, leaf, nil
}

func (r *ListColumnReader) RowPosition() int64 { return r.rowPosition }
func (r *ListColumnReader) Metrics() ColumnCursorMetrics {
	if r == nil || r.cursor == nil {
		return ColumnCursorMetrics{}
	}
	return r.cursor.Metrics()
}

func (r *ListColumnReader) Close() {
	if r == nil || r.closed {
		return
	}
	r.closed = true
	if r.cursor != nil {
		r.cursor.Close()
	}
}

func (r *ListColumnReader) Read(ctx context.Context, rows int) (*vector.Vector, error) {
	if r == nil || r.closed {
		return nil, errors.New("parquet: list reader is closed")
	}
	if rows <= 0 || rows > vector.DefaultVectorSize {
		return nil, fmt.Errorf("parquet: invalid list batch size %d", rows)
	}
	remainingRows := r.totalRows - r.rowPosition
	if remainingRows <= 0 {
		return nil, io.EOF
	}
	if int64(rows) > remainingRows {
		rows = int(remainingRows)
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
	remainingEntries := r.cursor.meta.NumValues - r.cursor.Consumed()
	if remainingEntries > 0 && remainingEntries < int64(capHint) {
		capHint = int(remainingEntries)
	}
	if capHint < 1 {
		capHint = 1
	}
	scratch, err := vector.NewFlat(r.scope, r.bm, r.leaf.Logical, capHint)
	if err != nil {
		return nil, err
	}
	keepScratch := false
	defer func() {
		if !keepScratch {
			scratch.Release()
		}
	}()
	used := 0

	for row := 0; row < rows; row++ {
		first, err := r.cursor.PeekEntry(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
		if first.Repetition > r.list.MaxRep {
			return nil, errors.New("parquet: LIST row does not start at parent repetition level")
		}
		start := used
		listNull := first.Definition < r.list.MaxDef
		hasRepeated := first.Definition >= r.repeated.MaxDef
		firstEntry := true
		for {
			entry, err := r.cursor.PeekEntry(ctx)
			if err != nil {
				return nil, err
			}
			if !firstEntry && entry.Repetition <= r.list.MaxRep {
				break
			}
			if !firstEntry && !hasRepeated {
				return nil, errors.New("parquet: NULL or empty LIST has repeated continuation")
			}
			if !firstEntry && entry.Definition < r.repeated.MaxDef {
				return nil, errors.New("parquet: LIST continuation does not define repeated element")
			}
			scratch, err = ensureVectorCapacity(r.scope, r.bm, scratch, used, used+1)
			if err != nil {
				return nil, err
			}
			consumed, err := r.cursor.NextInto(ctx, scratch, used)
			if err != nil {
				return nil, err
			}
			if consumed.Definition >= r.repeated.MaxDef {
				if !consumed.HasValue {
					scratch.SetNull(used)
				}
				used++
				if uint64(used) > math.MaxUint32 {
					return nil, errors.New("parquet: LIST child offset exceeds uint32")
				}
			}
			firstEntry = false
			if r.cursor.Consumed() >= r.cursor.meta.NumValues {
				break
			}
			next, err := r.cursor.PeekEntry(ctx)
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

	child, err := vector.NewFlat(r.scope, r.bm, r.leaf.Logical, used)
	if err != nil {
		return nil, err
	}
	for i := 0; i < used; i++ {
		if err := copyDictionaryValue(child, i, scratch, i); err != nil {
			child.Release()
			return nil, err
		}
	}
	scratch.Release()
	keepScratch = true
	out.SetChildren(child)
	keepOut = true
	return out, nil
}

func ensureVectorCapacity(scope *memory.Scope, bm *memory.BufferManager, current *vector.Vector, used, needed int) (*vector.Vector, error) {
	if needed <= current.Count() {
		return current, nil
	}
	newCap := current.Count() * 2
	if newCap < needed {
		newCap = needed
	}
	if newCap < 1 {
		newCap = 1
	}
	next, err := vector.NewFlat(scope, bm, current.Type, newCap)
	if err != nil {
		return current, err
	}
	for i := 0; i < used; i++ {
		if err := copyDictionaryValue(next, i, current, i); err != nil {
			next.Release()
			return current, err
		}
	}
	current.Release()
	return next, nil
}

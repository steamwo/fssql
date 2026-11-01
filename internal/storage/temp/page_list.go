package temp

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/steamwo/fssql/internal/core/memory"
)

// PageList keeps an unbounded sequence of spill page IDs in managed memory.
type PageList struct {
	scope    *memory.Scope
	buffers  *memory.BufferManager
	buf      *memory.Buffer
	count    int
	capacity int
}

func NewPageList(scope *memory.Scope, buffers *memory.BufferManager) (*PageList, error) {
	if scope == nil || buffers == nil {
		return nil, errors.New("page list requires memory scope and buffer manager")
	}
	b, err := buffers.Allocate(scope, memory.TagMetadata, 8*8)
	if err != nil {
		return nil, err
	}
	return &PageList{scope: scope, buffers: buffers, buf: b, capacity: 8}, nil
}

func (l *PageList) Count() int { return l.count }

func (l *PageList) Append(id PageID) error {
	if l.buf == nil {
		return ErrClosed
	}
	if l.count == l.capacity {
		if l.capacity > math.MaxInt/2 {
			return errors.New("spill page list capacity overflow")
		}
		next := l.capacity * 2
		if err := l.buf.Resize(next*8, nil); err != nil {
			return err
		}
		l.capacity = next
	}
	binary.LittleEndian.PutUint64(l.buf.Bytes()[l.count*8:l.count*8+8], uint64(id))
	l.count++
	return nil
}

func (l *PageList) At(i int) (PageID, bool) {
	if l.buf == nil || i < 0 || i >= l.count {
		return 0, false
	}
	return PageID(binary.LittleEndian.Uint64(l.buf.Bytes()[i*8 : i*8+8])), true
}

func (l *PageList) Release() {
	if l.buf != nil {
		l.buf.Release()
		l.buf = nil
	}
	l.count = 0
	l.capacity = 0
}

package vector

import (
	"errors"
	"math"

	"github.com/steamwo/fssql/internal/core/types"
)

// StringBatchWriter borrows the destination data/validity and the string heap
// for one batch. It removes repeated buffer acquisition and growth checks from
// per-value string loading; Commit publishes the appended heap bytes.
type StringBatchWriter struct {
	data     []byte
	validity *ValidityMask
	heap     *StringHeap
	heapData []byte
	used     int
	count    int
}

func (v *Vector) NewStringBatchWriter() (*StringBatchWriter, error) {
	if v == nil || v.Kind != Flat || v.Type.ID != types.Varchar || v.data == nil || v.heap == nil || v.heap.buf == nil {
		return nil, errors.New("string batch requires a flat varchar vector")
	}
	return &StringBatchWriter{
		data:     v.data.Bytes(),
		validity: &v.validity,
		heap:     v.heap,
		heapData: v.heap.buf.Bytes(),
		used:     v.heap.used,
		count:    v.count,
	}, nil
}

func (w *StringBatchWriter) Append(row int, src []byte) error {
	if w == nil || w.heap == nil {
		return errors.New("string batch writer is closed")
	}
	if row < 0 || row >= w.count {
		return errors.New("string batch row out of bounds")
	}
	if uint64(len(src)) > math.MaxUint32 {
		return errors.New("string batch value exceeds uint32 length")
	}
	var ref StringRef
	ref.Length = uint32(len(src))
	if len(src) <= len(ref.Inline) {
		copy(ref.Inline[:], src)
	} else {
		const maxInt = int(^uint(0) >> 1)
		if len(src) > maxInt-w.used {
			return errors.New("string batch heap size overflow")
		}
		need := w.used + len(src)
		if need > len(w.heapData) {
			next := len(w.heapData) * 2
			if next < need {
				next = need
			}
			if err := w.heap.buf.Resize(next, nil); err != nil {
				return err
			}
			w.heapData = w.heap.buf.Bytes()
		}
		copy(w.heapData[w.used:need], src)
		ref.Offset = uint64(w.used)
		ref.External = true
		w.used = need
	}
	encodeStringRef(w.data[row*24:row*24+24], ref)
	w.validity.SetValid(row, true)
	return nil
}

func (w *StringBatchWriter) Commit() {
	if w == nil || w.heap == nil {
		return
	}
	if w.used > w.heap.used {
		w.heap.used = w.used
	}
	w.data = nil
	w.heapData = nil
	w.validity = nil
	w.heap = nil
}

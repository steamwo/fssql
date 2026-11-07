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
	return w.AppendParts(row, src, nil)
}

// Reserve 为后续追加的外部字符串精确预留堆字节，不收缩已有容量。
func (w *StringBatchWriter) Reserve(size int) error {
	if w == nil || w.heap == nil {
		return errors.New("string batch writer is closed")
	}
	if size < 0 {
		return errors.New("negative string batch heap reservation")
	}
	const maxInt = int(^uint(0) >> 1)
	if size > maxInt-w.used {
		return errors.New("string batch heap size overflow")
	}
	need := w.used + size
	if need <= len(w.heapData) {
		return nil
	}
	if err := w.heap.buf.Resize(need, nil); err != nil {
		return err
	}
	w.heapData = w.heap.buf.Bytes()
	return nil
}

// AppendParts copies two byte slices directly into the owned destination,
// avoiding an intermediate concatenation allocation.
func (w *StringBatchWriter) AppendParts(row int, left, right []byte) error {
	if w == nil || w.heap == nil {
		return errors.New("string batch writer is closed")
	}
	if row < 0 || row >= w.count {
		return errors.New("string batch row out of bounds")
	}
	size := uint64(len(left)) + uint64(len(right))
	if size > math.MaxUint32 || size > uint64(int(^uint(0)>>1)) {
		return errors.New("string batch value exceeds uint32 length")
	}
	length := int(size)
	var ref StringRef
	ref.Length = uint32(length)
	if length <= len(ref.Inline) {
		copy(ref.Inline[:], left)
		copy(ref.Inline[len(left):], right)
	} else {
		const maxInt = int(^uint(0) >> 1)
		if length > maxInt-w.used {
			return errors.New("string batch heap size overflow")
		}
		need := w.used + length
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
		copy(w.heapData[w.used:need], left)
		copy(w.heapData[w.used+len(left):need], right)
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

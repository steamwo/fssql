package export

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/maphash"
	"math"
	"math/bits"

	"github.com/golang/snappy"
	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/format/parquet"
)

const (
	maxParquetDictionaryEntries    = 256
	maxParquetDictionaryBytes      = 32 << 10
	parquetDictionaryMetadataBytes = 8 << 10
	parquetNullDictionaryIndex     = uint16(0xffff)
)

type parquetStringDictionary struct {
	reservation         *memory.Reservation
	indices             *memory.Buffer
	entries             [maxParquetDictionaryEntries][]byte     // Borrow immutable row-group values.
	slots               [maxParquetDictionaryEntries * 2]uint16 // Entry index + 1; zero is unused.
	count, bytes, width int
}

func (d *parquetStringDictionary) Release() {
	if d != nil {
		if d.indices != nil {
			d.indices.Release()
			d.indices = nil
		}
		if d.reservation != nil {
			d.reservation.Release()
			d.reservation = nil
		}
	}
}

// Plan the complete column before emitting a dictionary page. Large or
// high-cardinality inputs and optional allocation limits keep PLAIN encoding.
func planParquetDictionary(ctx context.Context, vec *vector.Vector, count, pageRows int, scope *memory.Scope, bm *memory.BufferManager) (*parquetStringDictionary, error) {
	if vec.Type.ID != types.Varchar || count < 32 || count > math.MaxInt/2 || count > math.MaxInt32 {
		return nil, nil
	}
	reservation, err := scope.Reserve(memory.TagMetadata, parquetDictionaryMetadataBytes)
	if errors.Is(err, memory.ErrLimitExceeded) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	d := &parquetStringDictionary{reservation: reservation}
	keep := false
	defer func() {
		if !keep {
			d.Release()
		}
	}()
	d.indices, err = bm.Allocate(scope, memory.TagExportBuffer, count*2)
	if errors.Is(err, memory.ErrLimitExceeded) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	indices := d.indices.Bytes()
	view, _ := vec.BorrowBytes()
	seed := maphash.MakeSeed()
	var plainBytes int64
	defined := 0
	for row := 0; row < count; row++ {
		if row&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		value, valid := view.Value(row)
		if !valid {
			binary.LittleEndian.PutUint16(indices[row*2:], parquetNullDictionaryIndex)
			continue
		}
		if len(value) > maxParquetDictionaryBytes-4 {
			return nil, nil
		}
		plainBytes += int64(4 + len(value))
		defined++
		slot := int(maphash.Bytes(seed, value) & uint64(len(d.slots)-1))
		for d.slots[slot] != 0 && !bytes.Equal(d.entries[int(d.slots[slot])-1], value) {
			slot = (slot + 1) & (len(d.slots) - 1)
		}
		if d.slots[slot] == 0 {
			if d.count == len(d.entries) || len(value)+4 > maxParquetDictionaryBytes-d.bytes {
				return nil, nil
			}
			d.entries[d.count] = value
			d.count++
			d.bytes += 4 + len(value)
			d.slots[slot] = uint16(d.count)
		}
		binary.LittleEndian.PutUint16(indices[row*2:], d.slots[slot]-1)
	}
	if d.count == 0 || defined < 2*d.count {
		return nil, nil
	}
	d.width = bits.Len(uint(d.count - 1))
	pages := 1 + (count-1)/pageRows
	estimated := int64(d.bytes) + (int64(defined)*int64(d.width)+7)/8 + int64(pages)*(11+int64(d.width)) + 64
	if estimated > plainBytes-plainBytes/4 {
		return nil, nil
	}
	keep = true
	return d, nil
}

func (d *parquetStringDictionary) encodeIndices(out *parquetPageBuffer, start, count int) error {
	if err := out.AppendByte(byte(d.width)); err != nil {
		return err
	}
	indices := d.indices.Bytes()[start*2 : (start+count)*2]
	defined := 0
	first := parquetNullDictionaryIndex
	same := true
	for row := 0; row < count; row++ {
		index := binary.LittleEndian.Uint16(indices[row*2:])
		if index == parquetNullDictionaryIndex {
			continue
		}
		if defined == 0 {
			first = index
		} else if index != first {
			same = false
		}
		defined++
	}
	if defined == 0 {
		return nil
	}
	if same {
		if err := out.AppendUvarint(uint64(defined) << 1); err != nil {
			return err
		}
		if d.width > 0 {
			return out.AppendByte(byte(first))
		}
		return nil
	}
	groups := (defined + 7) / 8
	if err := out.AppendUvarint(uint64(groups)<<1 | 1); err != nil {
		return err
	}
	var word uint64
	var scratch [8]byte
	position := 0
	for row := 0; row < count; row++ {
		index := binary.LittleEndian.Uint16(indices[row*2:])
		if index == parquetNullDictionaryIndex {
			continue
		}
		word |= uint64(index) << uint(position*d.width)
		position++
		if position == 8 {
			binary.LittleEndian.PutUint64(scratch[:], word)
			if err := out.Append(scratch[:d.width]); err != nil {
				return err
			}
			position, word = 0, 0
		}
	}
	if position != 0 {
		binary.LittleEndian.PutUint64(scratch[:], word)
		return out.Append(scratch[:d.width])
	}
	return nil
}

// Prepare all dictionary bytes and encrypted frames before the first write.
// A pre-write memory error can therefore fall back without replay or corruption.
func (w *ParquetWriter) writeDictionaryPage(ctx context.Context, body *parquetPageBuffer, d *parquetStringDictionary, rowGroup, column int16) (committed bool, uncompressed, compressed int64, err error) {
	body.used = 0
	var prefix [4]byte
	for _, entry := range d.entries[:d.count] {
		binary.LittleEndian.PutUint32(prefix[:], uint32(len(entry)))
		if err := body.Append(prefix[:]); err != nil {
			return false, 0, 0, err
		}
		if err := body.Append(entry); err != nil {
			return false, 0, 0, err
		}
	}
	payload := body.Bytes()
	var compressedBuffer, headerFrame, dataFrame *memory.Buffer
	defer func() {
		if compressedBuffer != nil {
			compressedBuffer.Release()
		}
		if headerFrame != nil {
			headerFrame.Release()
		}
		if dataFrame != nil {
			dataFrame.Release()
		}
	}()
	if w.opts.Compression == "snappy" {
		compressedBuffer, err = w.bm.Allocate(w.scope, memory.TagExportBuffer, snappy.MaxEncodedLen(len(payload)))
		if err != nil {
			return false, 0, 0, err
		}
		payload = snappy.Encode(compressedBuffer.Bytes(), payload)
	}
	onDisk := len(payload)
	if w.opts.Encryption != nil {
		onDisk, err = parquetEncryptedFrameSize(onDisk)
		if err != nil {
			return false, 0, 0, err
		}
	}
	header, err := parquet.EncodePageHeader(parquet.PageHeader{Type: parquet.PageDictionary, UncompressedPageSize: int32(body.Len()), CompressedPageSize: int32(onDisk), Dictionary: &parquet.DictionaryPageHeader{NumValues: int32(d.count), Encoding: parquet.EncodingPlain}})
	if err != nil {
		return false, 0, 0, err
	}
	if w.opts.Encryption != nil {
		header, headerFrame, err = w.encryptManaged(header, parquetModuleDictionaryPageHeader, rowGroup, column, -1)
		if err != nil {
			return false, 0, 0, err
		}
		payload, dataFrame, err = w.encryptManaged(payload, parquetModuleDictionaryPage, rowGroup, column, -1)
		if err != nil {
			return false, 0, 0, err
		}
	}
	// Keep conservative headroom for data-page indices, compression/encryption
	// and bounded statistics. Reject this optional encoding before any write.
	rows := min(w.rowCount, w.opts.PageRows)
	if rows > math.MaxInt32 {
		return false, 0, 0, memory.ErrLimitExceeded
	}
	bound := int64(rows)*3 + 32
	capacity := int64(body.buffer.Len())
	for capacity < bound {
		if capacity > math.MaxInt64/2 {
			capacity = bound
			break
		}
		capacity *= 2
	}
	extra := capacity - int64(body.buffer.Len()) + bound*3 + (8 << 10)
	workspace, err := w.scope.Reserve(memory.TagExportBuffer, extra)
	if err != nil {
		return false, 0, 0, err
	}
	workspace.Release()
	if err := w.writeBytes(ctx, header); err != nil {
		return true, 0, 0, err
	}
	if err := w.writeBytes(ctx, payload); err != nil {
		return true, 0, 0, err
	}
	return true, int64(len(header) + body.Len()), int64(len(header) + len(payload)), nil
}

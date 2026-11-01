package temp

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
)

const (
	spillBlockMagic      = "FSSP"
	spillBlockVersion    = uint16(1)
	spillBlockHeaderSize = 32
)

var (
	ErrCorruptSpillBlock = errors.New("corrupt spill block")
	spillChecksumTable   = crc32.MakeTable(crc32.Castagnoli)
)

type Compression uint8

const (
	CompressionNone Compression = iota
	CompressionRLE
)

type PageID uint64

type PageStoreConfig struct {
	SegmentMaxBytes  int64
	Compression      Compression
	MinCompressBytes int
}

type PageRef struct {
	ID            PageID
	SegmentID     uint64
	Offset        int64
	EncodedLength uint32
	RawLength     uint32
	Checksum      uint32
	Compression   Compression
}

type spillBlockHeader struct {
	compression Compression
	payloadLen  uint32
	rawLen      uint32
	checksum    uint32
}

func encodeSpillBlockHeader(dst *[spillBlockHeaderSize]byte, h spillBlockHeader) {
	copy(dst[0:4], spillBlockMagic)
	binary.LittleEndian.PutUint16(dst[4:6], spillBlockVersion)
	dst[6] = byte(h.compression)
	dst[7] = 0
	binary.LittleEndian.PutUint32(dst[8:12], h.payloadLen)
	binary.LittleEndian.PutUint32(dst[12:16], h.rawLen)
	binary.LittleEndian.PutUint32(dst[16:20], h.checksum)
	for i := 20; i < len(dst); i++ {
		dst[i] = 0
	}
}

func decodeSpillBlockHeader(src *[spillBlockHeaderSize]byte) (spillBlockHeader, error) {
	if string(src[0:4]) != spillBlockMagic {
		return spillBlockHeader{}, fmt.Errorf("%w: bad magic", ErrCorruptSpillBlock)
	}
	if version := binary.LittleEndian.Uint16(src[4:6]); version != spillBlockVersion {
		return spillBlockHeader{}, fmt.Errorf("%w: unsupported version %d", ErrCorruptSpillBlock, version)
	}
	compression := Compression(src[6])
	if compression != CompressionNone && compression != CompressionRLE {
		return spillBlockHeader{}, fmt.Errorf("%w: unsupported compression %d", ErrCorruptSpillBlock, compression)
	}
	for i := 20; i < len(src); i++ {
		if src[i] != 0 {
			return spillBlockHeader{}, fmt.Errorf("%w: non-zero reserved header byte", ErrCorruptSpillBlock)
		}
	}
	return spillBlockHeader{
		compression: compression,
		payloadLen:  binary.LittleEndian.Uint32(src[8:12]),
		rawLen:      binary.LittleEndian.Uint32(src[12:16]),
		checksum:    binary.LittleEndian.Uint32(src[16:20]),
	}, nil
}

func rleEncode(dst, src []byte) (int, bool) {
	if len(src) < 2 || len(dst) == 0 {
		return 0, false
	}
	out := 0
	for i := 0; i < len(src); {
		value := src[i]
		run := 1
		for i+run < len(src) && src[i+run] == value && run < 255 {
			run++
		}
		if out+2 > len(dst) {
			return 0, false
		}
		dst[out] = byte(run)
		dst[out+1] = value
		out += 2
		i += run
	}
	if out >= len(src) {
		return 0, false
	}
	return out, true
}

func rleDecode(dst, src []byte) error {
	out := 0
	for i := 0; i < len(src); {
		if i+1 >= len(src) {
			return fmt.Errorf("%w: truncated RLE pair", ErrCorruptSpillBlock)
		}
		run := int(src[i])
		if run == 0 {
			return fmt.Errorf("%w: zero-length RLE run", ErrCorruptSpillBlock)
		}
		if out+run > len(dst) {
			return fmt.Errorf("%w: RLE output exceeds raw length", ErrCorruptSpillBlock)
		}
		value := src[i+1]
		for j := 0; j < run; j++ {
			dst[out+j] = value
		}
		out += run
		i += 2
	}
	if out != len(dst) {
		return fmt.Errorf("%w: RLE output length %d, want %d", ErrCorruptSpillBlock, out, len(dst))
	}
	return nil
}

type spillSegment struct {
	id   uint64
	file *File
	live int
}

type SpillablePageStore struct {
	mu sync.Mutex

	tempScope   *QueryScope
	memoryScope *memory.Scope
	buffers     *memory.BufferManager
	config      PageStoreConfig

	nextPageID    PageID
	nextSegmentID uint64
	current       *spillSegment
	segments      map[uint64]*spillSegment
	pages         map[PageID]PageRef
	closed        bool
	poisoned      error
}

func NewSpillablePageStore(tempScope *QueryScope, memoryScope *memory.Scope, buffers *memory.BufferManager, config PageStoreConfig) (*SpillablePageStore, error) {
	if tempScope == nil {
		return nil, errors.New("temporary query scope is required")
	}
	if memoryScope == nil {
		return nil, errors.New("memory scope is required")
	}
	if buffers == nil {
		return nil, errors.New("buffer manager is required")
	}
	if config.SegmentMaxBytes <= 0 {
		config.SegmentMaxBytes = 64 << 20
	}
	if config.MinCompressBytes <= 0 {
		config.MinCompressBytes = 1024
	}
	if config.Compression != CompressionNone && config.Compression != CompressionRLE {
		return nil, fmt.Errorf("unsupported spill compression %d", config.Compression)
	}
	return &SpillablePageStore{
		tempScope:   tempScope,
		memoryScope: memoryScope,
		buffers:     buffers,
		config:      config,
		segments:    make(map[uint64]*spillSegment),
		pages:       make(map[PageID]PageRef),
	}, nil
}

func (s *SpillablePageStore) PageCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pages)
}

func (s *SpillablePageStore) Ref(id PageID) (PageRef, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ref, ok := s.pages[id]
	return ref, ok
}

func (s *SpillablePageStore) ensureWritableSegment(blockBytes int64) (*spillSegment, error) {
	if s.current != nil && s.current.file.Size() > 0 && s.current.file.Size()+blockBytes > s.config.SegmentMaxBytes {
		if err := s.current.file.Sync(); err != nil {
			return nil, err
		}
		s.current = nil
	}
	if s.current != nil {
		return s.current, nil
	}
	f, err := s.tempScope.CreateFile()
	if err != nil {
		return nil, err
	}
	s.nextSegmentID++
	seg := &spillSegment{id: s.nextSegmentID, file: f}
	s.segments[seg.id] = seg
	s.current = seg
	return seg, nil
}

func (s *SpillablePageStore) encodePayload(data []byte) (payload []byte, compression Compression, scratch *memory.Buffer) {
	if s.config.Compression != CompressionRLE || len(data) < s.config.MinCompressBytes || len(data) == 0 {
		return data, CompressionNone, nil
	}
	buf, err := s.buffers.Allocate(s.memoryScope, memory.TagSpillIO, len(data))
	if err != nil {
		return data, CompressionNone, nil
	}
	encoded, ok := rleEncode(buf.Bytes(), data)
	if !ok {
		buf.Release()
		return data, CompressionNone, nil
	}
	return buf.Bytes()[:encoded], CompressionRLE, buf
}

func (s *SpillablePageStore) WritePage(ctx context.Context, data []byte) (PageID, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if uint64(len(data)) > uint64(^uint32(0)) {
		return 0, fmt.Errorf("spill page too large: %d bytes", len(data))
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrClosed
	}
	if s.poisoned != nil {
		return 0, s.poisoned
	}

	payload, compression, scratch := s.encodePayload(data)
	if scratch != nil {
		defer scratch.Release()
	}
	if uint64(len(payload)) > uint64(^uint32(0)) {
		return 0, fmt.Errorf("encoded spill page too large: %d bytes", len(payload))
	}

	header := spillBlockHeader{
		compression: compression,
		payloadLen:  uint32(len(payload)),
		rawLen:      uint32(len(data)),
		checksum:    crc32.Checksum(data, spillChecksumTable),
	}
	var rawHeader [spillBlockHeaderSize]byte
	encodeSpillBlockHeader(&rawHeader, header)
	blockBytes := int64(spillBlockHeaderSize + len(payload))
	seg, err := s.ensureWritableSegment(blockBytes)
	if err != nil {
		return 0, err
	}
	offset := seg.file.Size()

	if n, err := seg.file.WriteContext(ctx, rawHeader[:]); err != nil || n != len(rawHeader) {
		if err == nil {
			err = io.ErrShortWrite
		}
		s.poisoned = fmt.Errorf("write spill block header: %w", err)
		return 0, s.poisoned
	}
	if n, err := seg.file.WriteContext(ctx, payload); err != nil || n != len(payload) {
		if err == nil {
			err = io.ErrShortWrite
		}
		s.poisoned = fmt.Errorf("write spill block payload: %w", err)
		return 0, s.poisoned
	}

	s.nextPageID++
	id := s.nextPageID
	ref := PageRef{
		ID:            id,
		SegmentID:     seg.id,
		Offset:        offset,
		EncodedLength: header.payloadLen,
		RawLength:     header.rawLen,
		Checksum:      header.checksum,
		Compression:   compression,
	}
	s.pages[id] = ref
	seg.live++
	return id, nil
}

func readAtExact(file *File, dst []byte, off int64) error {
	n, err := file.ReadAt(dst, off)
	if err != nil {
		return err
	}
	if n != len(dst) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (s *SpillablePageStore) ReadPage(ctx context.Context, id PageID) (*memory.Buffer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	ref, ok := s.pages[id]
	if !ok {
		return nil, fmt.Errorf("spill page %d not found", id)
	}
	seg := s.segments[ref.SegmentID]
	if seg == nil {
		return nil, fmt.Errorf("%w: segment %d missing", ErrCorruptSpillBlock, ref.SegmentID)
	}

	var rawHeader [spillBlockHeaderSize]byte
	if err := readAtExact(seg.file, rawHeader[:], ref.Offset); err != nil {
		return nil, fmt.Errorf("read spill block header: %w", err)
	}
	header, err := decodeSpillBlockHeader(&rawHeader)
	if err != nil {
		return nil, err
	}
	if header.payloadLen != ref.EncodedLength || header.rawLen != ref.RawLength || header.checksum != ref.Checksum || header.compression != ref.Compression {
		return nil, fmt.Errorf("%w: block metadata mismatch", ErrCorruptSpillBlock)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	payload, err := s.buffers.Allocate(s.memoryScope, memory.TagSpillIO, int(header.payloadLen))
	if err != nil {
		return nil, err
	}
	if err := readAtExact(seg.file, payload.Bytes(), ref.Offset+spillBlockHeaderSize); err != nil {
		payload.Release()
		return nil, fmt.Errorf("read spill block payload: %w", err)
	}
	if err := ctx.Err(); err != nil {
		payload.Release()
		return nil, err
	}

	switch header.compression {
	case CompressionNone:
		if uint32(payload.Len()) != header.rawLen {
			payload.Release()
			return nil, fmt.Errorf("%w: uncompressed length mismatch", ErrCorruptSpillBlock)
		}
		if crc32.Checksum(payload.Bytes(), spillChecksumTable) != header.checksum {
			payload.Release()
			return nil, fmt.Errorf("%w: checksum mismatch", ErrCorruptSpillBlock)
		}
		return payload, nil
	case CompressionRLE:
		out, err := s.buffers.Allocate(s.memoryScope, memory.TagSpillIO, int(header.rawLen))
		if err != nil {
			payload.Release()
			return nil, err
		}
		if err := rleDecode(out.Bytes(), payload.Bytes()); err != nil {
			out.Release()
			payload.Release()
			return nil, err
		}
		payload.Release()
		if crc32.Checksum(out.Bytes(), spillChecksumTable) != header.checksum {
			out.Release()
			return nil, fmt.Errorf("%w: checksum mismatch", ErrCorruptSpillBlock)
		}
		return out, nil
	default:
		payload.Release()
		return nil, fmt.Errorf("%w: unknown compression %d", ErrCorruptSpillBlock, header.compression)
	}
}

func (s *SpillablePageStore) DeletePage(id PageID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	ref, ok := s.pages[id]
	if !ok {
		return nil
	}
	seg := s.segments[ref.SegmentID]
	if seg == nil {
		return fmt.Errorf("%w: segment %d missing", ErrCorruptSpillBlock, ref.SegmentID)
	}
	if seg.live <= 0 {
		return fmt.Errorf("%w: invalid segment live count", ErrCorruptSpillBlock)
	}
	if seg.live == 1 {
		if err := seg.file.Remove(); err != nil {
			return err
		}
		delete(s.segments, seg.id)
		if s.current == seg {
			s.current = nil
		}
	} else {
		seg.live--
	}
	delete(s.pages, id)
	return nil
}

func (s *SpillablePageStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	var errs []error
	for _, seg := range s.segments {
		if err := seg.file.Sync(); err != nil && !errors.Is(err, ErrClosed) {
			errs = append(errs, err)
		}
		if err := seg.file.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		return errors.Join(errs...)
	}
	s.closed = true
	return nil
}

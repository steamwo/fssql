package export

import (
	"errors"

	"github.com/golang/snappy"
	"github.com/steamwo/fssql/internal/core/memory"
)

// A workspace belongs to one row-group flush. Only ordinary page capacities
// survive a page write; no workspace competes with the next row group's vectors.
type parquetPageWorkspace struct {
	scope      *memory.Scope
	bm         *memory.BufferManager
	initial    int
	body       *parquetPageBuffer
	compressed *memory.Buffer
	frame      *memory.Buffer
}

func (p *parquetPageWorkspace) page() (*parquetPageBuffer, error) {
	if p.body == nil {
		var err error
		p.body, err = newParquetPageBuffer(p.scope, p.bm, p.initial)
		if errors.Is(err, memory.ErrLimitExceeded) {
			p.releaseEncoded()
			p.body, err = newParquetPageBuffer(p.scope, p.bm, p.initial)
		}
		if err != nil {
			return nil, err
		}
	}
	p.body.used = 0
	return p.body, nil
}

// The previous contents are dead, so release before growing instead of copying.
func (p *parquetPageWorkspace) buffer(slot **memory.Buffer, size int) ([]byte, error) {
	if *slot != nil && (*slot).Len() < size {
		(*slot).Release()
		*slot = nil
	}
	if *slot == nil {
		var err error
		*slot, err = p.bm.Allocate(p.scope, memory.TagExportBuffer, size)
		if err != nil {
			return nil, err
		}
	}
	return (*slot).Bytes()[:size], nil
}

func (p *parquetPageWorkspace) compress(payload []byte) ([]byte, error) {
	size := snappy.MaxEncodedLen(len(payload))
	if size < 0 {
		return nil, errors.New("export: parquet page exceeds Snappy block limit")
	}
	// Different NULL distributions and the last short page can need less space.
	// Keep their reservation exact so stale capacity cannot crowd out encryption.
	if p.compressed != nil && p.compressed.Len() != size {
		p.compressed.Release()
		p.compressed = nil
	}
	buf, err := p.buffer(&p.compressed, size)
	if errors.Is(err, memory.ErrLimitExceeded) {
		// The preceding page's encrypted frame is idle during compression.
		p.releaseFrame()
		buf, err = p.buffer(&p.compressed, size)
	}
	if err != nil {
		return nil, err
	}
	return snappy.Encode(buf, payload), nil
}

func (p *parquetPageWorkspace) encrypt(w *ParquetWriter, plain []byte, module int8, rowGroup, column, page int16) ([]byte, error) {
	size, err := parquetEncryptedFrameSize(len(plain))
	if err != nil {
		return nil, err
	}
	buf, err := p.buffer(&p.frame, size)
	if err != nil {
		return nil, err
	}
	return sealParquetModule(buf, w.opts.Encryption, w.fileAAD[:], module, rowGroup, column, page, plain)
}

func (p *parquetPageWorkspace) releaseFrame() {
	if p.frame != nil {
		p.frame.Release()
		p.frame = nil
	}
}

func (p *parquetPageWorkspace) releaseEncoded() {
	if p.compressed != nil {
		p.compressed.Release()
		p.compressed = nil
	}
	p.releaseFrame()
}

func (p *parquetPageWorkspace) release() {
	p.releaseEncoded()
	p.body.Release()
	p.body = nil
}

func (p *parquetPageWorkspace) trim() {
	if p.body != nil && p.body.buffer.Len() > p.initial {
		p.body.Release()
		p.body = nil
	}
	encodedLimit := snappy.MaxEncodedLen(p.initial)
	if encodedLimit < 0 {
		encodedLimit = p.initial
	}
	if p.compressed != nil && p.compressed.Len() > encodedLimit {
		p.compressed.Release()
		p.compressed = nil
	}
	const frameOverhead = parquetCryptoLengthBytes + parquetCryptoNonceBytes + parquetCryptoTagBytes
	if p.frame != nil && p.frame.Len()-frameOverhead > encodedLimit {
		p.releaseFrame()
	}
}

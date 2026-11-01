package compression

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/format/scan"
)

type Gzip struct{}

func (Gzip) Name() string { return "gzip" }
func (Gzip) Capability() scan.CompressionCapability { return scan.CompressionSequential }
func (Gzip) Decode(src []byte, s *memory.Scope, bm *memory.BufferManager) (*memory.Buffer, error) {
	if s == nil || bm == nil {
		return nil, fmt.Errorf("compression: managed memory required")
	}
	r, err := gzip.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, fmt.Errorf("%w: gzip header: %v", ErrCorrupt, err)
	}
	defer r.Close()
	b, err := bm.Allocate(s, memory.TagCompression, 64<<10)
	if err != nil {
		return nil, err
	}
	used := 0
	for {
		if used == b.Len() {
			if err := b.Resize(b.Len()*2, nil); err != nil {
				b.Release()
				return nil, err
			}
		}
		n, readErr := r.Read(b.Bytes()[used:])
		used += n
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			b.Release()
			return nil, fmt.Errorf("%w: gzip decode: %v", ErrCorrupt, readErr)
		}
	}
	if err := r.Close(); err != nil {
		b.Release()
		return nil, fmt.Errorf("%w: gzip trailer: %v", ErrCorrupt, err)
	}
	if err := b.Resize(used, nil); err != nil {
		b.Release()
		return nil, err
	}
	return b, nil
}

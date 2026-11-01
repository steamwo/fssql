package compression

import (
	"errors"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/format/scan"
)

var ErrCorrupt = errors.New("compression: corrupt stream")
var ErrUnknownSize = errors.New("compression: zstd frame content size unknown")
var ErrUnavailable = errors.New("compression: codec unavailable")

type Codec interface {
	Name() string
	Capability() scan.CompressionCapability
	Decode([]byte, *memory.Scope, *memory.BufferManager) (*memory.Buffer, error)
}

type Identity struct{}

func (Identity) Name() string { return "identity" }
func (Identity) Capability() scan.CompressionCapability { return scan.CompressionIdentity }
func (Identity) Decode(src []byte, s *memory.Scope, bm *memory.BufferManager) (*memory.Buffer, error) {
	if s == nil || bm == nil {
		return nil, errors.New("compression: managed memory required")
	}
	b, err := bm.Allocate(s, memory.TagCompression, len(src))
	if err != nil {
		return nil, err
	}
	copy(b.Bytes(), src)
	return b, nil
}

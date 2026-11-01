//go:build !linux || !cgo

package compression

import (
	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/format/scan"
)

type Zstd struct{}

func (Zstd) Name() string { return "zstd" }
func (Zstd) Capability() scan.CompressionCapability { return scan.CompressionSequential }
func ZstdAvailable() bool { return false }
func (Zstd) Decode([]byte, *memory.Scope, *memory.BufferManager) (*memory.Buffer, error) {
	return nil, ErrUnavailable
}

//go:build linux && cgo

package compression

/*
#cgo LDFLAGS: -l:libzstd.so.1
#include <stdint.h>
#include <stddef.h>
unsigned long long ZSTD_getFrameContentSize(const void* src, size_t srcSize);
size_t ZSTD_decompress(void* dst, size_t dstCapacity, const void* src, size_t compressedSize);
unsigned ZSTD_isError(size_t code);
const char* ZSTD_getErrorName(size_t code);
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/format/scan"
)

type Zstd struct{}

func (Zstd) Name() string { return "zstd" }
func (Zstd) Capability() scan.CompressionCapability { return scan.CompressionSequential }
func ZstdAvailable() bool { return true }
func (Zstd) Decode(src []byte, s *memory.Scope, bm *memory.BufferManager) (*memory.Buffer, error) {
	if s == nil || bm == nil {
		return nil, fmt.Errorf("compression: managed memory required")
	}
	if len(src) == 0 {
		return nil, fmt.Errorf("%w: empty zstd stream", ErrCorrupt)
	}
	sz := uint64(C.ZSTD_getFrameContentSize(unsafe.Pointer(&src[0]), C.size_t(len(src))))
	if sz == ^uint64(0) {
		return nil, ErrUnknownSize
	}
	if sz == ^uint64(1) {
		return nil, fmt.Errorf("%w: invalid zstd frame", ErrCorrupt)
	}
	if sz > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("compression: zstd output too large: %d", sz)
	}
	b, err := bm.Allocate(s, memory.TagCompression, int(sz))
	if err != nil {
		return nil, err
	}
	var dst unsafe.Pointer
	if sz > 0 {
		dst = unsafe.Pointer(&b.Bytes()[0])
	}
	ret := C.ZSTD_decompress(dst, C.size_t(sz), unsafe.Pointer(&src[0]), C.size_t(len(src)))
	if C.ZSTD_isError(ret) != 0 {
		b.Release()
		return nil, fmt.Errorf("%w: zstd decode: %s", ErrCorrupt, C.GoString(C.ZSTD_getErrorName(ret)))
	}
	if uint64(ret) != sz {
		b.Release()
		return nil, fmt.Errorf("%w: zstd size got=%d want=%d", ErrCorrupt, uint64(ret), sz)
	}
	return b, nil
}

package scan

import "github.com/steamwo/fssql/internal/storage/vfs"

// ScanMode is the effective I/O mode after composing VFS, compression, and
// format capabilities. The zero value is deliberately unavailable so callers
// must opt into a readable path explicitly.
type ScanMode uint8

const (
	ScanUnavailable ScanMode = iota
	ScanSequential
	ScanParallelRange
)

// CompressionCapability describes only the scan-relevant property of a
// compression layer. Identity means there is no compression layer and is
// neutral for parallelism. Sequential represents codecs such as ordinary gzip
// that cannot be split safely. Splittable is reserved for codecs/container
// layouts that can independently decode ranges.
type CompressionCapability uint8

const (
	CompressionIdentity CompressionCapability = iota
	CompressionSequential
	CompressionSplittable
)

// FormatCapability describes whether a format can establish independent scan
// boundaries. ParallelBoundary requires a format-specific boundary recognizer;
// it does not by itself authorize range I/O.
type FormatCapability uint8

const (
	FormatSequential FormatCapability = iota
	FormatParallelBoundary
)

// CapabilityBlocker identifies the first layer that prevents a more capable
// scan mode. It is diagnostic/planning metadata; it must not trigger an
// implicit whole-file fallback.
type CapabilityBlocker uint8

const (
	BlockerNone CapabilityBlocker = iota
	BlockerUnreadableVFS
	BlockerVFSParallelRange
	BlockerCompression
	BlockerFormat
)

// CapabilityDecision is the composed result of:
//
//	VFS capability x Compression capability x Format capability.
//
// A sequential decision means the planner must choose an explicit sequential
// path. It never means "issue a range request and silently download the whole
// object".
type CapabilityDecision struct {
	Mode    ScanMode
	Blocker CapabilityBlocker
}

// ResolveScanCapability composes storage, compression, and format capabilities
// into one effective scan decision.
func ResolveScanCapability(
	vfsCaps vfs.CapabilitySet,
	compression CompressionCapability,
	format FormatCapability,
) CapabilityDecision {
	readable := vfsCaps.Has(vfs.RandomRead) || vfsCaps.Has(vfs.StreamingRead)
	if !readable {
		return CapabilityDecision{Mode: ScanUnavailable, Blocker: BlockerUnreadableVFS}
	}

	if !vfsCaps.Has(vfs.RandomRead) || !vfsCaps.Has(vfs.ParallelRandomRead) {
		return CapabilityDecision{Mode: ScanSequential, Blocker: BlockerVFSParallelRange}
	}
	if compression == CompressionSequential {
		return CapabilityDecision{Mode: ScanSequential, Blocker: BlockerCompression}
	}
	if format != FormatParallelBoundary {
		return CapabilityDecision{Mode: ScanSequential, Blocker: BlockerFormat}
	}
	return CapabilityDecision{Mode: ScanParallelRange, Blocker: BlockerNone}
}

package scan

import (
	"testing"

	"github.com/steamwo/fssql/internal/storage/vfs"
)

func TestResolveScanCapability(t *testing.T) {
	parallelVFS := vfs.Capabilities(vfs.RandomRead, vfs.ParallelRandomRead)

	tests := []struct {
		name        string
		vfsCaps     vfs.CapabilitySet
		compression CompressionCapability
		format      FormatCapability
		want        CapabilityDecision
	}{
		{
			name:        "identity compression and boundary-aware format permit parallel range",
			vfsCaps:     parallelVFS,
			compression: CompressionIdentity,
			format:      FormatParallelBoundary,
			want:        CapabilityDecision{Mode: ScanParallelRange},
		},
		{
			name:        "splittable compression permits parallel range",
			vfsCaps:     parallelVFS,
			compression: CompressionSplittable,
			format:      FormatParallelBoundary,
			want:        CapabilityDecision{Mode: ScanParallelRange},
		},
		{
			name:        "ordinary sequential compression forces sequential scan",
			vfsCaps:     parallelVFS,
			compression: CompressionSequential,
			format:      FormatParallelBoundary,
			want:        CapabilityDecision{Mode: ScanSequential, Blocker: BlockerCompression},
		},
		{
			name:        "sequential format forces sequential scan",
			vfsCaps:     parallelVFS,
			compression: CompressionIdentity,
			format:      FormatSequential,
			want:        CapabilityDecision{Mode: ScanSequential, Blocker: BlockerFormat},
		},
		{
			name:        "streaming-only VFS remains readable but sequential",
			vfsCaps:     vfs.Capabilities(vfs.StreamingRead),
			compression: CompressionIdentity,
			format:      FormatParallelBoundary,
			want:        CapabilityDecision{Mode: ScanSequential, Blocker: BlockerVFSParallelRange},
		},
		{
			name:        "random read without parallel random read remains sequential",
			vfsCaps:     vfs.Capabilities(vfs.RandomRead),
			compression: CompressionIdentity,
			format:      FormatParallelBoundary,
			want:        CapabilityDecision{Mode: ScanSequential, Blocker: BlockerVFSParallelRange},
		},
		{
			name:        "parallel bit without random read is not enough",
			vfsCaps:     vfs.Capabilities(vfs.ParallelRandomRead, vfs.StreamingRead),
			compression: CompressionIdentity,
			format:      FormatParallelBoundary,
			want:        CapabilityDecision{Mode: ScanSequential, Blocker: BlockerVFSParallelRange},
		},
		{
			name:        "VFS with no read capability is unavailable",
			vfsCaps:     vfs.Capabilities(vfs.ServerSideList),
			compression: CompressionIdentity,
			format:      FormatParallelBoundary,
			want:        CapabilityDecision{Mode: ScanUnavailable, Blocker: BlockerUnreadableVFS},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ResolveScanCapability(tt.vfsCaps, tt.compression, tt.format); got != tt.want {
				t.Fatalf("ResolveScanCapability() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestSequentialCompressionWinsBeforeFormatBlocker(t *testing.T) {
	got := ResolveScanCapability(
		vfs.Capabilities(vfs.RandomRead, vfs.ParallelRandomRead),
		CompressionSequential,
		FormatSequential,
	)
	want := CapabilityDecision{Mode: ScanSequential, Blocker: BlockerCompression}
	if got != want {
		t.Fatalf("ResolveScanCapability() = %+v, want %+v", got, want)
	}
}

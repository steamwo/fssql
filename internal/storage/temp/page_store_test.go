package temp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
)

func newPageStoreHarness(t testing.TB, memoryLimit int64, config PageStoreConfig) (*TemporaryMemoryManager, *QueryScope, *memory.Manager, *memory.Scope, *SpillablePageStore) {
	t.Helper()
	quotas := NewTemporaryMemoryManager(Limits{Global: -1, DefaultResourceGroup: -1, DefaultQuery: -1})
	storage, err := NewTemporaryStorageManager(t.TempDir(), quotas)
	if err != nil {
		t.Fatal(err)
	}
	tempScope, err := storage.NewQueryScope("default")
	if err != nil {
		t.Fatal(err)
	}
	memoryManager := memory.NewManager(memoryLimit)
	memoryScope := memoryManager.NewScope()
	store, err := NewSpillablePageStore(tempScope, memoryScope, memory.NewBufferManager(nil), config)
	if err != nil {
		t.Fatal(err)
	}
	return quotas, tempScope, memoryManager, memoryScope, store
}

func TestSpillBlockHeaderRoundTrip(t *testing.T) {
	want := spillBlockHeader{compression: CompressionRLE, payloadLen: 123, rawLen: 456, checksum: 0xdeadbeef}
	var encoded [spillBlockHeaderSize]byte
	encodeSpillBlockHeader(&encoded, want)
	got, err := decodeSpillBlockHeader(&encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("header=%+v, want %+v", got, want)
	}
}

func TestSpillablePageStoreRoundTripAndReclaim(t *testing.T) {
	quotas, tempScope, memoryManager, memoryScope, store := newPageStoreHarness(t, 1<<20, PageStoreConfig{SegmentMaxBytes: 1 << 20})
	pages := [][]byte{
		[]byte("alpha"),
		bytes.Repeat([]byte{0x42}, 128),
		[]byte("omega"),
	}
	ids := make([]PageID, 0, len(pages))
	for _, page := range pages {
		id, err := store.WritePage(context.Background(), page)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if store.PageCount() != len(pages) {
		t.Fatalf("page count=%d, want %d", store.PageCount(), len(pages))
	}
	if tempScope.Used() == 0 {
		t.Fatal("expected spill bytes to be owned")
	}

	for i, id := range ids {
		buf, err := store.ReadPage(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf.Bytes(), pages[i]) {
			t.Fatalf("page %d mismatch", id)
		}
		buf.Release()
	}
	if got := memoryManager.Used(); got != 0 {
		t.Fatalf("managed memory leak after reads: %d", got)
	}

	for _, id := range ids {
		if err := store.DeletePage(id); err != nil {
			t.Fatal(err)
		}
	}
	if got := tempScope.Used(); got != 0 {
		t.Fatalf("temp bytes not reclaimed after final page delete: %d", got)
	}
	if got := tempScope.OwnedFiles(); got != 0 {
		t.Fatalf("temp files not reclaimed: %d", got)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tempScope.Close(); err != nil {
		t.Fatal(err)
	}
	if err := memoryScope.Close(); err != nil {
		t.Fatal(err)
	}
	if got := quotas.GlobalUsed(); got != 0 {
		t.Fatalf("global temp quota leak: %d", got)
	}
}

func TestSpillablePageStoreRLECompression(t *testing.T) {
	_, tempScope, memoryManager, memoryScope, store := newPageStoreHarness(t, 1<<20, PageStoreConfig{
		Compression:      CompressionRLE,
		MinCompressBytes: 1,
	})
	data := bytes.Repeat([]byte{'A'}, 4096)
	id, err := store.WritePage(context.Background(), data)
	if err != nil {
		t.Fatal(err)
	}
	ref, ok := store.Ref(id)
	if !ok {
		t.Fatal("missing page ref")
	}
	if ref.Compression != CompressionRLE {
		t.Fatalf("compression=%d, want RLE", ref.Compression)
	}
	if int64(ref.EncodedLength)+spillBlockHeaderSize >= int64(len(data)) {
		t.Fatalf("compressed spill block did not shrink: encoded=%d raw=%d", ref.EncodedLength, len(data))
	}
	if got := memoryManager.Used(); got != 0 {
		t.Fatalf("compression scratch leak after write: %d", got)
	}
	buf, err := store.ReadPage(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatal("RLE round trip mismatch")
	}
	buf.Release()
	if got := memoryManager.Used(); got != 0 {
		t.Fatalf("read buffer leak: %d", got)
	}
	if err := store.DeletePage(id); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tempScope.Close(); err != nil {
		t.Fatal(err)
	}
	if err := memoryScope.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSpillablePageStoreReadsRotatedSegments(t *testing.T) {
	_, tempScope, _, memoryScope, store := newPageStoreHarness(t, 1<<20, PageStoreConfig{SegmentMaxBytes: 40})
	first := bytes.Repeat([]byte{'x'}, 16)
	second := bytes.Repeat([]byte{'y'}, 16)
	id1, err := store.WritePage(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := store.WritePage(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	ref1, _ := store.Ref(id1)
	ref2, _ := store.Ref(id2)
	if ref1.SegmentID == ref2.SegmentID {
		t.Fatal("expected segment rotation")
	}
	buf, err := store.ReadPage(context.Background(), id1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), first) {
		t.Fatal("rotated segment read mismatch")
	}
	buf.Release()
	if err := store.DeletePage(id1); err != nil {
		t.Fatal(err)
	}
	if err := store.DeletePage(id2); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tempScope.Close(); err != nil {
		t.Fatal(err)
	}
	if err := memoryScope.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSpillablePageStoreDetectsCorruption(t *testing.T) {
	_, tempScope, memoryManager, memoryScope, store := newPageStoreHarness(t, 1<<20, PageStoreConfig{})
	data := []byte("checksum protected spill payload")
	id, err := store.WritePage(context.Background(), data)
	if err != nil {
		t.Fatal(err)
	}
	ref, _ := store.Ref(id)
	seg := store.segments[ref.SegmentID]
	path := seg.file.Name()
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xff}, ref.Offset+spillBlockHeaderSize+2); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	buf, err := store.ReadPage(context.Background(), id)
	if buf != nil {
		buf.Release()
		t.Fatal("unexpected buffer from corrupt block")
	}
	if !errors.Is(err, ErrCorruptSpillBlock) {
		t.Fatalf("read error=%v, want corrupt spill block", err)
	}
	if got := memoryManager.Used(); got != 0 {
		t.Fatalf("managed memory leak after corrupt read: %d", got)
	}
	if err := store.DeletePage(id); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tempScope.Close(); err != nil {
		t.Fatal(err)
	}
	if err := memoryScope.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSpillablePageStoreReadAllocationFailureHasNoLeak(t *testing.T) {
	_, tempScope, memoryManager, memoryScope, store := newPageStoreHarness(t, 4, PageStoreConfig{})
	id, err := store.WritePage(context.Background(), []byte("12345678"))
	if err != nil {
		t.Fatal(err)
	}
	buf, err := store.ReadPage(context.Background(), id)
	if buf != nil {
		buf.Release()
		t.Fatal("unexpected buffer")
	}
	if !errors.Is(err, memory.ErrLimitExceeded) {
		t.Fatalf("read error=%v, want memory limit", err)
	}
	if got := memoryManager.Used(); got != 0 {
		t.Fatalf("managed memory leak after allocation failure: %d", got)
	}
	if err := store.DeletePage(id); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tempScope.Close(); err != nil {
		t.Fatal(err)
	}
	if err := memoryScope.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSpillablePageStoreShortWritePoisonsStoreAndCleansUp(t *testing.T) {
	quotas := NewTemporaryMemoryManager(Limits{Global: 1024, DefaultResourceGroup: 1024, DefaultQuery: 1024})
	storage, err := newTemporaryStorageManager(t.TempDir(), quotas, faultBackend{writeMode: "short"})
	if err != nil {
		t.Fatal(err)
	}
	tempScope, err := storage.NewQueryScope("default")
	if err != nil {
		t.Fatal(err)
	}
	mm := memory.NewManager(1024)
	ms := mm.NewScope()
	store, err := NewSpillablePageStore(tempScope, ms, memory.NewBufferManager(nil), PageStoreConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.WritePage(context.Background(), []byte("abcdef")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("write error=%v, want short write", err)
	}
	if store.PageCount() != 0 {
		t.Fatalf("partial block became visible as page")
	}
	if tempScope.Used() == 0 {
		t.Fatal("partial physical write should remain owned until cleanup")
	}
	if _, err := store.WritePage(context.Background(), []byte("again")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("poisoned store error=%v, want original short write", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tempScope.Close(); err != nil {
		t.Fatal(err)
	}
	if got := quotas.GlobalUsed(); got != 0 {
		t.Fatalf("temp quota leak after cleanup: %d", got)
	}
	if err := ms.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSpillablePageStoreCanceledReadDoesNotAllocate(t *testing.T) {
	_, tempScope, memoryManager, memoryScope, store := newPageStoreHarness(t, 1024, PageStoreConfig{})
	id, err := store.WritePage(context.Background(), []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.ReadPage(ctx, id); !errors.Is(err, context.Canceled) {
		t.Fatalf("read error=%v, want canceled", err)
	}
	if got := memoryManager.Used(); got != 0 {
		t.Fatalf("managed memory leak: %d", got)
	}
	if err := store.DeletePage(id); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tempScope.Close(); err != nil {
		t.Fatal(err)
	}
	if err := memoryScope.Close(); err != nil {
		t.Fatal(err)
	}
}

func FuzzRLECodec(f *testing.F) {
	f.Add([]byte("aaaaabbbbcccccccc"))
	f.Add([]byte("abcdef"))
	f.Fuzz(func(t *testing.T, src []byte) {
		if len(src) > 1<<20 {
			t.Skip()
		}
		dst := make([]byte, len(src))
		n, ok := rleEncode(dst, src)
		if !ok {
			return
		}
		decoded := make([]byte, len(src))
		if err := rleDecode(decoded, dst[:n]); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(decoded, src) {
			t.Fatal("RLE round trip mismatch")
		}
	})
}

func BenchmarkRLEEncode(b *testing.B) {
	src := bytes.Repeat([]byte{'x'}, 64<<10)
	dst := make([]byte, len(src))
	b.SetBytes(int64(len(src)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := rleEncode(dst, src); !ok {
			b.Fatal("expected compressible input")
		}
	}
}

func BenchmarkSpillPageStoreRoundTrip(b *testing.B) {
	_, tempScope, _, memoryScope, store := newPageStoreHarness(b, 8<<20, PageStoreConfig{SegmentMaxBytes: 8 << 20})
	data := bytes.Repeat([]byte{0x5a}, 64<<10)
	ctx := context.Background()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id, err := store.WritePage(ctx, data)
		if err != nil {
			b.Fatal(err)
		}
		buf, err := store.ReadPage(ctx, id)
		if err != nil {
			b.Fatal(err)
		}
		buf.Release()
		if err := store.DeletePage(id); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := store.Close(); err != nil {
		b.Fatal(err)
	}
	if err := tempScope.Close(); err != nil {
		b.Fatal(err)
	}
	if err := memoryScope.Close(); err != nil {
		b.Fatal(err)
	}
}

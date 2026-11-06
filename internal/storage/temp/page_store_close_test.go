package temp

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
)

func TestSpillablePageStoreCloseReclaimsUnconsumedPages(t *testing.T) {
	quotas, tempScope, memoryManager, memoryScope, store := newPageStoreHarness(t, 1<<20, PageStoreConfig{SegmentMaxBytes: 1 << 20})
	if _, err := store.WritePage(context.Background(), bytes.Repeat([]byte("x"), 4096)); err != nil { t.Fatal(err) }
	if tempScope.Used() == 0 || tempScope.OwnedFiles() == 0 { t.Fatalf("expected owned spill before close used=%d files=%d", tempScope.Used(), tempScope.OwnedFiles()) }
	if err := store.Close(); err != nil { t.Fatal(err) }
	if got := tempScope.Used(); got != 0 { t.Fatalf("store close left temp quota=%d", got) }
	if got := tempScope.OwnedFiles(); got != 0 { t.Fatalf("store close left files=%d", got) }
	if got := memoryManager.Used(); got != 0 { t.Fatalf("store close left managed metadata=%d", got) }
	if err := tempScope.Close(); err != nil { t.Fatal(err) }
	if err := memoryScope.Close(); err != nil { t.Fatal(err) }
	if got := quotas.GlobalUsed(); got != 0 { t.Fatalf("global temp quota leak=%d", got) }
	_ = memory.TagSpillIO
}

func TestSpillablePageStoreCloseReclaimsMetadataAfterRemoveFailure(t *testing.T) {
	backend := &mutableFaultBackend{Backend: osBackend{}, removeErr: fs.ErrPermission}
	quotas := NewTemporaryMemoryManager(Limits{Global: -1, DefaultResourceGroup: -1, DefaultQuery: -1})
	storage, err := newTemporaryStorageManager(t.TempDir(), quotas, backend)
	if err != nil {
		t.Fatal(err)
	}
	tempScope, err := storage.NewQueryScope("default")
	if err != nil {
		t.Fatal(err)
	}
	memoryManager := memory.NewManager(1 << 20)
	memoryScope := memoryManager.NewScope()
	store, err := NewSpillablePageStore(tempScope, memoryScope, memory.NewBufferManager(nil), PageStoreConfig{SegmentMaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1025; i++ {
		if _, err := store.WritePage(context.Background(), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	const metadataBytes = 2048*pageDirectoryEntryWidth + segmentMetadataBytes
	if got := memoryManager.Used(); got != metadataBytes {
		t.Fatalf("metadata before close=%d, want %d", got, metadataBytes)
	}
	if err := store.Close(); !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("close error=%v, want permission denied", err)
	}
	if got := memoryManager.Used(); got != 0 {
		t.Fatalf("store close left managed metadata=%d", got)
	}
	if tempScope.Used() == 0 || tempScope.OwnedFiles() != 1 {
		t.Fatalf("failed file removal lost retry ownership used=%d files=%d", tempScope.Used(), tempScope.OwnedFiles())
	}
	if err := tempScope.Close(); !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("query cleanup error=%v, want permission denied", err)
	}
	backend.removeErr = nil
	if err := tempScope.Close(); err != nil {
		t.Fatal(err)
	}
	if err := memoryScope.Close(); err != nil {
		t.Fatal(err)
	}
	if got := quotas.GlobalUsed(); got != 0 {
		t.Fatalf("global temp quota leak=%d", got)
	}
}

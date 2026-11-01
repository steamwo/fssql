package temp

import (
	"bytes"
	"context"
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

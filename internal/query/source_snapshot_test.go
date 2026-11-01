package query

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
)

func TestSourceSnapshotIsImmutableWithinQuery(t *testing.T) {
	q := New(context.Background(), memory.NewManager(1024))
	if err := q.BindSourceSnapshot(SourceSnapshot{Fingerprint: "snapshot-a"}); err != nil {
		t.Fatal(err)
	}
	if err := q.BindSourceSnapshot(SourceSnapshot{Fingerprint: "snapshot-a"}); err != nil {
		t.Fatalf("idempotent bind failed: %v", err)
	}
	if err := q.BindSourceSnapshot(SourceSnapshot{Fingerprint: "snapshot-b"}); !errors.Is(err, ErrSourceSnapshotConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	got, ok := q.SourceSnapshot()
	if !ok || got.Fingerprint != "snapshot-a" {
		t.Fatalf("unexpected snapshot: %#v %v", got, ok)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

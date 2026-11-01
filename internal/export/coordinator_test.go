package export

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/query"
)

func TestCoordinatorValidatesFrozenSourceSnapshot(t *testing.T) {
	q := query.New(context.Background(), memory.NewManager(1024))
	if err := q.BindSourceSnapshot(query.SourceSnapshot{Fingerprint: "snapshot-1"}); err != nil {
		t.Fatal(err)
	}
	m := newTestManifest(t)
	c, err := NewCoordinator(q, m)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if m.ExportState != ExportStateWriting {
		t.Fatalf("expected WRITING, got %s", m.ExportState)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorRejectsMissingOrChangedSourceSnapshot(t *testing.T) {
	q := query.New(context.Background(), memory.NewManager(1024))
	m := newTestManifest(t)
	c, _ := NewCoordinator(q, m)
	if err := c.Start(); !errors.Is(err, ErrSourceSnapshotMismatch) {
		t.Fatalf("expected missing snapshot mismatch, got %v", err)
	}
	if m.ExportState != ExportStatePlanned {
		t.Fatalf("state changed after validation failure: %s", m.ExportState)
	}
	if err := q.BindSourceSnapshot(query.SourceSnapshot{Fingerprint: "other"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); !errors.Is(err, ErrSourceSnapshotMismatch) {
		t.Fatalf("expected changed snapshot mismatch, got %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

package export

import (
	"errors"
	"strings"
	"testing"
)

func newTestManifest(t *testing.T) *Manifest {
	t.Helper()
	m, err := NewManifest("job-1", "query-1", "snapshot-1", "s3://bucket/out", "parquet", "zstd", "statement_snapshot", []string{"part-000.parquet", "part-001.parquet"})
	if err != nil {
		t.Fatalf("NewManifest: %v", err)
	}
	return m
}

func TestManifestRoundTrip(t *testing.T) {
	m := newTestManifest(t)
	data, err := m.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	got, err := UnmarshalManifest(data)
	if err != nil {
		t.Fatalf("UnmarshalManifest: %v", err)
	}
	if got.JobID != m.JobID || got.SourceSnapshot.Fingerprint != m.SourceSnapshot.Fingerprint || len(got.FileStates) != 2 {
		t.Fatalf("round trip mismatch: %#v", got)
	}
}

func TestManifestRejectsIllegalTransitions(t *testing.T) {
	m := newTestManifest(t)
	if err := m.TransitionExport(ExportStateCommitting); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected invalid export transition, got %v", err)
	}
	if err := m.TransitionFile("part-000.parquet", FileStateComplete, 10, "x"); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected invalid file transition, got %v", err)
	}
}

func TestCompletedFileIsImmutableAndIncompleteCanRedo(t *testing.T) {
	m := newTestManifest(t)
	if err := m.TransitionFile("part-000.parquet", FileStateWriting, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := m.TransitionFile("part-000.parquet", FileStateComplete, 42, "sum"); err != nil {
		t.Fatal(err)
	}
	if err := m.TransitionFile("part-000.parquet", FileStateWriting, 0, ""); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("completed file should be immutable, got %v", err)
	}

	if err := m.TransitionFile("part-001.parquet", FileStateWriting, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := m.TransitionFile("part-001.parquet", FileStateFailed, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := m.TransitionFile("part-001.parquet", FileStateWriting, 0, ""); err != nil {
		t.Fatalf("incomplete file should be redoable: %v", err)
	}
}

func TestCommitRequiresAllFilesComplete(t *testing.T) {
	m := newTestManifest(t)
	if err := m.TransitionExport(ExportStateWriting); err != nil {
		t.Fatal(err)
	}
	if err := m.TransitionExport(ExportStateCommitting); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("commit should require all files complete, got %v", err)
	}
	for _, name := range []string{"part-000.parquet", "part-001.parquet"} {
		if err := m.TransitionFile(name, FileStateWriting, 0, ""); err != nil {
			t.Fatal(err)
		}
		if err := m.TransitionFile(name, FileStateComplete, 1, "sum"); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.TransitionExport(ExportStateCommitting); err != nil {
		t.Fatalf("TransitionExport committing: %v", err)
	}
	if err := m.TransitionExport(ExportStateCommitted); err != nil {
		t.Fatalf("TransitionExport committed: %v", err)
	}
}

func TestManifestValidationRejectsCorruption(t *testing.T) {
	m := newTestManifest(t)
	m.ManifestVersion = 99
	if err := m.Validate(); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("expected version error, got %v", err)
	}

	m = newTestManifest(t)
	m.FileStates[1].Path = m.FileStates[0].Path
	if err := m.Validate(); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("expected duplicate path error, got %v", err)
	}

	_, err := UnmarshalManifest([]byte(`{"manifest_version":1,"job_id":"job","query_fingerprint":"q","source_snapshot":{"fingerprint":"s"},"destination":{"uri":"s3://b/o"},"format":"csv","consistency":"snapshot","file_states":[{"path":"x","state":"BROKEN"}],"export_state":"PLANNED"}`))
	if !errors.Is(err, ErrInvalidManifest) || !strings.Contains(err.Error(), "unknown state") {
		t.Fatalf("expected corrupted state rejection, got %v", err)
	}
}

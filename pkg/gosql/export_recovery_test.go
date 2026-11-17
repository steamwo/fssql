package gosql

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	exportpkg "github.com/steamwo/fssql/internal/export"
	"github.com/steamwo/fssql/internal/storage/local"
)

func TestExportRecoveryPublicCopyAndRestart(t *testing.T) {
	for _, format := range []CopyFormat{CopyFormatCSV, CopyFormatJSON, CopyFormatNDJSON, CopyFormatParquet} {
		for _, mode := range []CopyPublishMode{CopyPublishStrict, CopyPublishCompatible} {
			t.Run(string(format)+"/"+string(mode), func(t *testing.T) {
				root, dir := t.TempDir(), t.TempDir()
				cfg := Config{LocalFiles: &LocalFileConfig{RootDir: root}, ManifestDir: dir, TempDir: t.TempDir()}
				db, err := Open(cfg)
				if err != nil {
					t.Fatal(err)
				}
				dest, _ := FileURI(filepath.Join(root, "result"))
				options := CopyOptions{Format: format, PublishMode: mode}
				if format != CopyFormatParquet {
					options.Compression = "gzip"
				}
				result, err := db.CopyTo(context.Background(), "SELECT 42 AS answer", dest, options)
				if err != nil {
					t.Fatal(err)
				}
				if result.Rows != 1 || len(result.JobID) != 32 {
					t.Fatalf("result=%+v", result)
				}
				data, err := os.ReadFile(filepath.Join(root, "result"))
				if err != nil {
					t.Fatal(err)
				}
				sum := sha256.Sum256(data)
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db, err = Open(cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				for i := 0; i < 2; i++ {
					recovered, err := db.RecoverExport(context.Background(), result.JobID)
					if err != nil {
						t.Fatal(err)
					}
					if recovered.JobID != result.JobID || recovered.Destination != dest || recovered.PublishMode != mode || recovered.Bytes != int64(len(data)) || recovered.Checksum != hex.EncodeToString(sum[:]) {
						t.Fatalf("recovered=%+v", recovered)
					}
				}
				_, err = db.CopyTo(context.Background(), "SELECT 42 AS answer", dest, options)
				if !errors.Is(err, ErrCommitConflict) {
					t.Fatalf("same-content first publish must conflict: %v", err)
				}
				var job *ExportJobError
				if !errors.As(err, &job) || job.JobID == result.JobID || len(job.JobID) != 32 {
					t.Fatalf("new invocation reused JobID: %v", err)
				}
				before := result.Bytes
				if err := os.Remove(filepath.Join(root, "result")); err != nil {
					t.Fatal(err)
				}
				if _, err := db.RecoverExport(context.Background(), result.JobID); !errors.Is(err, ErrExportNotRecoverable) {
					t.Fatal(err)
				}
				if before == 0 {
					t.Fatal("nonempty format output expected")
				}
			})
		}
	}
}

func TestExportRecoverySQLPublishModeAndValidation(t *testing.T) {
	root, dir := t.TempDir(), t.TempDir()
	db, err := Open(Config{LocalFiles: &LocalFileConfig{RootDir: root}, ManifestDir: dir, TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dest, _ := FileURI(filepath.Join(root, "sql.csv"))
	if err := db.Exec(context.Background(), fmt.Sprintf("COPY (SELECT 9 AS n) TO '%s' (FORMAT CSV, PUBLISH_MODE 'compatible')", dest)); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	if len(files) != 1 {
		t.Fatal(files)
	}
	job := strings.TrimSuffix(filepath.Base(files[0]), ".json")
	r, err := db.RecoverExport(context.Background(), job)
	if err != nil || r.PublishMode != CopyPublishCompatible {
		t.Fatalf("result=%+v err=%v", r, err)
	}
	for _, id := range []string{"", "../" + job, job + "/x", strings.ToUpper(job), "%2e%2e", strings.Repeat("a", 33), strings.Repeat("g", 32)} {
		if _, err := db.RecoverExport(context.Background(), id); err == nil {
			t.Fatalf("accepted %q", id)
		}
	}
	if _, err := db.CopyTo(context.Background(), "SELECT 1", dest, CopyOptions{Format: CopyFormatCSV, PublishMode: "overwrite"}); err == nil {
		t.Fatal("invalid mode accepted")
	}
	if err := db.Exec(context.Background(), fmt.Sprintf("COPY (SELECT 1) TO '%s' (PUBLISH_MODE 'overwrite')", dest)); err == nil {
		t.Fatal("invalid SQL mode accepted")
	}
	if _, err := db.RecoverExport(context.Background(), strings.Repeat("a", 64)); err == nil {
		t.Fatal("nonexistent v1 job accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.RecoverExport(ctx, job); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RecoverExport(context.Background(), job); err == nil {
		t.Fatal("closed DB accepted")
	}
}

func TestExportRecoveryV1Compatibility(t *testing.T) {
	root, dir := t.TempDir(), t.TempDir()
	db, err := Open(Config{LocalFiles: &LocalFileConfig{RootDir: root}, ManifestDir: dir, TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	path := filepath.Join(root, "legacy.csv")
	data := []byte("id\n1\n")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	u, _ := local.URIFromPath(path)
	driver, _ := db.exportRegistry.DriverFor(u)
	info, err := driver.Stat(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	rootURI, _ := FileURI(root)
	job := strings.Repeat("b", 64)
	m, err := exportpkg.NewManifest(job, "query", "source", rootURI, "csv", "identity", "atomic_publish", []string{"legacy.csv"})
	if err != nil {
		t.Fatal(err)
	}
	_ = m.TransitionExport(exportpkg.ExportStateWriting)
	_ = m.TransitionFile("legacy.csv", exportpkg.FileStateWriting, 0, "")
	sum := sha256.Sum256(data)
	_ = m.TransitionFilePublished("legacy.csv", int64(len(data)), hex.EncodeToString(sum[:]), exportpkg.PublishedObjectIdentity{ETag: info.Identity.ETag, Size: info.Size})
	_ = m.TransitionExport(exportpkg.ExportStateCommitting)
	for _, state := range []exportpkg.ExportState{exportpkg.ExportStateCommitting, exportpkg.ExportStateCommitted} {
		m.ExportState = state
		encoded, err := m.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, job+".json"), encoded, 0600); err != nil {
			t.Fatal(err)
		}
		r, err := db.RecoverExport(context.Background(), job)
		if err != nil || r.JobID != job || r.Bytes != int64(len(data)) {
			t.Fatalf("v1 result=%+v err=%v", r, err)
		}
		loaded, _ := os.ReadFile(filepath.Join(dir, job+".json"))
		var decoded map[string]any
		_ = json.Unmarshal(loaded, &decoded)
		if decoded["manifest_version"] != float64(1) || decoded["export_state"] != string(state) {
			t.Fatal("v1 was rewritten/upgraded")
		}
	}
	m.JobID = strings.Repeat("c", 64)
	encoded, _ := m.MarshalBinary()
	_ = os.WriteFile(filepath.Join(dir, job+".json"), encoded, 0600)
	if _, err := db.RecoverExport(context.Background(), job); err == nil {
		t.Fatal("internal JobID mismatch accepted")
	}
	_ = os.Remove(path)
	m.JobID = job
	encoded, _ = m.MarshalBinary()
	_ = os.WriteFile(filepath.Join(dir, job+".json"), encoded, 0600)
	if _, err := db.RecoverExport(context.Background(), job); !errors.Is(err, ErrExportNotRecoverable) {
		t.Fatal(err)
	}
}

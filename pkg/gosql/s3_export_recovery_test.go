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
	"github.com/steamwo/fssql/internal/storage/remote/s3/s3test"
)

func s3ExportConfig(t *testing.T, server *s3test.Server) Config {
	t.Helper()
	return Config{S3: &S3Config{Endpoint: server.URL(), Region: "test", ForcePathStyle: true, AllowInsecureHTTP: true, AccessKey: "access", SecretKey: "secret", MaxRetries: 1}, ManifestDir: t.TempDir(), TempDir: t.TempDir()}
}

func TestS3CopyFourFormatsAndRecovery(t *testing.T) {
	for _, format := range []CopyFormat{CopyFormatCSV, CopyFormatJSON, CopyFormatNDJSON, CopyFormatParquet} {
		for _, rows := range []int{0, 3} {
			t.Run(fmt.Sprintf("%s/%d", format, rows), func(t *testing.T) {
				server := s3test.New()
				defer server.Close()
				cfg := s3ExportConfig(t, server)
				db, err := Open(cfg)
				if err != nil {
					t.Fatal(err)
				}
				compression := "identity"
				if format == CopyFormatCSV {
					compression = "gzip"
				}
				if format == CopyFormatNDJSON {
					compression = "zstd"
				}
				if format == CopyFormatParquet {
					compression = "snappy"
				}
				result, err := db.CopyTo(context.Background(), fmt.Sprintf("SELECT id FROM range(%d) AS t(id)", rows), "s3://bucket/result", CopyOptions{Format: format, Compression: compression})
				if err != nil {
					t.Fatal(err)
				}
				if result.Rows != int64(rows) || len(result.JobID) != 32 {
					t.Fatalf("result=%+v", result)
				}
				obj, ok := server.Object("/bucket/result")
				if !ok || len(obj.Data) == 0 {
					t.Fatal("valid empty format representation missing")
				}
				sum := sha256.Sum256(obj.Data)
				if server.Counts("create") != 1 || server.Counts("complete") != 1 || server.Counts("put") != 0 {
					t.Fatal("zero rows confused with zero physical bytes")
				}
				if rows > 0 {
					fn := "read_" + string(format)
					if format == CopyFormatParquet {
						fn = "read_parquet"
					}
					args := "'s3://bucket/result'"
					if format == CopyFormatCSV {
						args += ", header=true"
					}
					_, err = db.QueryChunks(context.Background(), "SELECT CAST(sum(id) AS BIGINT) FROM "+fn+"("+args+")", func(c *Chunk) error {
						value, ok, err := c.Int64(0, 0)
						if err != nil || !ok || value != 3 {
							return fmt.Errorf("sum=%d ok=%v err=%v", value, ok, err)
						}
						return nil
					})
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db, err = Open(cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				r, err := db.RecoverExport(context.Background(), result.JobID)
				if err != nil {
					t.Fatal(err)
				}
				if r.Checksum != hex.EncodeToString(sum[:]) || r.Bytes != int64(len(obj.Data)) || r.PublishMode != CopyPublishStrict {
					t.Fatalf("recovery=%+v", r)
				}
				before := server.Counts("complete")
				if _, err := db.RecoverExport(context.Background(), result.JobID); err != nil {
					t.Fatal(err)
				}
				if server.Counts("complete") != before {
					t.Fatal("committed recovery republished")
				}
				data, err := os.ReadFile(filepath.Join(cfg.ManifestDir, result.JobID+".json"))
				if err != nil {
					t.Fatal(err)
				}
				var m exportpkg.Manifest
				if err := json.Unmarshal(data, &m); err != nil {
					t.Fatal(err)
				}
				if m.ManifestVersion != 2 || m.FileStates[0].PublishedIdentity == nil || m.FileStates[0].PublishedIdentity.VersionID != obj.Version || m.Publish.CleanupPending {
					t.Fatal("v2 committed identity/cleanup not persisted")
				}
			})
		}
	}
}

func TestS3SQLCopyZeroBytesAndFormats(t *testing.T) {
	for _, format := range []CopyFormat{CopyFormatCSV, CopyFormatJSON, CopyFormatNDJSON, CopyFormatParquet} {
		t.Run(string(format), func(t *testing.T) {
			server := s3test.New()
			defer server.Close()
			cfg := s3ExportConfig(t, server)
			sqlDB, err := OpenSQLDB(cfg)
			if err != nil {
				t.Fatal(err)
			}
			options := "FORMAT " + string(format) + ", PUBLISH_MODE 'compatible'"
			if format == CopyFormatCSV {
				options += ", HEADER false"
			}
			_, err = sqlDB.ExecContext(context.Background(), "COPY (SELECT id FROM range(0) AS t(id)) TO ? ("+options+")", "s3://bucket/result")
			if err != nil {
				t.Fatal(err)
			}
			if err := sqlDB.Close(); err != nil {
				t.Fatal(err)
			}
			zero := format == CopyFormatCSV || format == CopyFormatNDJSON
			if zero {
				if server.Counts("put") != 1 || server.Counts("create") != 0 {
					t.Fatal("empty physical output did not use empty_put")
				}
			} else if server.Counts("complete") != 1 || server.Counts("put") != 0 {
				t.Fatal("nonempty zero-row format used empty_put")
			}
			files, _ := filepath.Glob(filepath.Join(cfg.ManifestDir, "*.json"))
			if len(files) != 1 {
				t.Fatal(files)
			}
			job := filepath.Base(files[0])
			job = job[:len(job)-len(".json")]
			db, err := Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			r, err := db.RecoverExport(context.Background(), job)
			if err != nil {
				t.Fatal(err)
			}
			if r.PublishMode != CopyPublishCompatible || zero && r.Bytes != 0 {
				t.Fatalf("recovery=%+v", r)
			}
		})
	}
}

func TestS3PublicUncertainRecoveryWithoutOriginalSource(t *testing.T) {
	for _, fault := range []string{"complete_lost", "complete_before", "complete_xml", "complete_error", "complete_race", "create_lost"} {
		t.Run(fault, func(t *testing.T) {
			server := s3test.New()
			defer server.Close()
			server.SetFault(fault)
			cfg := s3ExportConfig(t, server)
			cfg.TableFunctions = []TableFunction{remediationBlobRows()}
			db, err := Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			_, err = db.CopyTo(context.Background(), "SELECT b FROM remediation_blob_rows()", "s3://bucket/result", CopyOptions{Format: CopyFormatCSV})
			if err == nil {
				t.Fatal("fault did not fail")
			}
			var job string
			if fault == "complete_error" || fault == "complete_race" || fault == "create_lost" {
				var failure *ExportJobError
				if !errors.As(err, &failure) || errors.Is(err, ErrCommitUncertain) {
					t.Fatal(err)
				}
				job = failure.JobID
			} else {
				var failure *CommitUncertainError
				if !errors.Is(err, ErrCommitUncertain) || !errors.As(err, &failure) {
					t.Fatal(err)
				}
				job = failure.JobID
			}
			if db.MemoryUsed() != 0 {
				t.Fatal("failed query retained memory")
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			server.SetFault("")
			cfg.TableFunctions = nil
			db, err = Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			_, err = db.RecoverExport(context.Background(), job)
			if fault == "complete_error" || fault == "complete_race" || fault == "create_lost" {
				if err == nil {
					t.Fatal("failed job resumed publication")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if db.MemoryUsed() != 0 {
				t.Fatal("recovery retained memory")
			}
		})
	}
}

func TestS3RecoveryReauthorizesConfigurationAndCommittedVersion(t *testing.T) {
	server := s3test.New()
	defer server.Close()
	cfg := s3ExportConfig(t, server)
	db, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	result, err := db.CopyTo(context.Background(), "SELECT 1 AS id", "s3://bucket/result", CopyOptions{Format: CopyFormatCSV})
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	before := len(server.Events())
	cfg.S3.SecretKey = "changed"
	db, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.RecoverExport(context.Background(), result.JobID); !errors.Is(err, ErrExportNotRecoverable) {
		t.Fatal(err)
	}
	_ = db.Close()
	if len(server.Events()) != before {
		t.Fatal("wrong configuration performed remote actions")
	}
	cfg.S3.SecretKey = "secret"
	db, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	obj, _ := server.Object("/bucket/result")
	server.Put("/bucket/result", obj.Data, "") // same bytes, a different current version
	if _, err := db.RecoverExport(context.Background(), result.JobID); !errors.Is(err, ErrCommitConflict) {
		t.Fatal(err)
	}
	server.DeleteObject("/bucket/result")
	if _, err := db.RecoverExport(context.Background(), result.JobID); !errors.Is(err, ErrExportNotRecoverable) {
		t.Fatal(err)
	}
}

func TestS3RecoveryV1DoesNotRepublish(t *testing.T) {
	server := s3test.New()
	defer server.Close()
	server.Put("/bucket/legacy.csv", []byte("id\n1\n"), "")
	cfg := s3ExportConfig(t, server)
	db, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	obj, _ := server.Object("/bucket/legacy.csv")
	job := strings.Repeat("d", 64)
	manifest, err := exportpkg.NewManifest(job, "query", "source", "s3://bucket/", "csv", "identity", "atomic_publish", []string{"legacy.csv"})
	if err != nil {
		t.Fatal(err)
	}
	_ = manifest.TransitionExport(exportpkg.ExportStateWriting)
	_ = manifest.TransitionFile("legacy.csv", exportpkg.FileStateWriting, 0, "")
	sum := sha256.Sum256(obj.Data)
	_ = manifest.TransitionFilePublished("legacy.csv", int64(len(obj.Data)), hex.EncodeToString(sum[:]), exportpkg.PublishedObjectIdentity{Size: int64(len(obj.Data)), ETag: obj.ETag, VersionID: obj.Version})
	_ = manifest.TransitionExport(exportpkg.ExportStateCommitting)
	for _, state := range []exportpkg.ExportState{exportpkg.ExportStateCommitting, exportpkg.ExportStateCommitted} {
		manifest.ExportState = state
		data, err := manifest.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(cfg.ManifestDir, job+".json"), data, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := db.RecoverExport(context.Background(), job); err != nil {
			t.Fatal(err)
		}
	}
	server.DeleteObject("/bucket/legacy.csv")
	if _, err := db.RecoverExport(context.Background(), job); !errors.Is(err, ErrExportNotRecoverable) {
		t.Fatal(err)
	}
	if server.Counts("complete") != 0 || server.Counts("put") != 0 || server.Counts("create") != 0 {
		t.Fatal("v1 recovery recreated output")
	}
}

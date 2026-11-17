package gosql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	exportpkg "github.com/steamwo/fssql/internal/export"
	"github.com/steamwo/fssql/internal/storage/remote/s3"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func realS3Request(t *testing.T, cfg Config, method string, target *url.URL, data []byte) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r, err := http.NewRequestWithContext(ctx, method, target.String(), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(data)
	signer := s3.SigV4Signer{Region: cfg.S3.Region, Creds: s3.Credentials{AccessKey: cfg.S3.AccessKey, SecretKey: cfg.S3.SecretKey}}
	if err := signer.Sign(r, time.Now(), hex.EncodeToString(hash[:])); err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func TestRealRemoteS3MultipartMismatch(t *testing.T) {
	if os.Getenv("FSSQL_REAL_PROTOCOL") != "s3" {
		t.Skip("real MinIO multipart only")
	}
	for _, fault := range []string{"missing-upload", "changed-part"} {
		t.Run(fault, func(t *testing.T) {
			cfg, root, mode := realRemoteConfig(t)
			job := realJobID(t)
			if err := runRealJob(t, cfg, root, mode, job, func(event string) error {
				if event == "save-PREPARED-after" {
					return io.ErrUnexpectedEOF
				}
				return nil
			}); err == nil {
				t.Fatal("did not stop before publish")
			}
			data, err := os.ReadFile(filepath.Join(cfg.ManifestDir, job+".json"))
			if err != nil {
				t.Fatal(err)
			}
			var m exportpkg.Manifest
			if json.Unmarshal(data, &m) != nil || m.Publish.Sealed == nil {
				t.Fatal("missing prepared state")
			}
			var own struct {
				UploadID string            `json:"upload_id"`
				Parts    []json.RawMessage `json:"parts"`
			}
			if json.Unmarshal(m.Publish.Ownership, &own) != nil || own.UploadID == "" || len(own.Parts) != 2 {
				t.Fatal("not a real two-part upload")
			}
			dest, _ := vfs.ParseURI(m.Publish.Intent.Destination)
			target, _ := url.Parse(cfg.S3.Endpoint)
			target.Path = "/" + dest.Host + dest.Path
			query := url.Values{"uploadId": []string{own.UploadID}}
			target.RawQuery = query.Encode()
			if fault == "missing-upload" {
				if status := realS3Request(t, cfg, http.MethodDelete, target, nil); status != 204 {
					t.Fatalf("abort upload status=%d", status)
				}
			} else {
				query.Set("partNumber", "1")
				target.RawQuery = query.Encode()
				if status := realS3Request(t, cfg, http.MethodPut, target, bytes.Repeat([]byte("x"), 8<<20)); status != 200 {
					t.Fatalf("replace part status=%d", status)
				}
				query.Del("partNumber")
				target.RawQuery = query.Encode()
				abortTarget := *target
				defer realS3Request(t, cfg, http.MethodDelete, &abortTarget, nil)
			}
			db, err := Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for n := 0; n < 2; n++ {
				if _, err := db.RecoverExport(context.Background(), job); !errors.Is(err, ErrExportNotRecoverable) {
					t.Fatalf("mismatched parts recovery: %v", err)
				}
			}
			target.RawQuery = ""
			if status := realS3Request(t, cfg, http.MethodHead, target, nil); status != 404 {
				t.Fatalf("invalid multipart became visible: status=%d", status)
			}
			if db.MemoryUsed() != 0 {
				t.Fatal("recovery memory leak")
			}
		})
	}
}

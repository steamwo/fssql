package gosql

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestWebDAVCreateWithoutStrongETagStopsUpload(t *testing.T) {
	for _, etag := range []string{"", `W/"weak"`} {
		t.Run(etag, func(t *testing.T) {
			var received int
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "LOCK" {
					w.WriteHeader(http.StatusMethodNotAllowed)
					return
				}
				if r.Method != http.MethodPut || r.Header.Get("If-None-Match") != "*" {
					t.Errorf("unexpected request: %s", r.Method)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				data, _ := io.ReadAll(r.Body)
				received += len(data)
				if etag != "" {
					w.Header().Set("ETag", etag)
				}
				w.WriteHeader(http.StatusCreated)
			}))
			defer s.Close()
			endpoint := "webdav://" + strings.TrimPrefix(s.URL, "http://")
			db, err := Open(Config{TempDir: t.TempDir(), ManifestDir: t.TempDir(), WebDAV: &WebDAVConfig{Endpoint: endpoint, Username: "test", Password: "test", Network: RemoteNetworkConfig{AllowLoopback: true}}})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			_, err = db.CopyTo(context.Background(), "SELECT 1 AS id", endpoint+"/result.csv", CopyOptions{Format: CopyFormatCSV, PublishMode: CopyPublishCompatible})
			if err == nil || !strings.Contains(err.Error(), "exclusive staging PUT must return a strong ETag") || received != 0 {
				t.Fatalf("error=%v uploaded bytes=%d", err, received)
			}
		})
	}
}

// TestRealRemoteExport requires a dedicated writable directory on a test
// server. Each output has a random name and is retained for inspection.
func realRemoteConfig(t *testing.T) (Config, string, CopyPublishMode) {
	kind := os.Getenv("FSSQL_REAL_PROTOCOL")
	if kind == "" {
		t.Skip("set FSSQL_REAL_PROTOCOL and the dedicated test endpoint to run")
	}
	endpoint, dir := os.Getenv("FSSQL_REAL_ENDPOINT"), os.Getenv("FSSQL_REAL_DIR")
	if !strings.HasPrefix(endpoint, kind+"://") || !strings.HasPrefix(dir, "/") || strings.ContainsAny(dir, "'?#") {
		t.Fatal("invalid endpoint or dedicated directory")
	}
	user, password := os.Getenv("FSSQL_REAL_USER"), os.Getenv("FSSQL_REAL_PASSWORD")
	cfg := Config{TempDir: t.TempDir(), ManifestDir: t.TempDir()}
	mode := CopyPublishStrict
	switch kind {
	case "s3":
		if !strings.HasPrefix(strings.TrimPrefix(endpoint, "s3://"), "fssql-agent-test-") {
			t.Fatal("S3 requires a dedicated fssql-agent-test- bucket")
		}
		cfg.S3 = &S3Config{Endpoint: os.Getenv("FSSQL_MINIO_ENDPOINT"), Region: "us-east-1", ForcePathStyle: true, AllowInsecureHTTP: true, AccessKey: user, SecretKey: password, RequestTimeout: 20 * time.Second, MaxRetries: 1}
	case "sftp":
		cfg.SFTP = &SFTPConfig{Endpoint: endpoint, Username: user, IdentityFile: os.Getenv("FSSQL_REAL_IDENTITY"), KnownHostsFile: os.Getenv("FSSQL_REAL_KNOWN_HOSTS")}
	case "webdav":
		cfg.WebDAV = &WebDAVConfig{Endpoint: endpoint, Username: user, Password: password, AtomicMoveVerified: os.Getenv("FSSQL_REAL_ATOMIC_MOVE_VERIFIED") == "1", Network: RemoteNetworkConfig{AllowLoopback: true, AllowPrivate: true}}
		if !cfg.WebDAV.AtomicMoveVerified {
			mode = CopyPublishCompatible
		}
	case "ftp", "ftps":
		mode = CopyPublishCompatible
		cfg.FTP = &FTPConfig{Endpoint: endpoint, Username: user, Password: password, AllowPlainFTP: kind == "ftp", Network: RemoteNetworkConfig{AllowLoopback: true, AllowPrivate: true}}
		if kind == "ftps" {
			pem, err := os.ReadFile(os.Getenv("FSSQL_REAL_CA"))
			if err != nil {
				t.Fatal(err)
			}
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(pem) {
				t.Fatal("invalid FSSQL_REAL_CA certificate")
			}
			cfg.FTP.TLSConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
		}
	default:
		t.Fatalf("unsupported test protocol %q", kind)
	}
	if requested := os.Getenv("FSSQL_REAL_MODE"); requested != "" {
		if requested != string(CopyPublishStrict) && requested != string(CopyPublishCompatible) {
			t.Fatal("FSSQL_REAL_MODE must be strict or compatible")
		}
		mode = CopyPublishMode(requested)
	}
	return cfg, endpoint + strings.TrimRight(dir, "/"), mode
}

func TestRealRemoteExport(t *testing.T) {
	cfg, root, mode := realRemoteConfig(t)
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, format := range []CopyFormat{CopyFormatCSV, CopyFormatJSON, CopyFormatNDJSON, CopyFormatParquet} {
		t.Run(string(format), func(t *testing.T) {
			dest := root + "/fssql-stage7-" + hex.EncodeToString(random[:]) + "." + string(format)
			db, err := Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			result, err := db.CopyTo(ctx, "SELECT id FROM range(2) AS t(id)", dest, CopyOptions{Format: format, PublishMode: mode})
			if err != nil {
				_ = db.Close()
				t.Fatal(err)
			}
			args := "'" + dest + "'"
			if format == CopyFormatCSV {
				args += ", header=true"
			}
			var sum int64
			_, err = db.QueryChunks(ctx, "SELECT CAST(sum(id) AS BIGINT) FROM read_"+string(format)+"("+args+")", func(c *Chunk) error {
				v, valid, err := c.Int64(0, 0)
				if err != nil || !valid {
					return fmt.Errorf("invalid sum: valid=%t: %v", valid, err)
				}
				sum = v
				return nil
			})
			if err != nil || sum != 1 || result.Rows != 2 {
				_ = db.Close()
				t.Fatalf("sum=%d rows=%d: %v", sum, result.Rows, err)
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for i := 0; i < 2; i++ {
				recovered, err := db.RecoverExport(ctx, result.JobID)
				if err != nil || recovered.Bytes != result.Bytes || recovered.PublishMode != mode {
					t.Fatalf("recovery=%+v: %v", recovered, err)
				}
			}
			if used := db.MemoryUsed(); used != 0 {
				t.Fatalf("managed memory leak: %d", used)
			}
		})
	}
}

func TestRealRemoteExistingDestination(t *testing.T) {
	cfg, root, mode := realRemoteConfig(t)
	db, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dest := root + "/existing-" + realJobID(t) + ".csv"
	first, err := db.CopyTo(context.Background(), "SELECT 1 AS id", dest, CopyOptions{Format: CopyFormatCSV, PublishMode: mode})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CopyTo(context.Background(), "SELECT 2 AS id", dest, CopyOptions{Format: CopyFormatCSV, PublishMode: mode}); !errors.Is(err, ErrCommitConflict) {
		t.Fatalf("existing destination: %v", err)
	}
	if _, err := db.RecoverExport(context.Background(), first.JobID); err != nil {
		t.Fatalf("existing destination overwritten: %v", err)
	}
	if db.MemoryUsed() != 0 {
		t.Fatal("managed memory leak")
	}
}

package gosql

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// A real server completes a mutating HTTP request; the proxy then drops its
// response connection. This exercises unknown ownership/commit over the wire,
// separately from the driver-boundary and manifest-store fault matrix.
func TestRealRemoteHTTPResponseLoss(t *testing.T) {
	kind := os.Getenv("FSSQL_REAL_PROTOCOL")
	if kind != "webdav" && kind != "s3" {
		t.Skip("real WebDAV or MinIO only")
	}
	for _, point := range []string{"create", "publish"} {
		t.Run(point, func(t *testing.T) {
			cfg, root, mode := realRemoteConfig(t)
			var backendURL string
			if kind == "s3" {
				backendURL = cfg.S3.Endpoint
			} else {
				backendURL = strings.Replace(cfg.WebDAV.Endpoint, "webdav://", "http://", 1)
			}
			backend, err := url.Parse(backendURL)
			if err != nil {
				t.Fatal(err)
			}
			var count atomic.Int32
			proxy := httputil.NewSingleHostReverseProxy(backend)
			director := proxy.Director
			var proxyURL string
			proxy.Director = func(r *http.Request) {
				director(r)
				if kind == "webdav" {
					r.Host = backend.Host
					for _, header := range []string{"Destination", "If"} {
						r.Header.Set(header, strings.ReplaceAll(r.Header.Get(header), proxyURL, backend.String()))
					}
				}
			}
			proxy.ErrorLog = log.New(io.Discard, "", 0)
			proxy.ModifyResponse = func(r *http.Response) error {
				q := r.Request.URL.Query()
				match := kind == "webdav" && (point == "create" && r.Request.Method == "LOCK" || point == "publish" && r.Request.Method == "MOVE") || kind == "s3" && r.Request.Method == "POST" && (point == "create" && q.Has("uploads") || point == "publish" && q.Has("uploadId"))
				if match {
					count.Add(1)
					if r.StatusCode < 200 || r.StatusCode >= 300 {
						t.Errorf("upstream mutation status=%d", r.StatusCode)
					}
					_, _ = io.Copy(io.Discard, r.Body)
					_ = r.Body.Close()
					return io.ErrUnexpectedEOF
				}
				return nil
			}
			proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
				c, _, err := w.(http.Hijacker).Hijack()
				if err == nil {
					_ = c.Close()
				}
			}
			server := httptest.NewServer(proxy)
			defer server.Close()
			proxyURL = server.URL
			if kind == "webdav" {
				original := cfg.WebDAV.Endpoint
				cfg.WebDAV.Endpoint = strings.Replace(server.URL, "http://", "webdav://", 1)
				root = strings.Replace(root, original, cfg.WebDAV.Endpoint, 1)
			} else {
				cfg.S3.Endpoint = server.URL
			}
			job := realJobID(t)
			err = runRealJob(t, cfg, root, mode, job, func(string) error { return nil })
			if count.Load() != 1 || err == nil {
				t.Fatalf("mutations=%d error=%v", count.Load(), err)
			}
			if point == "publish" {
				if !errors.Is(err, ErrCommitUncertain) {
					t.Fatalf("lost publication: %v", err)
				}
				checkRealRecovery(t, cfg, job, true)
			} else {
				data, err := os.ReadFile(filepath.Join(cfg.ManifestDir, job+".json"))
				if err != nil {
					t.Fatal(err)
				}
				var m struct {
					Publish struct {
						CleanupPending bool `json:"cleanup_pending"`
					} `json:"publish"`
				}
				if json.Unmarshal(data, &m) != nil || !m.Publish.CleanupPending {
					t.Fatal("unknown ownership incorrectly cleaned")
				}
				db, err := Open(cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				if _, err := db.RecoverExport(context.Background(), job); !errors.Is(err, ErrExportNotRecoverable) {
					t.Fatalf("lost creation recovery: %v", err)
				}
			}
		})
	}
}

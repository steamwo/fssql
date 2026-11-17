package gosql

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	exportpkg "github.com/steamwo/fssql/internal/export"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type httpParquetSink struct{ bytes.Buffer }

func (s *httpParquetSink) Write(_ context.Context, p []byte) (int, error) { return s.Buffer.Write(p) }
func (*httpParquetSink) Flush(context.Context) error                      { return nil }
func (s *httpParquetSink) Close(context.Context) (exportpkg.WriteResult, error) {
	return exportpkg.WriteResult{Bytes: int64(s.Len())}, nil
}
func (*httpParquetSink) Abort(context.Context) error { return nil }
func httpParquet(t *testing.T) []byte {
	t.Helper()
	m := memory.NewManager(8 << 20)
	s := m.NewScope()
	defer s.Close()
	bm := memory.NewBufferManager(nil)
	sink := &httpParquetSink{}
	typ := types.LogicalType{ID: types.Integer}
	w, err := exportpkg.NewParquetWriter([]exportpkg.Column{{Name: "id", Type: typ}}, exportpkg.ParquetWriteOptions{}, s, bm, sink)
	if err != nil {
		t.Fatal(err)
	}
	v, err := vector.NewFlat(s, bm, typ, 2)
	if err != nil {
		t.Fatal(err)
	}
	v.SetInt32(0, 7)
	v.SetInt32(1, 11)
	chunk, err := vector.NewDataChunk(v)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteChunk(context.Background(), chunk); err != nil {
		t.Fatal(err)
	}
	chunk.Release()
	if _, err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.Used() != 0 {
		t.Fatal("fixture memory leaked")
	}
	return sink.Bytes()
}

func TestHTTPFormatsNativeAndDatabaseSQL(t *testing.T) {
	parquet := httpParquet(t)
	for _, format := range []string{"csv", "json", "ndjson", "parquet"} {
		for _, ranged := range []bool{false, true} {
			for _, secure := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/range=%t/tls=%t", format, ranged, secure), func(t *testing.T) {
					data := []byte("id\n7\n11\n")
					if format == "json" {
						data = []byte("[{\"id\":7},{\"id\":11}]")
					}
					if format == "ndjson" {
						data = []byte("{\"id\":7}\n{\"id\":11}\n")
					}
					if format == "parquet" {
						data = parquet
					}
					var gets atomic.Int32
					handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						gets.Add(1)
						if r.Method != "GET" || r.RequestURI != "/signed%2Fdata//file?sig=secret&a=&a=?*[" {
							t.Errorf("request changed: %s %s", r.Method, r.RequestURI)
						}
						if ranged {
							w.Header().Set("ETag", "\"fixture\"")
							http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(data))
						} else {
							w.WriteHeader(200)
							w.(http.Flusher).Flush()
							_, _ = w.Write(data)
						}
					})
					server := httptest.NewUnstartedServer(handler)
					if secure {
						server.StartTLS()
					} else {
						server.Start()
					}
					defer server.Close()
					endpoint := HTTPConfig{Endpoint: server.URL, Network: RemoteNetworkConfig{AllowLoopback: true}}
					if secure {
						roots := x509.NewCertPool()
						roots.AddCert(server.Certificate())
						endpoint.TLSConfig = &tls.Config{RootCAs: roots}
					}
					config := Config{HTTP: []HTTPConfig{endpoint}, TempDir: t.TempDir(), ManifestDir: t.TempDir()}
					db, err := Open(config)
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()
					uri := server.URL + "/signed%2Fdata//file?sig=secret&a=&a=?*["
					args := "'" + uri + "'"
					sqlArgs := "?"
					if format == "csv" {
						args += ", header=true"
						sqlArgs += ", header=true"
					}
					query := "SELECT CAST(sum(id) AS BIGINT) FROM read_" + format + "(" + args + ")"
					var sum int64
					_, err = db.QueryChunks(context.Background(), query, func(chunk *Chunk) error {
						value, ok, err := chunk.Int64(0, 0)
						if err != nil {
							return err
						}
						if !ok {
							return errors.New("NULL sum")
						}
						sum += value
						return nil
					})
					if err != nil || sum != 18 {
						t.Fatalf("native sum=%d err=%v", sum, err)
					}
					if db.MemoryUsed() != 0 || db.temporary.Used() != 0 {
						t.Fatal("query leaked managed resources")
					}
					sqlDB, err := OpenSQLDB(config)
					if err != nil {
						t.Fatal(err)
					}
					defer sqlDB.Close()
					err = sqlDB.QueryRowContext(context.Background(), "SELECT CAST(sum(id) AS BIGINT) FROM read_"+format+"("+sqlArgs+")", uri).Scan(&sum)
					if err != nil || sum != 18 {
						t.Fatalf("database/sql sum=%d err=%v", sum, err)
					}
					if !ranged && gets.Load() != 2 {
						t.Fatalf("whole-body GET repeated: %d", gets.Load())
					}
				})
			}
		}
	}
}

func TestHTTPAuthenticationConfigurationCopyAndTLSClone(t *testing.T) {
	for _, auth := range []string{"anonymous", "basic", "bearer", "api-key"} {
		t.Run(auth, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				valid := true
				switch auth {
				case "anonymous":
					valid = r.Header.Get("Authorization") == ""
				case "basic":
					u, p, ok := r.BasicAuth()
					valid = ok && u == "user" && p == "password"
				case "bearer":
					valid = r.Header.Get("Authorization") == "Bearer token"
				case "api-key":
					valid = r.Header.Get("X-Key") == "original"
				}
				if !valid {
					w.WriteHeader(401)
					return
				}
				_, _ = io.WriteString(w, "id\n7\n11\n")
			}))
			defer server.Close()
			roots := x509.NewCertPool()
			roots.AddCert(server.Certificate())
			tlsConfig := &tls.Config{RootCAs: roots}
			headers := map[string]string{}
			c := HTTPConfig{Endpoint: server.URL, TLSConfig: tlsConfig, Headers: headers, Network: RemoteNetworkConfig{AllowLoopback: true}}
			switch auth {
			case "basic":
				c.Username, c.Password = "user", "password"
			case "bearer":
				c.BearerToken = "token"
			case "api-key":
				headers["X-Key"] = "original"
			}
			configs := []HTTPConfig{c}
			db, err := Open(Config{HTTP: configs, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			headers["X-Key"] = "changed"
			configs[0].Username = "changed"
			tlsConfig.ServerName = "wrong.example"
			_, err = db.QueryChunks(context.Background(), "SELECT * FROM read_csv('"+server.URL+"/data',header=true)", func(*Chunk) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			if db.MemoryUsed() != 0 || db.temporary.Used() != 0 {
				t.Fatal("resources leaked")
			}
		})
	}
}

func TestHTTPMultipleEndpointJoinAndSignedURLIsolation(t *testing.T) {
	var firstGets, secondGets atomic.Int32
	one := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstGets.Add(1)
		if r.Header.Get("Authorization") != "Bearer one" {
			w.WriteHeader(401)
			return
		}
		data := "id\n1\n"
		if r.URL.Query().Get("sig") == "two" {
			data = "id\n2\n"
		}
		w.Header().Set("ETag", "\"same\"")
		http.ServeContent(w, r, "data", time.Time{}, strings.NewReader(data))
	}))
	defer one.Close()
	two := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondGets.Add(1)
		if r.Header.Get("X-Key") != "two" || r.Header.Get("Authorization") != "" {
			w.WriteHeader(401)
			return
		}
		_, _ = io.WriteString(w, "id\n1\n")
	}))
	defer two.Close()
	db, err := Open(Config{HTTP: []HTTPConfig{{Endpoint: one.URL, BearerToken: "one", Network: RemoteNetworkConfig{AllowLoopback: true}}, {Endpoint: two.URL, Headers: map[string]string{"X-Key": "two"}, Network: RemoteNetworkConfig{AllowLoopback: true}}}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queries := []struct {
		sql  string
		want int64
	}{
		{"SELECT CAST(sum(a.id+b.id) AS BIGINT) FROM read_csv('" + one.URL + "/data?sig=one',header=true) a JOIN read_csv('" + two.URL + "/data',header=true) b ON a.id=b.id", 2},
		{"SELECT CAST(sum(a.id+b.id) AS BIGINT) FROM read_csv('" + one.URL + "/data?sig=one',header=true) a CROSS JOIN read_csv('" + one.URL + "/data?sig=two',header=true) b", 3},
	}
	for _, q := range queries {
		var got int64
		_, err := db.QueryChunks(context.Background(), q.sql, func(c *Chunk) error { value, _, e := c.Int64(0, 0); got += value; return e })
		if err != nil || got != q.want {
			t.Fatalf("value=%d err=%v", got, err)
		}
	}
	if firstGets.Load() == 0 || secondGets.Load() != 1 || db.MemoryUsed() != 0 || db.temporary.Used() != 0 {
		t.Fatal("endpoint sharing or cleanup failed")
	}
}

func TestHTTPDefaultDenyValidationAndRequireRange(t *testing.T) {
	db, err := Open(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, scheme := range []string{"http", "https"} {
		_, err := db.QueryChunks(context.Background(), "SELECT * FROM read_csv('"+scheme+"://example.test/data?sig=secret')", func(*Chunk) error { return nil })
		if !errors.Is(err, vfs.ErrUnsupportedScheme) || strings.Contains(err.Error(), "secret") {
			t.Fatal(err)
		}
	}
	configured, err := Open(Config{HTTP: []HTTPConfig{{Endpoint: "https://no-dns-needed.invalid"}}})
	if err != nil {
		t.Fatal(err)
	}
	_ = configured.Close()
	for _, c := range []HTTPConfig{{Endpoint: "https://example.test/path"}, {Endpoint: "https://example.test", Headers: map[string]string{"Authorization": "secret"}}, {Endpoint: "https://example.test", FallbackPolicy: "wrong"}} {
		if bad, err := Open(Config{HTTP: []HTTPConfig{c}}); err == nil {
			_ = bad.Close()
			t.Fatal("invalid HTTP config accepted")
		}
	}
	var gets atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { gets.Add(1); _, _ = io.WriteString(w, "id\n1\n") }))
	defer server.Close()
	rangeDB, err := Open(Config{HTTP: []HTTPConfig{{Endpoint: server.URL, FallbackPolicy: HTTPFallbackRequireRange, Network: RemoteNetworkConfig{AllowLoopback: true}}}, TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer rangeDB.Close()
	_, err = rangeDB.QueryChunks(context.Background(), "SELECT * FROM read_csv('"+server.URL+"/data?sig=secret')", func(*Chunk) error { return nil })
	if !errors.Is(err, vfs.ErrRangeUnsupported) || gets.Load() != 1 || rangeDB.temporary.Used() != 0 || rangeDB.MemoryUsed() != 0 {
		t.Fatal(err)
	}
}

func TestHTTPMaterializationQuotaFailureAndCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		_, _ = io.WriteString(w, strings.Repeat("1\n", 100000))
	}))
	defer server.Close()
	for _, tc := range []struct {
		mem, temp int64
		want      error
	}{{1 << 20, 10, storagetemp.ErrQuotaExceeded}, {16, 1 << 20, memory.ErrLimitExceeded}} {
		db, err := Open(Config{HTTP: []HTTPConfig{{Endpoint: server.URL, Network: RemoteNetworkConfig{AllowLoopback: true}}}, MemoryLimitBytes: tc.mem, TempLimitBytes: tc.temp, TempDir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.QueryChunks(context.Background(), "SELECT * FROM read_csv('"+server.URL+"/data?sig=secret')", func(*Chunk) error { return nil })
		if !errors.Is(err, tc.want) || db.MemoryUsed() != 0 || db.temporary.Used() != 0 {
			t.Fatalf("want=%v err=%v mem=%d temp=%d", tc.want, err, db.MemoryUsed(), db.temporary.Used())
		}
		_ = db.Close()
	}
	started := make(chan struct{}, 1)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		select {
		case started <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	}))
	defer slow.Close()
	db, err := Open(Config{HTTP: []HTTPConfig{{Endpoint: slow.URL, Network: RemoteNetworkConfig{AllowLoopback: true}}}, TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := db.QueryChunks(ctx, "SELECT * FROM read_csv('"+slow.URL+"/data')", func(*Chunk) error { return nil })
		done <- err
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("query did not cancel")
	}
	if db.MemoryUsed() != 0 || db.temporary.Used() != 0 {
		t.Fatal("canceled query leaked")
	}
}

func TestHTTPDatabaseSQLRowsCloseAndBindFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/invalid" {
			_, _ = io.WriteString(w, "invalid JSON")
			return
		}
		_, _ = io.WriteString(w, "id\n"+strings.Repeat("7\n", 10000))
	}))
	defer server.Close()
	config := Config{HTTP: []HTTPConfig{{Endpoint: server.URL, Network: RemoteNetworkConfig{AllowLoopback: true}}}, TempDir: t.TempDir(), ManifestDir: t.TempDir()}
	sqlDB, err := OpenSQLDB(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	rows, err := sqlDB.QueryContext(context.Background(), "SELECT * FROM read_csv(?,header=true)", server.URL+"/data")
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal(rows.Err())
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(config.TempDir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("early Rows.Close retained temporary objects: entries=%d err=%v", len(entries), err)
	}
	db, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.QueryChunks(context.Background(), "SELECT * FROM read_json('"+server.URL+"/invalid?sig=secret')", func(*Chunk) error { return nil })
	if err == nil || strings.Contains(err.Error(), "secret") || db.MemoryUsed() != 0 || db.temporary.Used() != 0 {
		t.Fatal(err)
	}
	_, err = db.QueryChunks(context.Background(), "SELECT * FROM read_csv('"+server.URL+"/unconfigured')", func(*Chunk) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
}

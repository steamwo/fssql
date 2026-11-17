package gosql

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

func gzipReadFixture(t *testing.T, data []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
func zstdReadFixture(data []byte) []byte {
	out := []byte{0x28, 0xb5, 0x2f, 0xfd, 0xa0}
	var size [4]byte
	binary.LittleEndian.PutUint32(size[:], uint32(len(data)))
	out = append(out, size[:]...)
	for {
		n := min(len(data), 128<<10)
		h := uint32(n) << 3
		if n == len(data) {
			h |= 1
		}
		out = append(out, byte(h), byte(h>>8), byte(h>>16))
		out = append(out, data[:n]...)
		data = data[n:]
		if len(data) == 0 {
			break
		}
	}
	return out
}
func compressionSum(t *testing.T, db *DB, query string, want int64) {
	t.Helper()
	var got int64
	_, err := db.QueryChunks(context.Background(), query, func(c *Chunk) error {
		value, valid, err := c.Int64(0, 0)
		if !valid && err == nil {
			return errors.New("NULL aggregate")
		}
		got += value
		return err
	})
	if err != nil || got != want {
		t.Fatalf("sum=%d want=%d err=%v query=%s", got, want, err, query)
	}
	if db.MemoryUsed() != 0 || db.temporary.Used() != 0 {
		t.Fatal("compression query leaked")
	}
}

func TestCompressedTextHTTPOptionsAndAliases(t *testing.T) {
	for _, format := range []string{"csv", "json", "ndjson"} {
		for _, fileKind := range []string{"none", "gzip", "zstd"} {
			for _, transportGzip := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/http-gzip=%t", format, fileKind, transportGzip), func(t *testing.T) {
					plain := []byte("id\n7\n11\n")
					if format == "json" {
						plain = []byte("[{\"id\":7},{\"id\":11}]")
					}
					if format == "ndjson" {
						plain = []byte("{\"id\":7}\n{\"id\":11}\n")
					}
					body := plain
					if fileKind == "gzip" {
						body = gzipReadFixture(t, body)
					}
					if fileKind == "zstd" {
						body = zstdReadFixture(body)
					}
					if transportGzip {
						body = gzipReadFixture(t, body)
					}
					var gets atomic.Int32
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						gets.Add(1)
						if transportGzip {
							w.Header().Set("Content-Encoding", "gzip")
						}
						w.Header().Set("Content-Type", "application/octet-stream")
						if transportGzip && fileKind == "zstd" {
							w.WriteHeader(200)
							w.(http.Flusher).Flush()
						}
						_, _ = w.Write(body)
					}))
					defer server.Close()
					config := Config{HTTP: []HTTPConfig{{Endpoint: server.URL, Network: RemoteNetworkConfig{AllowLoopback: true}}}, MemoryLimitBytes: 32 << 20, TempLimitBytes: int64(len(plain)), TempDir: t.TempDir()}
					db, err := Open(config)
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()
					url := server.URL + "/no-suffix?sig=secret"
					option := ""
					if format == "csv" {
						option = ",header=true"
					}
					for _, mode := range []string{"auto", fileKind} {
						compressionSum(t, db, "SELECT CAST(sum(id) AS BIGINT) FROM read_"+format+"('"+url+"'"+option+",compression='"+mode+"')", 18)
					}
					compressionSum(t, db, "SELECT CAST(sum(id) AS BIGINT) FROM read_"+format+"('"+url+"'"+option+")", 18)
					sqlDB, err := OpenSQLDB(config)
					if err != nil {
						t.Fatal(err)
					}
					defer sqlDB.Close()
					var got int64
					err = sqlDB.QueryRowContext(context.Background(), "SELECT CAST(sum(id) AS BIGINT) FROM read_"+format+"(?"+option+",compression=?)", url, fileKind).Scan(&got)
					if err != nil || got != 18 {
						t.Fatalf("SQL compression sum=%d err=%v", got, err)
					}
					alias := "read_csv_auto"
					extra := ",header=true"
					if format != "csv" {
						alias = "read_json_auto"
						extra = ""
						if format == "ndjson" {
							extra = ",format='newline_delimited'"
						}
					}
					compressionSum(t, db, "SELECT CAST(sum(id) AS BIGINT) FROM "+alias+"('"+url+"'"+extra+",compression='auto')", 18)
					if gets.Load() != 5 {
						t.Fatalf("duplicate HTTP materializations: %d", gets.Load())
					}
				})
			}
		}
	}
}

func TestHTTPGzipPartialProbeAndRequireRangeFileDecode(t *testing.T) {
	plain := []byte("id\n7\n11\n")
	gz := gzipReadFixture(t, plain)
	for _, require := range []bool{false, true} {
		t.Run(fmt.Sprint(require), func(t *testing.T) {
			var gets atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gets.Add(1)
				w.Header().Set("ETag", "\"fixed\"")
				w.Header().Set("Content-Encoding", "gzip")
				if r.Header.Get("Range") != "" {
					w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-0/%d", len(gz)))
					w.WriteHeader(206)
					_, _ = w.Write(gz[:1])
					return
				}
				_, _ = w.Write(gz)
			}))
			defer server.Close()
			policy := HTTPFallbackMaterialize
			if require {
				policy = HTTPFallbackRequireRange
			}
			db, err := Open(Config{HTTP: []HTTPConfig{{Endpoint: server.URL, Network: RemoteNetworkConfig{AllowLoopback: true}, FallbackPolicy: policy}}, TempDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			query := "SELECT CAST(sum(id) AS BIGINT) FROM read_csv('" + server.URL + "/data',header=true,compression='none')"
			if !require {
				compressionSum(t, db, query, 18)
				if gets.Load() != 2 {
					t.Fatal("partial gzip was decoded or full GET repeated")
				}
			} else {
				_, err := db.QueryChunks(context.Background(), query, func(*Chunk) error { return nil })
				if err == nil || gets.Load() != 1 {
					t.Fatal("require_range accepted compressed partial response")
				}
			}
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", "\"file-gzip\"")
		http.ServeContent(w, r, "no-suffix", time.Time{}, bytes.NewReader(gz))
	}))
	defer server.Close()
	db, err := Open(Config{HTTP: []HTTPConfig{{Endpoint: server.URL, Network: RemoteNetworkConfig{AllowLoopback: true}, FallbackPolicy: HTTPFallbackRequireRange}}, TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	compressionSum(t, db, "SELECT CAST(sum(id) AS BIGINT) FROM read_csv('"+server.URL+"/data',header=true,compression='gzip')", 18)
}

func TestHTTPGzipParquetAndRejectFileWrapping(t *testing.T) {
	parquet := httpParquet(t)
	for _, wrap := range []bool{false, true} {
		t.Run(fmt.Sprint(wrap), func(t *testing.T) {
			body := parquet
			if wrap {
				body = gzipReadFixture(t, body)
			}
			body = gzipReadFixture(t, body)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Encoding", "gzip")
				_, _ = w.Write(body)
			}))
			defer server.Close()
			db, err := Open(Config{HTTP: []HTTPConfig{{Endpoint: server.URL, Network: RemoteNetworkConfig{AllowLoopback: true}}}, TempDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			query := "SELECT CAST(sum(id) AS BIGINT) FROM read_parquet('" + server.URL + "/data')"
			if !wrap {
				compressionSum(t, db, query, 18)
			} else {
				_, err := db.QueryChunks(context.Background(), query, func(*Chunk) error { return nil })
				if err == nil {
					t.Fatal("whole-file wrapped Parquet accepted")
				}
				if db.MemoryUsed() != 0 || db.temporary.Used() != 0 {
					t.Fatal("wrapped Parquet leaked")
				}
			}
		})
	}
}

func TestHTTPCompressionFailureCleanup(t *testing.T) {
	plain := []byte("id\n" + strings.Repeat("7\n", 100000))
	good := gzipReadFixture(t, plain)
	bad := append([]byte(nil), good...)
	bad[len(bad)-8] ^= 1
	for _, tc := range []struct {
		name      string
		body      []byte
		mem, temp int64
		want      error
	}{
		{"checksum", bad, 32 << 20, 1 << 20, gzip.ErrChecksum},
		{"truncation", good[:len(good)-2], 32 << 20, 1 << 20, io.ErrUnexpectedEOF},
		{"memory", good, 100000, 1 << 20, memory.ErrLimitExceeded},
		{"quota", good, 32 << 20, 100, storagetemp.ErrQuotaExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Encoding", "gzip")
				_, _ = w.Write(tc.body)
			}))
			defer server.Close()
			db, err := Open(Config{HTTP: []HTTPConfig{{Endpoint: server.URL, Network: RemoteNetworkConfig{AllowLoopback: true}}}, MemoryLimitBytes: tc.mem, TempLimitBytes: tc.temp, TempDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			_, err = db.QueryChunks(context.Background(), "SELECT * FROM read_csv('"+server.URL+"/data?sig=secret',header=true)", func(*Chunk) error { return nil })
			if !errors.Is(err, tc.want) || strings.Contains(err.Error(), "secret") || db.MemoryUsed() != 0 || db.temporary.Used() != 0 {
				t.Fatalf("want=%v err=%v", tc.want, err)
			}
		})
	}
}

func TestCompressedLocalGlobAndMixedRepresentations(t *testing.T) {
	root := t.TempDir()
	plain := []byte("id\n7\n11\n")
	for name, data := range map[string][]byte{"a.data": plain, "b.data": gzipReadFixture(t, plain), "c.data": zstdReadFixture(plain)} {
		if err := os.WriteFile(filepath.Join(root, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	db, err := Open(Config{LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	compressionSum(t, db, "SELECT CAST(sum(id) AS BIGINT) FROM read_csv('file:///"+filepath.ToSlash(filepath.Join(root, "*.data"))+"',header=true)", 54)
	_, err = db.QueryChunks(context.Background(), "SELECT * FROM read_csv('file:///"+filepath.ToSlash(filepath.Join(root, "a.data"))+"',compression='gzip')", func(*Chunk) error { return nil })
	if err == nil {
		t.Fatal("compression mismatch accepted")
	}
	if db.MemoryUsed() != 0 || db.temporary.Used() != 0 {
		t.Fatal("local mismatch retained state")
	}
}

func TestCompressedHTTPViewSharingAndCancellation(t *testing.T) {
	plain := []byte("id\n7\n11\n")
	body := gzipReadFixture(t, plain)
	var gets atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { gets.Add(1); _, _ = w.Write(body) }))
	defer server.Close()
	db, err := Open(Config{HTTP: []HTTPConfig{{Endpoint: server.URL, Network: RemoteNetworkConfig{AllowLoopback: true}}}, TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, mode := range []string{"auto", "gzip"} {
		gets.Store(0)
		wantViews := int64(1)
		if mode == "gzip" {
			wantViews = 2
		}
		query := "SELECT CAST(sum(a.id+b.id) AS BIGINT) FROM read_csv('" + server.URL + "/data',header=true,compression='auto') a CROSS JOIN read_csv('" + server.URL + "/data',header=true,compression='" + mode + "') b"
		var sum int64
		_, err := db.QueryChunks(context.Background(), query, func(c *Chunk) error {
			v, _, e := c.Int64(0, 0)
			sum += v
			if db.temporary.Used() != wantViews*int64(len(plain)) {
				return fmt.Errorf("unexpected view temporary bytes %d", db.temporary.Used())
			}
			return e
		})
		if err != nil || sum != 72 || int64(gets.Load()) != wantViews || db.MemoryUsed() != 0 || db.temporary.Used() != 0 {
			t.Fatalf("sum=%d GETs=%d err=%v", sum, gets.Load(), err)
		}
	}
	started := make(chan struct{}, 1)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(body[:10])
		w.(http.Flusher).Flush()
		select {
		case started <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	}))
	defer slow.Close()
	slowDB, err := Open(Config{HTTP: []HTTPConfig{{Endpoint: slow.URL, Network: RemoteNetworkConfig{AllowLoopback: true}}}, TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer slowDB.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := slowDB.QueryChunks(ctx, "SELECT * FROM read_csv('"+slow.URL+"/data',header=true)", func(*Chunk) error { return nil })
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
		t.Fatal("gzip decode did not cancel")
	}
	if slowDB.MemoryUsed() != 0 || slowDB.temporary.Used() != 0 {
		t.Fatal("canceled gzip decoder leaked")
	}
}

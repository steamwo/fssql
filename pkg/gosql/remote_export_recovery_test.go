package gosql

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	exportpkg "github.com/steamwo/fssql/internal/export"
	"github.com/steamwo/fssql/internal/storage/remote/filetest"
)

// The child test executable replaces only the ssh process. The real OpenSSH
// factory, SFTP handshake/packets, pool and public COPY pipeline still execute.
func init() {
	if os.Getenv("FSSQL_STAGE6_SFTP_PROXY") != "1" {
		return
	}
	c, err := net.Dial("tcp", os.Getenv("FSSQL_STAGE6_SFTP_ADDRESS"))
	if err != nil {
		os.Exit(2)
	}
	go func() { _, _ = io.Copy(c, os.Stdin) }()
	_, _ = io.Copy(os.Stdout, c)
	_ = c.Close()
	os.Exit(0)
}

var remoteExportModes = []string{"sftp/strict", "webdav/strict", "sftp/compatible", "webdav/compatible", "ftp/compatible", "ftps/compatible"}

func TestRemoteSFTPQueriesDoNotOwnPooledProcessLifetime(t *testing.T) {
	store := filetest.New()
	cfg, dest, mode := remoteExportConfig(t, "sftp/strict", store)
	db, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	first, err := db.CopyTo(ctx, "SELECT 1 AS id", dest+"-first", CopyOptions{Format: CopyFormatCSV, PublishMode: mode})
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	// Allow a CommandContext-owned process to observe the finished query.
	time.Sleep(100 * time.Millisecond)
	if _, err := db.CopyTo(context.Background(), "SELECT 2 AS id", dest+"-second", CopyOptions{Format: CopyFormatCSV, PublishMode: mode}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RecoverExport(context.Background(), first.JobID); err != nil {
		t.Fatal(err)
	}
	if db.MemoryUsed() != 0 {
		t.Fatal("managed memory leak")
	}
}

func remoteExportConfig(t *testing.T, kind string, store *filetest.Store) (Config, string, CopyPublishMode) {
	t.Helper()
	protocol, mode, _ := strings.Cut(kind, "/")
	cfg := Config{TempDir: t.TempDir(), ManifestDir: t.TempDir()}
	var endpoint string
	switch protocol {
	case "sftp":
		l, closeFn, err := filetest.Listen(store.SFTP)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(closeFn)
		t.Setenv("FSSQL_STAGE6_SFTP_PROXY", "1")
		t.Setenv("FSSQL_STAGE6_SFTP_ADDRESS", l.Addr().String())
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		endpoint = "sftp://" + l.Addr().String()
		cfg.SFTP = &SFTPConfig{Endpoint: endpoint, Username: "user", KnownHostsFile: "fixture_known_hosts", SSHBinary: executable, MaxSessions: 2}
	case "webdav", "webdavs":
		store.DAVLocks = true
		s := httptest.NewUnstartedServer(store)
		if protocol == "webdavs" {
			s.StartTLS()
		} else {
			s.Start()
		}
		t.Cleanup(s.Close)
		endpoint = protocol + "://" + s.Listener.Addr().String()
		cfg.WebDAV = &WebDAVConfig{Endpoint: endpoint, Username: "user", Password: "pass", AtomicMoveVerified: true, Network: RemoteNetworkConfig{AllowLoopback: true}}
		if protocol == "webdavs" {
			roots := x509.NewCertPool()
			roots.AddCert(s.Certificate())
			cfg.WebDAV.TLSConfig = &tls.Config{RootCAs: roots}
		}
	case "ftp", "ftps":
		var serverTLS, clientTLS *tls.Config
		if protocol == "ftps" {
			s := httptest.NewTLSServer(http.NotFoundHandler())
			t.Cleanup(s.Close)
			serverTLS = s.TLS.Clone()
			roots := x509.NewCertPool()
			roots.AddCert(s.Certificate())
			clientTLS = &tls.Config{RootCAs: roots}
		}
		l, closeFn, err := filetest.Listen(func(c net.Conn) { store.FTP(c, serverTLS) })
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(closeFn)
		endpoint = protocol + "://" + l.Addr().String()
		cfg.FTP = &FTPConfig{Endpoint: endpoint, Username: "user", Password: "pass", AllowPlainFTP: true, Network: RemoteNetworkConfig{AllowLoopback: true}, TLSConfig: clientTLS, MaxSessions: 2}
	}
	return cfg, endpoint + "/result", CopyPublishMode(mode)
}

func TestRemoteCopyFourFormatsAndRecovery(t *testing.T) {
	for _, kind := range remoteExportModes {
		for _, format := range []CopyFormat{CopyFormatCSV, CopyFormatJSON, CopyFormatNDJSON, CopyFormatParquet} {
			for _, rows := range []int{0, 2} {
				t.Run(fmt.Sprintf("%s/%s/%d", kind, format, rows), func(t *testing.T) {
					store := filetest.New()
					cfg, dest, mode := remoteExportConfig(t, kind, store)
					db, err := Open(cfg)
					if err != nil {
						t.Fatal(err)
					}
					compression := "identity"
					switch format {
					case CopyFormatCSV:
						compression = "gzip"
					case CopyFormatNDJSON:
						compression = "zstd"
					case CopyFormatParquet:
						compression = "snappy"
					}
					result, err := db.CopyTo(context.Background(), fmt.Sprintf("SELECT id FROM range(%d) AS t(id)", rows), dest, CopyOptions{Format: format, Compression: compression, PublishMode: mode})
					if err != nil {
						_ = db.Close()
						t.Fatal(err)
					}
					if result.Rows != int64(rows) || len(result.JobID) != 32 {
						t.Fatal(result)
					}
					b, ok := store.Get("/result")
					if !ok || len(b) == 0 {
						t.Fatal("format representation missing")
					}
					sum := sha256.Sum256(b)
					if rows > 0 {
						args := "'" + dest + "'"
						if format == CopyFormatCSV {
							args += ", header=true"
						}
						_, err = db.QueryChunks(context.Background(), "SELECT CAST(sum(id) AS BIGINT) FROM read_"+string(format)+"("+args+")", func(c *Chunk) error {
							value, ok, e := c.Int64(0, 0)
							if e != nil || !ok || value != 1 {
								return fmt.Errorf("sum=%d ok=%t err=%v", value, ok, e)
							}
							return nil
						})
						if err != nil {
							_ = db.Close()
							t.Fatal(err)
						}
					}
					if db.MemoryUsed() != 0 {
						t.Fatalf("managed leak %d", db.MemoryUsed())
					}
					if err = db.Close(); err != nil {
						t.Fatal(err)
					}
					db, err = Open(cfg)
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()
					for n := 0; n < 2; n++ {
						r, e := db.RecoverExport(context.Background(), result.JobID)
						if e != nil {
							t.Fatal(e)
						}
						if r.Checksum != hex.EncodeToString(sum[:]) || r.Bytes != int64(len(b)) || r.PublishMode != mode {
							t.Fatal(r)
						}
					}
					if db.MemoryUsed() != 0 || store.StagingCount() != 0 {
						t.Fatal("resource leak")
					}
					raw, e := os.ReadFile(filepath.Join(cfg.ManifestDir, result.JobID+".json"))
					if e != nil {
						t.Fatal(e)
					}
					var m exportpkg.Manifest
					if json.Unmarshal(raw, &m) != nil || m.ManifestVersion != 2 || m.ExportState != exportpkg.ExportStateCommitted || m.Publish.CleanupPending {
						t.Fatal("manifest not committed")
					}
					if strings.Contains(string(raw), "password") || strings.Contains(string(raw), "fixture_known_hosts") {
						t.Fatal("credential configuration leaked")
					}
				})
			}
		}
	}
}

func TestRemoteSQLCopyZeroPhysicalBytes(t *testing.T) {
	for _, kind := range remoteExportModes {
		for _, format := range []CopyFormat{CopyFormatCSV, CopyFormatJSON, CopyFormatNDJSON, CopyFormatParquet} {
			t.Run(kind+"/"+string(format), func(t *testing.T) {
				store := filetest.New()
				cfg, dest, mode := remoteExportConfig(t, kind, store)
				db, e := OpenSQLDB(cfg)
				if e != nil {
					t.Fatal(e)
				}
				defer db.Close()
				opts := "FORMAT " + string(format) + ", PUBLISH_MODE '" + string(mode) + "'"
				if format == CopyFormatCSV {
					opts += ", HEADER false"
				}
				if _, e = db.ExecContext(context.Background(), "COPY (SELECT 1 AS id WHERE false) TO '"+dest+"' ("+opts+")"); e != nil {
					t.Fatal(e)
				}
				b, ok := store.Get("/result")
				if !ok {
					t.Fatal("missing zero-row final")
				}
				if format == CopyFormatCSV || format == CopyFormatNDJSON {
					if len(b) != 0 {
						t.Fatalf("expected physical zero, got %q", b)
					}
				} else if len(b) == 0 {
					t.Fatal("missing format tail")
				}
			})
		}
	}
}

func TestRemotePublicUncertainRecoveryWithoutQuery(t *testing.T) {
	for _, kind := range remoteExportModes {
		t.Run(kind, func(t *testing.T) {
			store := filetest.New()
			cfg, dest, mode := remoteExportConfig(t, kind, store)
			db, e := Open(cfg)
			if e != nil {
				t.Fatal(e)
			}
			store.SetFault("publish_lost")
			_, e = db.CopyTo(context.Background(), "SELECT 42 AS id", dest, CopyOptions{Format: CopyFormatCSV, PublishMode: mode})
			var uncertain *CommitUncertainError
			if !errors.Is(e, ErrCommitUncertain) || !errors.As(e, &uncertain) || len(uncertain.JobID) != 32 {
				_ = db.Close()
				t.Fatal(e)
			}
			if db.MemoryUsed() != 0 {
				t.Fatal("uncertain result leaked managed memory")
			}
			_ = db.Close()
			store.SetFault("")
			db, e = Open(cfg)
			if e != nil {
				t.Fatal(e)
			}
			defer db.Close()
			if _, e = db.RecoverExport(context.Background(), uncertain.JobID); e != nil {
				t.Fatal(e)
			}
			if db.MemoryUsed() != 0 {
				t.Fatal("recovery memory leak")
			}
			// New source-independent scope can only verify an already committed
			// target; missing final must never cause SQL execution/republication.
			store.Remove("/result")
			if _, e = db.RecoverExport(context.Background(), uncertain.JobID); !errors.Is(e, ErrExportNotRecoverable) {
				t.Fatal(e)
			}
		})
	}
}

func TestRemoteRecoveryUsesCurrentCredentialScope(t *testing.T) {
	for _, kind := range remoteExportModes {
		t.Run(kind, func(t *testing.T) {
			store := filetest.New()
			cfg, dest, mode := remoteExportConfig(t, kind, store)
			db, e := Open(cfg)
			if e != nil {
				t.Fatal(e)
			}
			r, e := db.CopyTo(context.Background(), "SELECT 1 AS id", dest, CopyOptions{Format: CopyFormatCSV, PublishMode: mode})
			_ = db.Close()
			if e != nil {
				t.Fatal(e)
			}
			if cfg.FTP != nil {
				cfg.FTP.Password = "changed"
			}
			if cfg.SFTP != nil {
				cfg.SFTP.Username = "changed"
			}
			if cfg.WebDAV != nil {
				cfg.WebDAV.Password = "changed"
			}
			db, e = Open(cfg)
			if e != nil {
				t.Fatal(e)
			}
			defer db.Close()
			if _, e = db.RecoverExport(context.Background(), r.JobID); e == nil {
				t.Fatal("changed credential scope accepted")
			}
			b, ok := store.Get("/result")
			if !ok || !strings.Contains(string(b), "1") {
				t.Fatal("reauthorization changed final")
			}
			if db.MemoryUsed() != 0 {
				t.Fatal("authorization failure leaked memory")
			}
		})
	}
}

func TestRemoteWebDAVSTLSCopyAndRecovery(t *testing.T) {
	for _, kind := range []string{"webdavs/strict", "webdavs/compatible"} {
		t.Run(kind, func(t *testing.T) {
			store := filetest.New()
			cfg, dest, mode := remoteExportConfig(t, kind, store)
			db, e := Open(cfg)
			if e != nil {
				t.Fatal(e)
			}
			defer db.Close()
			r, e := db.CopyTo(context.Background(), "SELECT 7 AS id", dest, CopyOptions{Format: CopyFormatParquet, PublishMode: mode})
			if e != nil {
				t.Fatal(e)
			}
			if _, e = db.RecoverExport(context.Background(), r.JobID); e != nil {
				t.Fatal(e)
			}
			if db.MemoryUsed() != 0 {
				t.Fatal("TLS copy leaked memory")
			}
		})
	}
}

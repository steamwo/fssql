package gosql_test

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/steamwo/fssql/pkg/gosql"
)

const remoteCSV = "id\n7\n11\n"

func checkRemotePublicQueries(t *testing.T, config gosql.Config, uri string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := gosql.Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var tlsConfig *tls.Config
	if config.FTP != nil {
		tlsConfig = config.FTP.TLSConfig
	} else if config.WebDAV != nil {
		tlsConfig = config.WebDAV.TLSConfig
	}
	// Mutating the caller's TLS config after Open must not change this DB.
	if tlsConfig != nil {
		original := tlsConfig.ServerName
		tlsConfig.ServerName = "wrong.example.invalid"
		defer func() { tlsConfig.ServerName = original }()
	}
	var total int64
	_, err = db.QueryChunks(ctx, "SELECT CAST(sum(id) AS BIGINT) FROM read_csv('"+uri+"', header=true)", func(chunk *gosql.Chunk) error {
		value, valid, err := chunk.Int64(0, 0)
		if err != nil {
			return err
		}
		if !valid {
			return fmt.Errorf("NULL sum")
		}
		total += value
		return nil
	})
	if err != nil || total != 18 {
		t.Fatalf("native sum=%d error=%v", total, err)
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("managed memory=%d", used)
	}
	if tlsConfig != nil {
		tlsConfig.ServerName = ""
	}
	sqlDB, err := gosql.OpenSQLDB(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	// Closing a physical SQL connection must not close the shared remote pool.
	for i := 0; i < 2; i++ {
		conn, err := sqlDB.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		err = conn.QueryRowContext(ctx, "SELECT CAST(sum(id) AS BIGINT) FROM read_csv(?, header=true)", uri).Scan(&total)
		conn.Close()
		if err != nil || total != 18 {
			t.Fatalf("database/sql sum=%d error=%v", total, err)
		}
		sqlDB.SetMaxIdleConns(0)
	}
	// Read-only fixtures and unverified strict endpoints reject publication.
	err = db.Exec(ctx, "COPY (SELECT 1 AS id) TO '"+uri+".out.csv' (FORMAT csv)")
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("expected publication rejection, got %v", err)
	}
}

func TestPublicWebDAVQueries(t *testing.T) {
	for _, secure := range []bool{false, true} {
		scheme := "webdav"
		if secure {
			scheme = "webdavs"
		}
		t.Run(scheme, func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				user, password, ok := r.BasicAuth()
				if !ok || user != "user" || password != "pass" {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				if r.Method == http.MethodOptions {
					w.Header().Set("Allow", "OPTIONS, HEAD, GET")
					w.WriteHeader(http.StatusNoContent)
					return
				}
				w.Header().Set("ETag", `"fixture"`)
				http.ServeContent(w, r, "data.csv", time.Unix(1234, 0), strings.NewReader(remoteCSV))
			})
			server := httptest.NewUnstartedServer(handler)
			if secure {
				server.StartTLS()
			} else {
				server.Start()
			}
			defer server.Close()
			endpoint := scheme + "://" + server.Listener.Addr().String()
			config := &gosql.WebDAVConfig{Endpoint: endpoint, Username: "user", Password: "pass", Network: gosql.RemoteNetworkConfig{AllowLoopback: true}}
			if secure {
				roots := x509.NewCertPool()
				roots.AddCert(server.Certificate())
				config.TLSConfig = &tls.Config{RootCAs: roots}
			}
			checkRemotePublicQueries(t, gosql.Config{WebDAV: config}, endpoint+"/data.csv")
			for _, failure := range []string{"network", "credentials", "certificate"} {
				if failure == "certificate" && !secure {
					continue
				}
				bad := *config
				switch failure {
				case "network":
					bad.Network.AllowLoopback = false
				case "credentials":
					bad.Password = "wrong"
				case "certificate":
					bad.TLSConfig = nil
				}
				db, err := gosql.OpenSQLDB(gosql.Config{WebDAV: &bad})
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				var n int
				err = db.QueryRowContext(ctx, "SELECT count(*) FROM read_csv(?)", endpoint+"/data.csv").Scan(&n)
				cancel()
				db.Close()
				if err == nil {
					t.Fatalf("%s: expected failure", failure)
				}
			}
		})
	}
}

func TestPublicFTPQueries(t *testing.T) {
	for _, secure := range []bool{false, true} {
		scheme := "ftp"
		if secure {
			scheme = "ftps"
		}
		t.Run(scheme, func(t *testing.T) {
			// Reuse httptest's locally generated certificate for both FTPS channels.
			certServer := httptest.NewTLSServer(http.NotFoundHandler())
			defer certServer.Close()
			roots := x509.NewCertPool()
			roots.AddCert(certServer.Certificate())
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			if secure {
				listener = tls.NewListener(listener, certServer.TLS.Clone())
			}
			defer listener.Close()
			go func() {
				for {
					conn, err := listener.Accept()
					if err != nil {
						return
					}
					go servePublicFTP(conn, secure, certServer.TLS)
				}
			}()
			endpoint := scheme + "://" + listener.Addr().String()
			config := gosql.Config{FTP: &gosql.FTPConfig{Endpoint: endpoint, Username: "user", Password: "pass", AllowPlainFTP: !secure, Network: gosql.RemoteNetworkConfig{AllowLoopback: true}, TLSConfig: &tls.Config{RootCAs: roots}}}
			checkRemotePublicQueries(t, config, endpoint+"/data.csv")
		})
	}
}

// servePublicFTP implements the subset used by the public CSV reader over real
// control/data sockets, including implicit TLS and authenticated login.
func servePublicFTP(conn net.Conn, secure bool, tlsConfig *tls.Config) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	_, _ = io.WriteString(conn, "220 ready\r\n")
	reader := bufio.NewScanner(conn)
	var data net.Listener
	defer func() {
		if data != nil {
			data.Close()
		}
	}()
	offset := 0
	for reader.Scan() {
		command, arg, _ := strings.Cut(reader.Text(), " ")
		response := "200 OK\r\n"
		switch command {
		case "USER":
			if arg != "user" {
				return
			}
			response = "331 password\r\n"
		case "PASS":
			if arg != "pass" {
				return
			}
			response = "230 logged in\r\n"
		case "FEAT":
			response = "211-Features\r\n REST STREAM\r\n211 End\r\n"
		case "SIZE":
			response = fmt.Sprintf("213 %d\r\n", len(remoteCSV))
		case "MDTM":
			response = "213 20260101000000\r\n"
		case "REST":
			offset, _ = strconv.Atoi(arg)
			response = "350 restart\r\n"
		case "EPSV":
			var err error
			data, err = net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				return
			}
			response = fmt.Sprintf("229 Entering Extended Passive Mode (|||%d|)\r\n", data.Addr().(*net.TCPAddr).Port)
		case "RETR":
			_, _ = io.WriteString(conn, "150 opening\r\n")
			stream, err := data.Accept()
			data.Close()
			data = nil
			if err != nil {
				return
			}
			_ = stream.SetDeadline(time.Now().Add(10 * time.Second))
			if secure {
				stream = tls.Server(stream, tlsConfig.Clone())
			}
			if offset < 0 || offset > len(remoteCSV) {
				stream.Close()
				return
			}
			_, _ = io.WriteString(stream, remoteCSV[offset:])
			stream.Close()
			response = "226 complete\r\n"
		case "QUIT":
			_, _ = io.WriteString(conn, "221 bye\r\n")
			return
		}
		if _, err := io.WriteString(conn, response); err != nil {
			return
		}
	}
}

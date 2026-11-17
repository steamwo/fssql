package gosql

import (
	"context"

	"errors"
	"strings"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/storage/remote/ftp"
	"github.com/steamwo/fssql/internal/storage/remote/sftp"
	"github.com/steamwo/fssql/internal/storage/remote/webdav"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func TestRemoteProtocolConfiguration(t *testing.T) {
	for _, scheme := range []string{"ftp", "ftps", "sftp", "webdav", "webdavs"} {
		t.Run(scheme, func(t *testing.T) {
			endpoint := scheme + "://example.com:1234"
			config := Config{}
			switch scheme {
			case "ftp", "ftps":
				config.FTP = &FTPConfig{Endpoint: endpoint, Username: "user", Password: "password", AllowPlainFTP: true}
			case "sftp":
				config.SFTP = &SFTPConfig{Endpoint: endpoint, Username: "user", KnownHostsFile: "known_hosts"}
			default:
				config.WebDAV = &WebDAVConfig{Endpoint: endpoint, Username: "user", Password: "password"}
			}
			db, err := Open(config) // Configuration must not dial or resolve DNS.
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if len(db.remoteResources.drivers) != 1 || db.remoteResources.drivers[0].Scheme() != scheme {
				t.Fatalf("configured drivers: %v", db.remoteResources.drivers)
			}
			driver := db.remoteResources.drivers[0]
			uri, _ := vfs.ParseURI(scheme + "://other.example.com:1234/data.csv")
			if _, err := driver.Stat(context.Background(), uri); err == nil || !strings.Contains(err.Error(), "endpoint mismatch") {
				t.Fatalf("endpoint isolation: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			// Closing a database must close its actual pool, rather than a manager
			// which could recreate that pool on a subsequent lookup.
			var poolErr error
			switch d := driver.(type) {
			case *ftp.Driver:
				pool, err := d.Pools.Pool(d.Key)
				if err != nil {
					t.Fatal(err)
				}
				_, poolErr = pool.Acquire(context.Background())
				if !errors.Is(poolErr, ftp.ErrPoolClosed) {
					t.Fatalf("pool: %v", poolErr)
				}
			case *sftp.Driver:
				pool, err := d.Pools.Pool(d.Key)
				if err != nil {
					t.Fatal(err)
				}
				_, poolErr = pool.Acquire(context.Background())
				if !errors.Is(poolErr, sftp.ErrPoolClosed) {
					t.Fatalf("pool: %v", poolErr)
				}
			case *webdav.Driver:
				if d.Clients.Count() != 1 {
					t.Fatal("missing HTTP client")
				}
			}
		})
	}
}

func TestRemoteProtocolsRemainDisabledByDefault(t *testing.T) {
	db, err := Open(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, scheme := range []string{"ftp", "ftps", "sftp", "webdav", "webdavs"} {
		_, err := db.QueryChunks(context.Background(), "SELECT * FROM read_csv('"+scheme+"://example.com/data.csv')", func(*Chunk) error { return nil })
		if err == nil || !strings.Contains(err.Error(), vfs.ErrUnsupportedScheme.Error()) {
			t.Fatalf("%s: %v", scheme, err)
		}
	}
}

func TestOpenRejectsInvalidRemoteConfiguration(t *testing.T) {
	for name, config := range map[string]Config{
		"plain FTP":             {FTP: &FTPConfig{Endpoint: "ftp://example.com", Username: "user"}},
		"FTP username":          {FTP: &FTPConfig{Endpoint: "ftps://example.com"}},
		"FTP command injection": {FTP: &FTPConfig{Endpoint: "ftps://example.com", Username: "user", Password: "secret\r\nQUIT"}},
		"SFTP username":         {SFTP: &SFTPConfig{Endpoint: "sftp://example.com", KnownHostsFile: "known_hosts"}},
		"SFTP known hosts":      {SFTP: &SFTPConfig{Endpoint: "sftp://example.com", Username: "user"}},
		"SFTP option injection": {SFTP: &SFTPConfig{Endpoint: "sftp://example.com", Username: "-oProxyCommand=x", KnownHostsFile: "known_hosts"}},
		"WebDAV username":       {WebDAV: &WebDAVConfig{Endpoint: "webdavs://example.com"}},
		"WebDAV Basic auth":     {WebDAV: &WebDAVConfig{Endpoint: "webdavs://example.com", Username: "user:other"}},
	} {
		t.Run(name, func(t *testing.T) {
			db, err := Open(config)
			if err == nil {
				db.Close()
				t.Fatal("expected configuration error")
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("credential in error: %v", err)
			}
		})
	}
	for _, endpoint := range []string{"", "example.com", "https://example.com", "ftps://user:secret@example.com", "ftps://example.com/data", "ftps://example.com?secret", "ftps://example.com#secret", "ftps://example.com:0", "ftps://example.com:65536", "ftps://example.com:"} {
		db, err := Open(Config{FTP: &FTPConfig{Endpoint: endpoint, Username: "user"}})
		if err == nil {
			db.Close()
			t.Fatalf("accepted endpoint %q", endpoint)
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatalf("credential in error: %v", err)
		}
	}
}

func TestRemoteResourcesCloseOnce(t *testing.T) {
	calls := 0
	want := errors.New("close failed")
	r := &remoteResources{closers: []func() error{func() error { calls++; return want }, func() error { calls++; return nil }}}
	for i := 0; i < 2; i++ {
		if err := r.Close(); !errors.Is(err, want) {
			t.Fatal(err)
		}
	}
	if calls != 2 {
		t.Fatalf("close calls=%d", calls)
	}
}

func TestRemoteNetworkPolicyDefaults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	u, _, err := publicRemoteEndpoint("webdav://127.0.0.1:1234", "webdav")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publicRemotePolicy(u, RemoteNetworkConfig{}, 80).ResolveURL(ctx, u.String()); err == nil {
		t.Fatal("loopback allowed by default")
	}
	p := publicRemotePolicy(u, RemoteNetworkConfig{AllowLoopback: true}, 80)
	if _, err := p.ResolveURL(ctx, u.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ResolveURL(ctx, "http://127.0.0.1:5678"); err == nil {
		t.Fatal("unconfigured port allowed")
	}
}

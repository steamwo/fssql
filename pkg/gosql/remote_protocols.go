package gosql

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/steamwo/fssql/internal/security"
	"github.com/steamwo/fssql/internal/storage/remote"
	"github.com/steamwo/fssql/internal/storage/remote/ftp"
	"github.com/steamwo/fssql/internal/storage/remote/sftp"
	"github.com/steamwo/fssql/internal/storage/remote/webdav"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// RemoteNetworkConfig controls access to non-public FTP, WebDAV and HTTP addresses.
// Zero values reject private, loopback and link-local addresses. Connections
// are restricted to the port in Endpoint (or the protocol's default port).
type RemoteNetworkConfig struct {
	AllowPrivate   bool
	AllowLoopback  bool
	AllowLinkLocal bool
}

// FTPConfig enables one FTP or implicit-TLS FTPS endpoint.
type FTPConfig struct {
	// Endpoint is ftp://host[:port] or ftps://host[:port], without a data path.
	Endpoint      string
	Username      string
	Password      string
	AllowPlainFTP bool
	Network       RemoteNetworkConfig
	TLSConfig     *tls.Config
	// MaxSessions defaults to 4; MaxAttempts defaults to 2.
	MaxSessions int
	MaxAttempts int
}

// SFTPConfig enables one endpoint through the system OpenSSH client.
// Authentication uses IdentityFile or the SSH agent; interactive login is disabled.
type SFTPConfig struct {
	// Endpoint is sftp://host[:port], without credentials or a data path.
	Endpoint       string
	Username       string
	KnownHostsFile string
	IdentityFile   string
	// SSHBinary defaults to "ssh". Host-key verification is always strict.
	SSHBinary      string
	ConnectTimeout time.Duration
	// MaxSessions defaults to 4; MaxAttempts defaults to 2.
	MaxSessions int
	MaxAttempts int
}

// WebDAVConfig enables one WebDAV endpoint using HTTP Basic authentication.
type WebDAVConfig struct {
	// AtomicMoveVerified asserts that this deployment provides atomic MOVE
	// visibility and honors Overwrite:F under concurrent destination creation.
	// Default concurrent Apache does not satisfy this assertion.
	// Required for strict COPY publication.
	AtomicMoveVerified bool
	// Endpoint is webdav://host[:port] or webdavs://host[:port], without a data path.
	Endpoint  string
	Username  string
	Password  string
	Network   RemoteNetworkConfig
	TLSConfig *tls.Config
}

type remoteResources struct {
	drivers []vfs.Driver
	closers []func() error
	once    sync.Once
	err     error
}

func (r *remoteResources) Close() error {
	if r == nil {
		return nil
	}
	r.once.Do(func() {
		for _, close := range r.closers {
			r.err = errors.Join(r.err, close())
		}
	})
	return r.err
}

func publicRemoteDrivers(config Config) (r *remoteResources, err error) {
	r = &remoteResources{}
	defer func() {
		if err != nil {
			err = errors.Join(err, r.Close())
		}
	}()
	if len(config.HTTP) > 0 {
		drivers, close, e := publicHTTPDrivers(config.HTTP)
		if e != nil {
			return r, e
		}
		r.drivers = append(r.drivers, drivers...)
		r.closers = append(r.closers, close)
	}
	if config.FTP != nil {
		driver, close, e := publicFTPDriver(*config.FTP)
		if e != nil {
			return r, e
		}
		r.drivers = append(r.drivers, driver)
		r.closers = append(r.closers, close)
	}
	if config.SFTP != nil {
		driver, close, e := publicSFTPDriver(*config.SFTP)
		if e != nil {
			return r, e
		}
		r.drivers = append(r.drivers, driver)
		r.closers = append(r.closers, close)
	}
	if config.WebDAV != nil {
		driver, close, e := publicWebDAVDriver(*config.WebDAV)
		if e != nil {
			return r, e
		}
		r.drivers = append(r.drivers, driver)
		r.closers = append(r.closers, close)
	}
	return r, nil
}

// Parse only endpoint URLs: silently discarding paths or URL credentials would
// give callers a different access/authentication boundary than they configured.
func publicRemoteEndpoint(raw string, schemes ...string) (*url.URL, remote.ClientKey, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, remote.ClientKey{}, errors.New("gosql: invalid remote endpoint URL")
	}
	validScheme := false
	for _, scheme := range schemes {
		validScheme = validScheme || u.Scheme == scheme
	}
	if !validScheme || u.Hostname() == "" || u.User != nil || u.Opaque != "" ||
		(u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return nil, remote.ClientKey{}, fmt.Errorf("gosql: remote endpoint must be %s://host[:port] without credentials, path, query or fragment", strings.Join(schemes, " or "))
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return nil, remote.ClientKey{}, errors.New("gosql: invalid remote endpoint port")
		}
	} else if strings.HasSuffix(u.Host, ":") {
		return nil, remote.ClientKey{}, errors.New("gosql: empty remote endpoint port")
	}
	return u, remote.ClientKey{Endpoint: u.Host, CredentialScope: "gosql", CredentialVersion: "1", SecurityProfile: "gosql"}, nil
}

func publicRemotePolicy(u *url.URL, config RemoteNetworkConfig, defaultPort int) *security.NetworkPolicy {
	port := defaultPort
	if u.Port() != "" {
		port, _ = strconv.Atoi(u.Port()) // publicRemoteEndpoint validated it.
	}
	p := security.NewNetworkPolicy(port)
	p.AllowPrivate = config.AllowPrivate
	p.AllowLoopback = config.AllowLoopback
	p.AllowLinkLocal = config.AllowLinkLocal
	return p
}

func publicFTPDriver(config FTPConfig) (vfs.Driver, func() error, error) {
	u, key, err := publicRemoteEndpoint(config.Endpoint, "ftp", "ftps")
	if err != nil {
		return nil, nil, err
	}
	if config.Username == "" || strings.ContainsAny(config.Username+config.Password, "\r\n\x00") {
		return nil, nil, errors.New("gosql: FTP requires a username and credentials without CR/LF/NUL")
	}
	if u.Scheme == "ftp" && !config.AllowPlainFTP {
		return nil, nil, security.ErrPlainFTPForbidden
	}
	port := 21
	if u.Scheme == "ftps" {
		port = 990
	}
	policy := publicRemotePolicy(u, config.Network, port)
	policy.AllowPlainFTP = config.AllowPlainFTP
	credentials := ftp.Credentials{Username: config.Username, Password: config.Password}
	key.CredentialVersion = remote.PublishScope(config.Username, config.Password)
	factory := &ftp.WireFactory{Scheme: u.Scheme, Policy: policy, TLSConfig: cloneRemoteTLS(config.TLSConfig), Credentials: ftp.CredentialProviderFunc(func(context.Context, remote.ClientKey, string) (ftp.Credentials, error) {
		return credentials, nil
	})}
	pools := ftp.NewManager(1, config.MaxSessions, factory)
	pool, err := pools.Pool(key)
	if err != nil {
		return nil, nil, err
	}
	driver, err := ftp.NewDriver(u.Scheme, key, pools, config.MaxAttempts)
	return driver, pool.Close, err
}

func publicSFTPDriver(config SFTPConfig) (vfs.Driver, func() error, error) {
	_, key, err := publicRemoteEndpoint(config.Endpoint, "sftp")
	if err != nil {
		return nil, nil, err
	}
	ssh := sftp.OpenSSHConfig{Binary: config.SSHBinary, User: config.Username, KnownHostsFile: config.KnownHostsFile, IdentityFile: config.IdentityFile, ConnectTimeout: config.ConnectTimeout}
	if err := ssh.Validate(); err != nil {
		return nil, nil, err
	}
	if strings.HasPrefix(config.Username, "-") || strings.ContainsAny(config.Username, "@\r\n\x00") {
		return nil, nil, errors.New("gosql: invalid SFTP username")
	}
	key.CredentialVersion = remote.PublishScope(config.Username, config.KnownHostsFile, config.IdentityFile, config.SSHBinary)
	pools := sftp.NewManager(1, config.MaxSessions, sftp.OpenSSHFactory{Config: ssh})
	pool, err := pools.Pool(key)
	if err != nil {
		return nil, nil, err
	}
	driver, err := sftp.NewDriver(key, pools, config.MaxAttempts)
	return driver, pool.Close, err
}

func publicWebDAVDriver(config WebDAVConfig) (vfs.Driver, func() error, error) {
	u, key, err := publicRemoteEndpoint(config.Endpoint, "webdav", "webdavs")
	if err != nil {
		return nil, nil, err
	}
	if config.Username == "" || strings.Contains(config.Username, ":") {
		return nil, nil, errors.New("gosql: WebDAV requires a username without ':'")
	}
	port := 80
	if u.Scheme == "webdavs" {
		port = 443
	}
	policy := publicRemotePolicy(u, config.Network, port)
	clients, err := webdav.NewClientManager(1, u.Scheme, policy, cloneRemoteTLS(config.TLSConfig))
	if err != nil {
		return nil, nil, err
	}
	client, err := clients.Client(key)
	if err != nil {
		return nil, nil, err
	}
	credentials := webdav.Credentials{Username: config.Username, Password: config.Password}
	driver, err := webdav.NewDriver(u.Scheme, key, clients, webdav.CredentialProviderFunc(func(context.Context, remote.ClientKey, string) (webdav.Credentials, error) {
		return credentials, nil
	}), policy, 0)
	if driver != nil {
		driver.AtomicMoveVerified = config.AtomicMoveVerified
	}
	return driver, func() error { client.CloseIdleConnections(); return nil }, err
}

func cloneRemoteTLS(config *tls.Config) *tls.Config {
	if config == nil {
		return nil
	}
	return config.Clone()
}

package ftp

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/steamwo/fssql/internal/security"
	"github.com/steamwo/fssql/internal/storage/remote"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// Credentials 保存 FTP 登录所需的最小 credential 集合。
type Credentials struct {
	// Username 是 USER 命令使用的账号。
	Username string
	// Password 是 PASS 命令使用的密码。
	Password string
}

// CredentialProvider 根据 endpoint-scoped ClientKey 返回对应版本的 credential。
type CredentialProvider interface {
	Credentials(context.Context, remote.ClientKey, string) (Credentials, error)
}

// CredentialProviderFunc 允许函数直接实现 CredentialProvider。
type CredentialProviderFunc func(context.Context, remote.ClientKey, string) (Credentials, error)

// Credentials 实现 CredentialProvider。
func (f CredentialProviderFunc) Credentials(ctx context.Context, key remote.ClientKey, scheme string) (Credentials, error) {
	return f(ctx, key, scheme)
}

// SecretCredentialProvider 把 Security SecretManager 适配到 FTP credential provider。
type SecretCredentialProvider struct {
	// Manager 提供 scoped secret ACL 与 credential rotation。
	Manager *security.SecretManager
	// Subject 是本连接代表的认证主体。
	Subject string
}

// Credentials 解析 ClientKey.CredentialScope 指定的 secret，并强制版本与 ClientKey 一致。
func (p SecretCredentialProvider) Credentials(ctx context.Context, key remote.ClientKey, scheme string) (Credentials, error) {
	if p.Manager == nil {
		return Credentials{}, fmt.Errorf("%w: secret manager required", vfs.ErrCredential)
	}
	if err := ctx.Err(); err != nil {
		return Credentials{}, err
	}
	target := scheme + "://" + key.Endpoint + "/"
	secret, err := p.Manager.Resolve(key.CredentialScope, p.Subject, target)
	if err != nil {
		return Credentials{}, fmt.Errorf("%w: %v", vfs.ErrCredential, err)
	}
	if secret.VersionString() != key.CredentialVersion {
		return Credentials{}, fmt.Errorf("%w: credential version changed: requested=%s current=%s", vfs.ErrCredential, key.CredentialVersion, secret.VersionString())
	}
	username, ok := secret.Value("username")
	if !ok || username == "" {
		return Credentials{}, fmt.Errorf("%w: username missing", vfs.ErrCredential)
	}
	password, ok := secret.Value("password")
	if !ok {
		return Credentials{}, fmt.Errorf("%w: password missing", vfs.ErrCredential)
	}
	return Credentials{Username: username, Password: password}, nil
}

// DialContextFunc 抽象 TCP 拨号；生产默认使用 net.Dialer，测试可注入确定性连接。
type DialContextFunc func(context.Context, string, string) (net.Conn, error)

// WireFactory 创建真实 FTP/FTPS protocol session。
type WireFactory struct {
	// Scheme 只能是 ftp 或 ftps；ftps 使用 implicit TLS。
	Scheme string
	// Policy 在任何网络拨号前执行 SSRF、端口和 plain FTP 策略。
	Policy *security.NetworkPolicy
	// Credentials 提供与 ClientKey.CredentialVersion 对齐的 credential。
	Credentials CredentialProvider
	// TLSConfig 是 FTPS TLS 基础配置；每次连接都会 Clone 并设置 ServerName。
	TLSConfig *tls.Config
	// DialContext 可选；nil 时使用 net.Dialer.DialContext。
	DialContext DialContextFunc
}

// Dial 创建登录后的 binary-mode FTP/FTPS Session。
func (f *WireFactory) Dial(ctx context.Context, key remote.ClientKey) (Session, error) {
	if f == nil {
		return nil, errors.New("ftp: nil wire factory")
	}
	scheme := strings.ToLower(f.Scheme)
	if scheme != "ftp" && scheme != "ftps" {
		return nil, fmt.Errorf("ftp: unsupported wire scheme %q", scheme)
	}
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if f.Policy == nil {
		return nil, errors.New("ftp: NetworkPolicy required")
	}
	if f.Credentials == nil {
		return nil, errors.New("ftp: credential provider required")
	}
	credentials, err := f.Credentials.Credentials(ctx, key, scheme)
	if err != nil {
		return nil, err
	}
	endpoint, err := f.Policy.ResolveURL(ctx, scheme+"://"+key.Endpoint+"/")
	if err != nil {
		return nil, err
	}
	addresses := endpoint.DialAddresses()
	if len(addresses) == 0 {
		return nil, fmt.Errorf("%w: no validated dial addresses", security.ErrEndpointDenied)
	}

	dial := f.DialContext
	if dial == nil {
		dialer := &net.Dialer{}
		dial = dialer.DialContext
	}
	var raw net.Conn
	var selectedIP net.IP
	var last error
	for index, address := range addresses {
		raw, err = dial(ctx, "tcp", address)
		if err == nil {
			selectedIP = append(net.IP(nil), endpoint.IPs[index]...)
			break
		}
		last = err
	}
	if raw == nil {
		return nil, fmt.Errorf("%w: dial endpoint: %v", ErrConnection, last)
	}

	control := raw
	tlsConfig := cloneTLSConfig(f.TLSConfig, endpoint.URL.Hostname())
	if scheme == "ftps" {
		tlsConn := tls.Client(raw, tlsConfig)
		if err := handshakeTLS(ctx, tlsConn); err != nil {
			_ = raw.Close()
			return nil, fmt.Errorf("%w: ftps handshake: %v", ErrConnection, err)
		}
		control = tlsConn
	}

	session := &wireSession{
		scheme: scheme, control: control, reader: bufio.NewReaderSize(control, maxControlLine), writer: bufio.NewWriter(control),
		selectedIP: selectedIP, serverName: endpoint.URL.Hostname(), tlsConfig: tlsConfig, dial: dial,
	}
	if err := session.bootstrap(ctx, credentials); err != nil {
		_ = session.Close()
		return nil, err
	}
	return session, nil
}

// cloneTLSConfig 为每个 endpoint 创建独立 TLS 配置，默认最低 TLS 1.2。
func cloneTLSConfig(base *tls.Config, serverName string) *tls.Config {
	var cfg *tls.Config
	if base == nil {
		cfg = &tls.Config{}
	} else {
		cfg = base.Clone()
	}
	cfg.ServerName = serverName
	if cfg.MinVersion == 0 {
		cfg.MinVersion = tls.VersionTLS12
	}
	return cfg
}

// handshakeTLS 对 TLS 握手应用 context cancellation/deadline。
func handshakeTLS(ctx context.Context, conn *tls.Conn) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return conn.HandshakeContext(ctx)
}

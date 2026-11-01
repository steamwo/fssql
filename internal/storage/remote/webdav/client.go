package webdav

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/steamwo/fssql/internal/security"
	"github.com/steamwo/fssql/internal/storage/remote"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// Credentials 保存 WebDAV Basic Auth 所需的 credential。
type Credentials struct {
	// Username 是 HTTP Basic Auth 用户名。
	Username string
	// Password 是 HTTP Basic Auth 密码。
	Password string
}

// CredentialProvider 按 endpoint-scoped ClientKey 返回对应版本的 credential。
type CredentialProvider interface {
	Credentials(context.Context, remote.ClientKey, string) (Credentials, error)
}

// CredentialProviderFunc 允许函数直接实现 CredentialProvider。
type CredentialProviderFunc func(context.Context, remote.ClientKey, string) (Credentials, error)

// Credentials 实现 CredentialProvider。
func (f CredentialProviderFunc) Credentials(ctx context.Context, key remote.ClientKey, scheme string) (Credentials, error) {
	return f(ctx, key, scheme)
}

// SecretCredentialProvider 把 Security SecretManager 适配到 WebDAV credential provider。
type SecretCredentialProvider struct {
	// Manager 提供 scoped secret ACL 与 credential rotation。
	Manager *security.SecretManager
	// Subject 是本连接代表的认证主体。
	Subject string
}

// Credentials 解析 ClientKey.CredentialScope，并拒绝已轮换的旧 credential version。
func (p SecretCredentialProvider) Credentials(ctx context.Context, key remote.ClientKey, scheme string) (Credentials, error) {
	if p.Manager == nil {
		return Credentials{}, fmt.Errorf("%w: secret manager required", vfs.ErrCredential)
	}
	if err := ctx.Err(); err != nil {
		return Credentials{}, err
	}
	secret, err := p.Manager.Resolve(key.CredentialScope, p.Subject, scheme+"://"+key.Endpoint+"/")
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

// NewClientManager 创建 AD-030 要求的 endpoint-scoped HTTP client 复用器。
// 每次 TCP 拨号都会重新经过 NetworkPolicy，并只使用本次校验绑定的 IP。
func NewClientManager(maxClients int, scheme string, policy *security.NetworkPolicy, tlsConfig *tls.Config) (*remote.ClientManager, error) {
	scheme = strings.ToLower(scheme)
	transportScheme, err := transportSchemeFor(scheme)
	if err != nil {
		return nil, err
	}
	if policy == nil {
		return nil, errors.New("webdav: NetworkPolicy required")
	}
	factory := func(key remote.ClientKey) (*http.Client, error) {
		if err := key.Validate(); err != nil {
			return nil, err
		}
		transport := &http.Transport{
			Proxy:                 nil,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          64,
			MaxIdleConnsPerHost:   8,
			MaxConnsPerHost:       16,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
		}
		if tlsConfig != nil {
			transport.TLSClientConfig = tlsConfig.Clone()
		}
		dialer := &net.Dialer{}
		transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			endpoint, err := policy.ResolveURL(ctx, transportScheme+"://"+address+"/")
			if err != nil {
				return nil, err
			}
			var last error
			for _, target := range endpoint.DialAddresses() {
				conn, dialErr := dialer.DialContext(ctx, network, target)
				if dialErr == nil {
					return conn, nil
				}
				last = dialErr
			}
			return nil, last
		}
		client := &http.Client{Transport: transport}
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			if len(via) == 0 {
				return http.ErrUseLastResponse
			}
			from := via[len(via)-1].URL.String()
			if _, err := policy.ValidateRedirect(req.Context(), from, req.URL.String()); err != nil {
				return err
			}
			// WebDAV mutation/auth semantics不跨 redirect 自动重放；完成安全复检后显式交回 3xx 给驱动处理。
			return http.ErrUseLastResponse
		}
		return client, nil
	}
	return remote.NewClientManager(maxClients, factory), nil
}

// transportSchemeFor 把 VFS WebDAV scheme 映射到实际 HTTP transport scheme。
func transportSchemeFor(scheme string) (string, error) {
	switch strings.ToLower(scheme) {
	case "webdav":
		return "http", nil
	case "webdavs":
		return "https", nil
	default:
		return "", fmt.Errorf("webdav: unsupported scheme %q", scheme)
	}
}

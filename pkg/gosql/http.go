package gosql

import (
	"crypto/tls"
	"errors"
	"net/url"
	"time"

	httpremote "github.com/steamwo/fssql/internal/storage/remote/http"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type HTTPFallbackPolicy string

const (
	HTTPFallbackMaterialize  HTTPFallbackPolicy = "materialize"
	HTTPFallbackRequireRange HTTPFallbackPolicy = "require_range"
)

// HTTPConfig authorizes one HTTP origin. Open validates and copies configuration
// without accessing the network. Empty FallbackPolicy means materialize.
type HTTPConfig struct {
	Endpoint    string // http(s)://host[:port], without path, query, userinfo or fragment.
	Username    string
	Password    string
	BearerToken string
	Headers     map[string]string
	Network     RemoteNetworkConfig
	// TLSConfig is saved using Clone, a shallow copy. Callers must not mutate
	// shared certificate pools, certificates or callback state after Open.
	// Dynamic TLS callbacks must be concurrency-safe.
	TLSConfig      *tls.Config
	MaxConnections int           // Default 8.
	RequestTimeout time.Duration // Zero adds no overall request timeout.
	FallbackPolicy HTTPFallbackPolicy
}

func publicHTTPDrivers(configs []HTTPConfig) ([]vfs.Driver, func() error, error) {
	internal := make([]httpremote.Config, 0, len(configs))
	for _, c := range configs {
		u, err := url.Parse(c.Endpoint)
		if err != nil {
			return nil, nil, errors.New("gosql: invalid HTTP endpoint")
		}
		port := 80
		if u.Scheme == "https" {
			port = 443
		}
		internal = append(internal, httpremote.Config{Endpoint: c.Endpoint, Username: c.Username, Password: c.Password, BearerToken: c.BearerToken, Headers: c.Headers, Network: publicRemotePolicy(u, c.Network, port), TLSConfig: c.TLSConfig, MaxConnections: c.MaxConnections, RequestTimeout: c.RequestTimeout, FallbackPolicy: httpremote.FallbackPolicy(c.FallbackPolicy)})
	}
	return httpremote.NewDrivers(internal)
}

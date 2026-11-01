package security

import (
	"context"
	"errors"
	"net"
	"testing"
)

type fakeResolver struct {
	addrs map[string][]net.IPAddr
	err   error
}

func (r fakeResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.addrs[host], nil
}

func publicResolver() fakeResolver {
	return fakeResolver{addrs: map[string][]net.IPAddr{
		"example.com":   {{IP: net.ParseIP("93.184.216.34")}},
		"other.example": {{IP: net.ParseIP("203.0.113.20")}},
	}}
}

func TestNetworkPolicyAllowsPublicEndpoint(t *testing.T) {
	p := NewNetworkPolicy(443).WithResolver(publicResolver())
	u, err := p.ValidateURL(context.Background(), "https://example.com/data")
	if err != nil {
		t.Fatal(err)
	}
	if u.Hostname() != "example.com" {
		t.Fatalf("hostname = %s", u.Hostname())
	}
}

func TestNetworkPolicyRejectsLoopbackPrivateAndLinkLocal(t *testing.T) {
	cases := []string{
		"https://127.0.0.1/data",
		"https://10.0.0.1/data",
		"https://169.254.10.20/data",
		"https://[::1]/data",
	}
	p := NewNetworkPolicy(443)
	for _, raw := range cases {
		if _, err := p.ValidateURL(context.Background(), raw); !errors.Is(err, ErrEndpointDenied) {
			t.Fatalf("%s error = %v, want ErrEndpointDenied", raw, err)
		}
	}
}

func TestNetworkPolicyRejectsMetadataEvenWhenPrivateAllowed(t *testing.T) {
	p := NewNetworkPolicy(80)
	p.AllowPrivate = true
	p.AllowLinkLocal = true
	if _, err := p.ValidateURL(context.Background(), "http://169.254.169.254/latest/meta-data"); !errors.Is(err, ErrEndpointDenied) {
		t.Fatalf("error = %v, want ErrEndpointDenied", err)
	}
}

func TestNetworkPolicyRejectsForbiddenPort(t *testing.T) {
	p := NewNetworkPolicy(443).WithResolver(publicResolver())
	if _, err := p.ValidateURL(context.Background(), "https://example.com:25/data"); !errors.Is(err, ErrForbiddenPort) {
		t.Fatalf("error = %v, want ErrForbiddenPort", err)
	}
}

func TestNetworkPolicyRejectsPlainFTPByDefault(t *testing.T) {
	p := NewNetworkPolicy(21).WithResolver(publicResolver())
	if _, err := p.ValidateURL(context.Background(), "ftp://example.com/file.csv"); !errors.Is(err, ErrPlainFTPForbidden) {
		t.Fatalf("error = %v, want ErrPlainFTPForbidden", err)
	}
	p.AllowPlainFTP = true
	if _, err := p.ValidateURL(context.Background(), "ftp://example.com/file.csv"); err != nil {
		t.Fatalf("plain ftp opt-in failed: %v", err)
	}
}

func TestNetworkPolicyRejectsMixedDNSAnswer(t *testing.T) {
	resolver := fakeResolver{addrs: map[string][]net.IPAddr{
		"mixed.example": {
			{IP: net.ParseIP("93.184.216.34")},
			{IP: net.ParseIP("127.0.0.1")},
		},
	}}
	p := NewNetworkPolicy(443).WithResolver(resolver)
	if _, err := p.ValidateURL(context.Background(), "https://mixed.example/file"); !errors.Is(err, ErrEndpointDenied) {
		t.Fatalf("error = %v, want ErrEndpointDenied", err)
	}
}

func TestValidateRedirectRevalidatesTargetAndBlocksDowngrade(t *testing.T) {
	p := NewNetworkPolicy(80, 443).WithResolver(publicResolver())
	if _, err := p.ValidateRedirect(context.Background(), "https://example.com/a", "http://other.example/b"); !errors.Is(err, ErrRedirectDowngrade) {
		t.Fatalf("downgrade error = %v, want ErrRedirectDowngrade", err)
	}
	if _, err := p.ValidateRedirect(context.Background(), "http://example.com/a", "http://127.0.0.1/b"); !errors.Is(err, ErrEndpointDenied) {
		t.Fatalf("redirect target error = %v, want ErrEndpointDenied", err)
	}
}

func TestNetworkPolicyRejectsMetadataHostnameBeforeDNS(t *testing.T) {
	p := NewNetworkPolicy(80).WithResolver(publicResolver())
	if _, err := p.ValidateURL(context.Background(), "http://metadata.google.internal/computeMetadata/v1"); !errors.Is(err, ErrEndpointDenied) {
		t.Fatalf("error = %v, want ErrEndpointDenied", err)
	}
}

func TestResolveURLBindsValidatedDialAddresses(t *testing.T) {
	resolver := fakeResolver{addrs: map[string][]net.IPAddr{
		"example.com": {{IP: net.ParseIP("93.184.216.34")}, {IP: net.ParseIP("203.0.113.10")}},
	}}
	p := NewNetworkPolicy(443).WithResolver(resolver)
	endpoint, err := p.ResolveURL(context.Background(), "https://example.com/data")
	if err != nil {
		t.Fatal(err)
	}
	got := endpoint.DialAddresses()
	if len(got) != 2 || got[0] != "93.184.216.34:443" || got[1] != "203.0.113.10:443" {
		t.Fatalf("dial addresses = %#v", got)
	}
	// 修改返回 IP 快照不应影响 resolver 的原始输入，防止共享可写 slice。
	endpoint.IPs[0][0] = 1
	if resolver.addrs["example.com"][0].IP.String() != "93.184.216.34" {
		t.Fatalf("resolver input mutated: %s", resolver.addrs["example.com"][0].IP)
	}
}

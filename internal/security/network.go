package security

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

var (
	// ErrEndpointDenied 表示目标地址被 NetworkPolicy 拒绝。
	ErrEndpointDenied = errors.New("security: endpoint denied")
	// ErrForbiddenPort 表示目标端口不在安全策略允许列表中。
	ErrForbiddenPort = errors.New("security: forbidden port")
	// ErrPlainFTPForbidden 表示 plain FTP 未被显式允许。
	ErrPlainFTPForbidden = errors.New("security: plain ftp forbidden")
	// ErrRedirectDowngrade 表示安全协议重定向到了非安全协议。
	ErrRedirectDowngrade = errors.New("security: redirect security downgrade")
)

// Resolver 抽象 DNS 解析，便于在请求前执行 SSRF 策略并支持确定性测试。
type Resolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

// stdResolver 使用 Go 标准库解析主机名。
type stdResolver struct{}

// LookupIPAddr 实现 Resolver。
func (stdResolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	return net.DefaultResolver.LookupIPAddr(ctx, host)
}

// NetworkPolicy 定义远程 endpoint 的 SSRF、端口与协议安全边界。
type NetworkPolicy struct {
	// resolver 负责把主机名解析为本次校验使用的实际地址集合。
	resolver Resolver
	// allowedPorts 非空时只允许显式列出的端口。
	allowedPorts map[int]struct{}
	// AllowPrivate 控制是否允许 RFC1918/ULA 私网地址；默认禁止。
	AllowPrivate bool
	// AllowLoopback 控制是否允许 loopback；默认禁止。
	AllowLoopback bool
	// AllowLinkLocal 控制是否允许 link-local；默认禁止。
	AllowLinkLocal bool
	// AllowPlainFTP 控制是否允许未加密 ftp://；默认禁止。
	AllowPlainFTP bool
}

// NewNetworkPolicy 创建默认拒绝私网、loopback、link-local 和 plain FTP 的策略。
func NewNetworkPolicy(allowedPorts ...int) *NetworkPolicy {
	ports := make(map[int]struct{}, len(allowedPorts))
	for _, port := range allowedPorts {
		if port > 0 && port <= 65535 {
			ports[port] = struct{}{}
		}
	}
	return &NetworkPolicy{resolver: stdResolver{}, allowedPorts: ports}
}

// WithResolver 替换 DNS 解析器；主要用于 endpoint-scoped resolver 或确定性测试。
func (p *NetworkPolicy) WithResolver(resolver Resolver) *NetworkPolicy {
	if resolver != nil {
		p.resolver = resolver
	}
	return p
}

// ValidateURL 校验协议、端口及解析出的全部 IP，任何一个地址越界都会拒绝整个 endpoint。
func (p *NetworkPolicy) ValidateURL(ctx context.Context, raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: parse url: %v", ErrEndpointDenied, err)
	}
	if u.Scheme == "" || u.Host == "" || u.Hostname() == "" {
		return nil, fmt.Errorf("%w: url must include scheme and host", ErrEndpointDenied)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme == "ftp" && !p.AllowPlainFTP {
		return nil, ErrPlainFTPForbidden
	}
	port, err := endpointPort(u)
	if err != nil {
		return nil, err
	}
	if len(p.allowedPorts) > 0 {
		if _, ok := p.allowedPorts[port]; !ok {
			return nil, fmt.Errorf("%w: %d", ErrForbiddenPort, port)
		}
	}

	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if isMetadataHostname(host) {
		return nil, fmt.Errorf("%w: metadata hostname %s", ErrEndpointDenied, host)
	}
	ips, err := p.resolve(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve %s: %v", ErrEndpointDenied, host, err)
	}
	for _, ip := range ips {
		if err := p.validateIP(ip); err != nil {
			return nil, err
		}
	}
	return u, nil
}

// ValidateRedirect 重新校验 redirect 目标，并禁止 TLS/安全协议降级。
func (p *NetworkPolicy) ValidateRedirect(ctx context.Context, fromRaw, toRaw string) (*url.URL, error) {
	from, err := url.Parse(fromRaw)
	if err != nil {
		return nil, fmt.Errorf("%w: parse redirect source: %v", ErrEndpointDenied, err)
	}
	to, err := url.Parse(toRaw)
	if err != nil {
		return nil, fmt.Errorf("%w: parse redirect target: %v", ErrEndpointDenied, err)
	}
	to = from.ResolveReference(to)
	if isSecureScheme(from.Scheme) && !isSecureScheme(to.Scheme) {
		return nil, fmt.Errorf("%w: %s -> %s", ErrRedirectDowngrade, from.Scheme, to.Scheme)
	}
	return p.ValidateURL(ctx, to.String())
}

// resolve 返回 endpoint 对应的地址集合；IP literal 不再经过 DNS。
func (p *NetworkPolicy) resolve(ctx context.Context, host string) ([]net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		return []net.IP{ip}, nil
	}
	resolver := p.resolver
	if resolver == nil {
		resolver = stdResolver{}
	}
	addrs, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, errors.New("no resolved addresses")
	}
	ips := make([]net.IP, 0, len(addrs))
	for _, addr := range addrs {
		if addr.IP == nil {
			return nil, errors.New("resolver returned nil ip")
		}
		ips = append(ips, addr.IP)
	}
	return ips, nil
}

// validateIP 应用 SSRF 地址分类规则；metadata 地址始终禁止。
func (p *NetworkPolicy) validateIP(ip net.IP) error {
	if ip == nil || ip.IsUnspecified() || ip.IsMulticast() {
		return fmt.Errorf("%w: non-routable ip %v", ErrEndpointDenied, ip)
	}
	if isMetadataIP(ip) {
		return fmt.Errorf("%w: metadata ip %s", ErrEndpointDenied, ip.String())
	}
	if ip.IsLoopback() && !p.AllowLoopback {
		return fmt.Errorf("%w: loopback ip %s", ErrEndpointDenied, ip.String())
	}
	if (ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast()) && !p.AllowLinkLocal {
		return fmt.Errorf("%w: link-local ip %s", ErrEndpointDenied, ip.String())
	}
	if ip.IsPrivate() && !p.AllowPrivate {
		return fmt.Errorf("%w: private ip %s", ErrEndpointDenied, ip.String())
	}
	return nil
}

// endpointPort 解析显式端口，或按协议返回冻结的默认网络端口。
func endpointPort(u *url.URL) (int, error) {
	if raw := u.Port(); raw != "" {
		port, err := strconv.Atoi(raw)
		if err != nil || port <= 0 || port > 65535 {
			return 0, fmt.Errorf("%w: invalid port %q", ErrForbiddenPort, raw)
		}
		return port, nil
	}
	switch strings.ToLower(u.Scheme) {
	case "ftp":
		return 21, nil
	case "ftps":
		return 990, nil
	case "sftp":
		return 22, nil
	case "http", "webdav":
		return 80, nil
	case "https", "webdavs":
		return 443, nil
	default:
		return 0, fmt.Errorf("%w: unknown default port for scheme %s", ErrForbiddenPort, u.Scheme)
	}
}

// isSecureScheme 判断协议是否提供传输层安全保护。
func isSecureScheme(scheme string) bool {
	switch strings.ToLower(scheme) {
	case "https", "ftps", "sftp", "webdavs":
		return true
	default:
		return false
	}
}

// isMetadataHostname 拒绝常见云元数据域名，避免依赖 DNS 结果是否恰好暴露 link-local 地址。
func isMetadataHostname(host string) bool {
	switch host {
	case "metadata.google.internal", "metadata.azure.internal":
		return true
	default:
		return false
	}
}

// isMetadataIP 拒绝常见云元数据地址；即使显式允许私网/link-local 也不能绕过。
func isMetadataIP(ip net.IP) bool {
	metadata := []net.IP{
		net.ParseIP("169.254.169.254"),
		net.ParseIP("100.100.100.200"),
		net.ParseIP("fd00:ec2::254"),
	}
	for _, candidate := range metadata {
		if candidate != nil && ip.Equal(candidate) {
			return true
		}
	}
	return false
}

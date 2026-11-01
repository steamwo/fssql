package remote

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/steamwo/fssql/internal/storage/vfs"
)

var (
	// ErrCapabilityOverclaim 表示 endpoint probe 宣称了驱动静态实现之外的能力。
	ErrCapabilityOverclaim = errors.New("remote: endpoint capability overclaim")
	// ErrCapabilityIdentity 表示远端 probe 未提供 endpoint-scoped identity。
	ErrCapabilityIdentity = errors.New("remote: invalid endpoint capability identity")
)

// EndpointCapabilityRow 是 P5-D3 的单 endpoint capability matrix 行。
type EndpointCapabilityRow struct {
	// Scheme 是 VFS protocol scheme。
	Scheme string
	// Endpoint 是 probe 绑定的远端 endpoint identity。
	Endpoint string
	// CredentialScope 是能力探测所使用的 credential scope。
	CredentialScope string
	// CredentialVersion 是能力探测所使用的 credential 版本；允许 legacy 空版本。
	CredentialVersion string
	// SecurityProfile 是能力探测绑定的安全策略身份。
	SecurityProfile string
	// Static 是驱动代码真实实现的最大能力集合。
	Static vfs.CapabilitySet
	// ProbeKnown 是 endpoint runtime 明确探测过的能力集合。
	ProbeKnown vfs.CapabilitySet
	// ProbeSupported 是 endpoint runtime 明确支持的能力集合。
	ProbeSupported vfs.CapabilitySet
	// Effective 是 Static 与 runtime probe 合并后的最终能力集合。
	Effective vfs.CapabilitySet
}

// ResolveEndpointCapability 解析一个远端 VFS driver 的静态/运行时/最终能力。
// Runtime probe 只能收窄静态能力；任何 overclaim 都被视为 contract 错误。
func ResolveEndpointCapability(ctx context.Context, driver vfs.Driver) (EndpointCapabilityRow, error) {
	if driver == nil {
		return EndpointCapabilityRow{}, errors.New("remote: nil capability driver")
	}
	static := driver.StaticCapabilities()
	probe, err := driver.Probe(ctx)
	if err != nil {
		return EndpointCapabilityRow{}, err
	}
	if err := probe.Validate(); err != nil {
		return EndpointCapabilityRow{}, err
	}
	if probe.Endpoint == "" || probe.CredentialScope == "" || probe.SecurityProfile == "" {
		return EndpointCapabilityRow{}, fmt.Errorf("%w: scheme=%s endpoint=%q credential_scope=%q security_profile=%q", ErrCapabilityIdentity, driver.Scheme(), probe.Endpoint, probe.CredentialScope, probe.SecurityProfile)
	}
	if overclaim := probe.Supported &^ static; overclaim != 0 {
		return EndpointCapabilityRow{}, fmt.Errorf("%w: scheme=%s capabilities=%#x", ErrCapabilityOverclaim, driver.Scheme(), uint64(overclaim))
	}
	effective, err := vfs.ResolveEffectiveCapabilities(static, probe)
	if err != nil {
		return EndpointCapabilityRow{}, err
	}
	return EndpointCapabilityRow{
		Scheme: driver.Scheme(), Endpoint: probe.Endpoint,
		CredentialScope: probe.CredentialScope, CredentialVersion: probe.CredentialVersion,
		SecurityProfile: probe.SecurityProfile, Static: static,
		ProbeKnown: probe.Known, ProbeSupported: probe.Supported, Effective: effective,
	}, nil
}

// BuildEndpointCapabilityMatrix 对多个远端 driver 生成稳定排序的 capability matrix。
func BuildEndpointCapabilityMatrix(ctx context.Context, drivers ...vfs.Driver) ([]EndpointCapabilityRow, error) {
	rows := make([]EndpointCapabilityRow, 0, len(drivers))
	for _, driver := range drivers {
		row, err := ResolveEndpointCapability(ctx, driver)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Scheme != rows[j].Scheme {
			return rows[i].Scheme < rows[j].Scheme
		}
		if rows[i].Endpoint != rows[j].Endpoint {
			return rows[i].Endpoint < rows[j].Endpoint
		}
		if rows[i].CredentialScope != rows[j].CredentialScope {
			return rows[i].CredentialScope < rows[j].CredentialScope
		}
		if rows[i].CredentialVersion != rows[j].CredentialVersion {
			return rows[i].CredentialVersion < rows[j].CredentialVersion
		}
		return rows[i].SecurityProfile < rows[j].SecurityProfile
	})
	return rows, nil
}

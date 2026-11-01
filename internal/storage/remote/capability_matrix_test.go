package remote

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// matrixFakeDriver 是 capability matrix 的纯协议测试替身。
type matrixFakeDriver struct {
	// scheme 是 fake protocol 名称。
	scheme string
	// static 是驱动静态能力。
	static vfs.CapabilitySet
	// probe 是 endpoint runtime probe。
	probe vfs.ProbeResult
}

// Scheme 返回 fake scheme。
func (d matrixFakeDriver) Scheme() string { return d.scheme }

// StaticCapabilities 返回 fake 静态能力。
func (d matrixFakeDriver) StaticCapabilities() vfs.CapabilitySet { return d.static }

// Probe 返回 fake runtime capability 结果。
func (d matrixFakeDriver) Probe(context.Context) (vfs.ProbeResult, error) { return d.probe, nil }

// Stat 未用于 matrix 测试。
func (d matrixFakeDriver) Stat(context.Context, vfs.URI) (vfs.ObjectInfo, error) {
	return vfs.ObjectInfo{}, errors.New("unused")
}

// List 未用于 matrix 测试。
func (d matrixFakeDriver) List(context.Context, vfs.URI, vfs.ListOptions) (vfs.ListPage, error) {
	return vfs.ListPage{}, errors.New("unused")
}

// ReadRange 未用于 matrix 测试。
func (d matrixFakeDriver) ReadRange(context.Context, vfs.RangeRequest, *memory.Scope, *memory.BufferManager) (vfs.RangeResult, error) {
	return vfs.RangeResult{}, errors.New("unused")
}

func TestResolveEndpointCapabilityNarrowsStaticCapabilities(t *testing.T) {
	driver := matrixFakeDriver{
		scheme: "test", static: vfs.Capabilities(vfs.RandomRead, vfs.StreamingWrite),
		probe: vfs.ProbeResult{
			Known: vfs.Capabilities(vfs.StreamingWrite), Supported: 0,
			Endpoint: "example:443", CredentialScope: "secret:test", CredentialVersion: "2", SecurityProfile: "strict",
		},
	}
	row, err := ResolveEndpointCapability(context.Background(), driver)
	if err != nil {
		t.Fatal(err)
	}
	if !row.Effective.Has(vfs.RandomRead) || row.Effective.Has(vfs.StreamingWrite) {
		t.Fatalf("effective=%#x", uint64(row.Effective))
	}
	if row.CredentialVersion != "2" || row.Endpoint != "example:443" {
		t.Fatalf("row=%+v", row)
	}
}

func TestResolveEndpointCapabilityRejectsRuntimeOverclaim(t *testing.T) {
	driver := matrixFakeDriver{
		scheme: "test", static: vfs.Capabilities(vfs.RandomRead),
		probe: vfs.ProbeResult{
			Supported: vfs.Capabilities(vfs.RandomRead, vfs.StableETag),
			Endpoint:  "example:443", CredentialScope: "secret:test", SecurityProfile: "strict",
		},
	}
	_, err := ResolveEndpointCapability(context.Background(), driver)
	if !errors.Is(err, ErrCapabilityOverclaim) {
		t.Fatalf("err=%v, want ErrCapabilityOverclaim", err)
	}
}

func TestResolveEndpointCapabilityRequiresScopedIdentity(t *testing.T) {
	driver := matrixFakeDriver{scheme: "test", static: vfs.Capabilities(vfs.RandomRead), probe: vfs.ProbeResult{Supported: 0}}
	_, err := ResolveEndpointCapability(context.Background(), driver)
	if !errors.Is(err, ErrCapabilityIdentity) {
		t.Fatalf("err=%v, want ErrCapabilityIdentity", err)
	}
}

func TestBuildEndpointCapabilityMatrixSortsDeterministically(t *testing.T) {
	makeDriver := func(scheme, endpoint string) matrixFakeDriver {
		caps := vfs.Capabilities(vfs.RandomRead)
		return matrixFakeDriver{
			scheme: scheme, static: caps,
			probe: vfs.ProbeResult{Supported: caps, Endpoint: endpoint, CredentialScope: "s", SecurityProfile: "strict"},
		}
	}
	rows, err := BuildEndpointCapabilityMatrix(context.Background(), makeDriver("sftp", "b"), makeDriver("ftp", "z"), makeDriver("sftp", "a"))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || rows[0].Scheme != "ftp" || rows[1].Endpoint != "a" || rows[2].Endpoint != "b" {
		t.Fatalf("rows=%+v", rows)
	}
}

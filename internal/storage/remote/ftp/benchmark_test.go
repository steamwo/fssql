package ftp

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/remote"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// BenchmarkPoolAcquireRelease 覆盖 endpoint-scoped bounded session pool 的高频 lease 路径。
func BenchmarkPoolAcquireRelease(b *testing.B) {
	key := remote.ClientKey{Endpoint: "example.com:990", CredentialScope: "bench", CredentialVersion: "1", SecurityProfile: "strict"}
	session := newFakeSession()
	pool, err := NewPool(key, 4, SessionFactoryFunc(func(context.Context, remote.ClientKey) (Session, error) { return session, nil }))
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lease, err := pool.Acquire(ctx)
		if err != nil {
			b.Fatal(err)
		}
		pool.Release(lease, false)
	}
}

// BenchmarkDriverReadRange64K 覆盖 REST capability gate、Managed Buffer 与 exact range read 热路径。
func BenchmarkDriverReadRange64K(b *testing.B) {
	session := newFakeSession()
	session.files["/data.bin"] = make([]byte, 64<<10)
	driver := testDriverForBenchmark(b, session)
	uri, err := vfs.ParseURI("ftps://example.com:990/data.bin")
	if err != nil {
		b.Fatal(err)
	}
	manager := memory.NewManager(-1)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	request := vfs.RangeRequest{URI: uri, Offset: 0, Length: 64 << 10}
	ctx := context.Background()
	b.SetBytes(64 << 10)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := driver.ReadRange(ctx, request, scope, bm)
		if err != nil {
			b.Fatal(err)
		}
		result.Buffer.Release()
	}
	b.StopTimer()
	if scope.Used() != 0 {
		b.Fatalf("managed bytes leaked: %d", scope.Used())
	}
}

// BenchmarkParseMLSDLine 覆盖目录元数据解析的 CPU/alloc 基线。
func BenchmarkParseMLSDLine(b *testing.B) {
	line := "type=file;size=65536;modify=20260102030405; data-00001.parquet"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := parseMLSDLine(line); err != nil {
			b.Fatal(err)
		}
	}
}

// testDriverForBenchmark 创建 benchmark 专用 driver，避免 testing.T 与 testing.B 类型耦合。
func testDriverForBenchmark(b *testing.B, session *fakeSession) *Driver {
	b.Helper()
	key := remote.ClientKey{Endpoint: "example.com:990", CredentialScope: "secret:warehouse", CredentialVersion: "1", SecurityProfile: "strict"}
	manager := NewManager(4, 2, SessionFactoryFunc(func(context.Context, remote.ClientKey) (Session, error) { return session, nil }))
	driver, err := NewDriver("ftps", key, manager, 2)
	if err != nil {
		b.Fatal(err)
	}
	return driver
}

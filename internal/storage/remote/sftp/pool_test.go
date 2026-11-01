package sftp

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/storage/remote"
)

type noopSession struct{ closed atomic.Int32 }

func (*noopSession) Stat(context.Context, string) (FileInfo, error)             { return FileInfo{}, nil }
func (*noopSession) List(context.Context, string) ([]DirEntry, error)           { return nil, nil }
func (*noopSession) ReadAt(context.Context, string, []byte, int64) (int, error) { return 0, io.EOF }
func (s *noopSession) Close() error                                             { s.closed.Add(1); return nil }

func testKey() remote.ClientKey {
	return remote.ClientKey{Endpoint: "example.test:22", CredentialScope: "test", SecurityProfile: "strict"}
}
func TestPoolBoundedCancelReuseAndPoison(t *testing.T) {
	var dials atomic.Int32
	factory := SessionFactoryFunc(func(context.Context, remote.ClientKey) (Session, error) { dials.Add(1); return &noopSession{}, nil })
	p, err := NewPool(testKey(), 2, factory)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := p.Acquire(context.Background())
	b, _ := p.Acquire(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := p.Acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("third acquire err=%v", err)
	}
	p.Release(a, false)
	a2, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if a2 != a {
		t.Fatal("expected idle session reuse")
	}
	p.Release(a2, true)
	if dials.Load() != 2 {
		t.Fatalf("dials=%d", dials.Load())
	}
	c, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if dials.Load() != 3 {
		t.Fatalf("poison should force new dial, dials=%d", dials.Load())
	}
	p.Release(b, false)
	p.Release(c, false)
	total, leased, idle := p.Stats()
	if leased != 0 || total != 2 || idle != 2 {
		t.Fatalf("stats total=%d leased=%d idle=%d", total, leased, idle)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestPoolFactoryFailureRollsBackSlot(t *testing.T) {
	var calls atomic.Int32
	factory := SessionFactoryFunc(func(context.Context, remote.ClientKey) (Session, error) {
		if calls.Add(1) == 1 {
			return nil, ErrConnection
		}
		return &noopSession{}, nil
	})
	p, _ := NewPool(testKey(), 1, factory)
	if _, err := p.Acquire(context.Background()); !errors.Is(err, ErrConnection) {
		t.Fatalf("err=%v", err)
	}
	if total, leased, _ := p.Stats(); total != 0 || leased != 0 {
		t.Fatalf("after failed dial total=%d leased=%d", total, leased)
	}
	s, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p.Release(s, false)
}

func BenchmarkPoolAcquireRelease(b *testing.B) {
	p, _ := NewPool(testKey(), 8, SessionFactoryFunc(func(context.Context, remote.ClientKey) (Session, error) { return &noopSession{}, nil }))
	s, _ := p.Acquire(context.Background())
	p.Release(s, false)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s, err := p.Acquire(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			p.Release(s, false)
		}
	})
}

package sftp

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type fakeDialer struct {
	key    string
	data   []byte
	dials  atomic.Int64
	closed atomic.Int64
	reset  atomic.Int64
}

func (d *fakeDialer) Dial(ctx context.Context, cfg DialConfig) (DialResult, error) {
	if err := ctx.Err(); err != nil {
		return DialResult{}, err
	}
	d.dials.Add(1)
	return DialResult{Session: &fakeSession{d: d}, HostKeySHA256: d.key}, nil
}

type fakeSession struct{ d *fakeDialer }

func (s *fakeSession) Stat(ctx context.Context, p string) (RemoteInfo, error) {
	if err := ctx.Err(); err != nil {
		return RemoteInfo{}, err
	}
	return RemoteInfo{Path: p, Size: int64(len(s.d.data)), Modified: time.Unix(100, 0)}, nil
}
func (s *fakeSession) List(ctx context.Context, prefix, token string, max int) (RemotePage, error) {
	return RemotePage{Entries: []RemoteEntry{{Path: "data.csv", Size: int64(len(s.d.data)), Modified: time.Unix(100, 0)}}}, nil
}
func (s *fakeSession) ReadAt(ctx context.Context, p string, off int64, dst []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if s.d.reset.CompareAndSwap(1, 0) {
		return 0, ErrConnectionReset
	}
	if off >= int64(len(s.d.data)) {
		return 0, nil
	}
	return copy(dst, s.d.data[off:]), nil
}
func (s *fakeSession) Close() error { s.d.closed.Add(1); return nil }

func config() Config {
	return Config{Endpoint: "sftp://host.example:22", CredentialScope: "test", SecurityProfile: "strict", HostKeySHA256: "SHA256:good", MaxSessions: 2, MaxRetries: 1}
}
func uri(t *testing.T) vfs.URI {
	u, err := vfs.ParseURI("sftp://host.example:22/data.csv")
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestHostKeyMismatchHardFailsAndDiscards(t *testing.T) {
	dial := &fakeDialer{key: "SHA256:bad", data: []byte("abc")}
	d, err := NewDriver(config(), dial)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	_, err = d.Stat(context.Background(), uri(t))
	if !errors.Is(err, ErrHostKeyMismatch) {
		t.Fatalf("err=%v", err)
	}
	if dial.closed.Load() != 1 {
		t.Fatalf("closed=%d", dial.closed.Load())
	}
	open, leased, idle := d.PoolStats()
	if open != 0 || leased != 0 || idle != 0 {
		t.Fatalf("pool=%d/%d/%d", open, leased, idle)
	}
}

func TestBoundedPoolAndCancelWait(t *testing.T) {
	dial := &fakeDialer{key: "SHA256:good", data: []byte("abc")}
	p := newSessionPool(1, dial, DialConfig{Endpoint: "sftp://host", CredentialScope: "x", SecurityProfile: "x"})
	l, _, err := p.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, _, err = p.acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
	l.Release()
	open, leased, idle := p.stats()
	if open != 1 || leased != 0 || idle != 1 {
		t.Fatalf("stats=%d/%d/%d", open, leased, idle)
	}
	p.close()
}

func TestPoisonDiscardAndRetry(t *testing.T) {
	dial := &fakeDialer{key: "SHA256:good", data: []byte("abcdef")}
	dial.reset.Store(1)
	d, err := NewDriver(config(), dial)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	m := memory.NewManager(1 << 20)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	r, err := d.ReadRange(context.Background(), vfs.RangeRequest{URI: uri(t), Offset: 1, Length: 3}, s, bm)
	if err != nil {
		t.Fatal(err)
	}
	if string(r.Buffer.Bytes()) != "bcd" {
		t.Fatalf("%q", r.Buffer.Bytes())
	}
	r.Buffer.Release()
	if dial.dials.Load() != 2 || dial.closed.Load() != 1 {
		t.Fatalf("dials=%d closed=%d", dial.dials.Load(), dial.closed.Load())
	}
	if m.Used() != 0 {
		t.Fatalf("leak=%d", m.Used())
	}
}

func TestCancelAndShortReadNoLeak(t *testing.T) {
	dial := &fakeDialer{key: "SHA256:good", data: []byte("abc")}
	d, err := NewDriver(config(), dial)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	m := memory.NewManager(1 << 20)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = d.ReadRange(ctx, vfs.RangeRequest{URI: uri(t), Offset: 0, Length: 2}, s, bm); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if _, err = d.ReadRange(context.Background(), vfs.RangeRequest{URI: uri(t), Offset: 2, Length: 3}, s, bm); !errors.Is(err, vfs.ErrShortRead) {
		t.Fatalf("err=%v", err)
	}
	if m.Used() != 0 {
		t.Fatalf("leak=%d", m.Used())
	}
}

func TestParallelRangePoolBound(t *testing.T) {
	data := make([]byte, 10000)
	for i := range data {
		data[i] = byte(i)
	}
	dial := &fakeDialer{key: "SHA256:good", data: data}
	cfg := config()
	cfg.MaxSessions = 2
	d, err := NewDriver(cfg, dial)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	m := memory.NewManager(1 << 20)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := d.ReadRange(context.Background(), vfs.RangeRequest{URI: uri(t), Offset: int64(i * 10), Length: 10}, s, bm)
			if err == nil {
				r.Buffer.Release()
			} else {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if len(errs) > 0 {
		t.Fatal(errs)
	}
	if dial.dials.Load() > 2 {
		t.Fatalf("dials=%d", dial.dials.Load())
	}
	if m.Used() != 0 {
		t.Fatalf("leak=%d", m.Used())
	}
}

func TestListProducesCanonicalEntries(t *testing.T) {
	dial := &fakeDialer{key: "SHA256:good", data: []byte("abc")}
	d, _ := NewDriver(config(), dial)
	defer d.Close()
	base, err := vfs.ParseURI("sftp://host.example:22/")
	if err != nil {
		t.Fatal(err)
	}
	page, err := d.List(context.Background(), base, vfs.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range page.Entries {
		names = append(names, entry.URI.ObjectKey())
	}
	sort.Strings(names)
	if fmt.Sprint(names) != "[data.csv]" {
		t.Fatal(names)
	}
}

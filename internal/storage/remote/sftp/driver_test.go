package sftp_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/format/csv"
	"github.com/steamwo/fssql/internal/storage/remote"
	sftp "github.com/steamwo/fssql/internal/storage/remote/sftp"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type fakeSession struct {
	mu       sync.Mutex
	data     []byte
	failRead error
	block    bool
	closed   atomic.Int32
}

func (s *fakeSession) Stat(ctx context.Context, path string) (sftp.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return sftp.FileInfo{}, err
	}
	return sftp.FileInfo{Size: int64(len(s.data)), ModUnix: 1234}, nil
}
func (s *fakeSession) List(ctx context.Context, path string) ([]sftp.DirEntry, error) {
	return []sftp.DirEntry{{Name: "data.csv", Info: sftp.FileInfo{Size: int64(len(s.data)), ModUnix: 1234}}}, ctx.Err()
}
func (s *fakeSession) ReadAt(ctx context.Context, path string, dst []byte, off int64) (int, error) {
	if s.block {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	s.mu.Lock()
	if s.failRead != nil {
		e := s.failRead
		s.failRead = nil
		s.mu.Unlock()
		return 0, e
	}
	s.mu.Unlock()
	if off >= int64(len(s.data)) {
		return 0, io.EOF
	}
	n := copy(dst, s.data[off:])
	if n < len(dst) {
		return n, io.EOF
	}
	return n, nil
}
func (s *fakeSession) Close() error { s.closed.Add(1); return nil }
func key() remote.ClientKey {
	return remote.ClientKey{Endpoint: "host.test:22", CredentialScope: "scope", SecurityProfile: "strict"}
}
func uri(t *testing.T) vfs.URI {
	u, err := vfs.ParseURI("sftp://host.test:22/data.csv")
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestDriverRetriesResetPoisonsSession(t *testing.T) {
	data := []byte("abcdef")
	var calls atomic.Int32
	var first *fakeSession
	factory := sftp.SessionFactoryFunc(func(context.Context, remote.ClientKey) (sftp.Session, error) {
		n := calls.Add(1)
		s := &fakeSession{data: data}
		if n == 1 {
			s.failRead = sftp.ErrConnection
			first = s
		}
		return s, nil
	})
	mgr := sftp.NewManager(4, 1, factory)
	d, err := sftp.NewDriver(key(), mgr, 2)
	if err != nil {
		t.Fatal(err)
	}
	m := memory.NewManager(1 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	r, err := d.ReadRange(context.Background(), vfs.RangeRequest{URI: uri(t), Offset: 1, Length: 3, Tag: memory.TagNetworkBuffer}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	if string(r.Buffer.Bytes()) != "bcd" {
		t.Fatalf("data=%q", r.Buffer.Bytes())
	}
	r.Buffer.Release()
	if first == nil || first.closed.Load() == 0 {
		t.Fatal("reset session was not discarded")
	}
	p, _ := mgr.Pool(key())
	_, leased, _ := p.Stats()
	if leased != 0 {
		t.Fatalf("leased=%d", leased)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed leak=%d", scope.Used())
	}
}
func TestWrongHostKeyNoRetry(t *testing.T) {
	var calls atomic.Int32
	factory := sftp.SessionFactoryFunc(func(context.Context, remote.ClientKey) (sftp.Session, error) {
		calls.Add(1)
		return nil, sftp.ErrHostKey
	})
	mgr := sftp.NewManager(2, 1, factory)
	d, _ := sftp.NewDriver(key(), mgr, 5)
	if _, err := d.Probe(context.Background()); !errors.Is(err, sftp.ErrHostKey) {
		t.Fatalf("err=%v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("host key failure retried %d times", calls.Load())
	}
}
func TestCancelReadReleasesLeaseAndManagedBuffer(t *testing.T) {
	factory := sftp.SessionFactoryFunc(func(context.Context, remote.ClientKey) (sftp.Session, error) {
		return &fakeSession{data: []byte("abcdef"), block: true}, nil
	})
	mgr := sftp.NewManager(2, 1, factory)
	d, _ := sftp.NewDriver(key(), mgr, 2)
	m := memory.NewManager(1 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := d.ReadRange(ctx, vfs.RangeRequest{URI: uri(t), Offset: 0, Length: 3, Tag: memory.TagNetworkBuffer}, scope, bm)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
	p, _ := mgr.Pool(key())
	_, leased, _ := p.Stats()
	if leased != 0 {
		t.Fatalf("leased=%d", leased)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed leak=%d", scope.Used())
	}
}
func TestEndpointMismatchRejectedBeforeDial(t *testing.T) {
	var calls atomic.Int32
	factory := sftp.SessionFactoryFunc(func(context.Context, remote.ClientKey) (sftp.Session, error) {
		calls.Add(1)
		return &fakeSession{}, nil
	})
	mgr := sftp.NewManager(2, 1, factory)
	d, _ := sftp.NewDriver(key(), mgr, 2)
	bad, _ := vfs.ParseURI("sftp://evil.test:22/data.csv")
	if _, err := d.Stat(context.Background(), bad); err == nil {
		t.Fatal("expected endpoint mismatch")
	}
	if calls.Load() != 0 {
		t.Fatal("endpoint mismatch dialed remote")
	}
}
func TestSFTPQueryDemoCSV(t *testing.T) {
	data := []byte("id,name\n1,alpha\n2,beta\n3,gamma\n")
	factory := sftp.SessionFactoryFunc(func(context.Context, remote.ClientKey) (sftp.Session, error) { return &fakeSession{data: data}, nil })
	mgr := sftp.NewManager(2, 2, factory)
	driver, _ := sftp.NewDriver(key(), mgr, 2)
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	m := memory.NewManager(4 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	binder, _ := csv.NewSourceBinder(registry, scope, bm)
	binding, err := binder.Bind(context.Background(), uri(t).String(), csv.SourceBindOptions{Inference: csv.InferenceOptions{Header: true}})
	if err != nil {
		t.Fatal(err)
	}
	src, err := csv.NewSource(context.Background(), registry, binding, csv.ScanOptions{EnableParallel: true, TargetRangeBytes: 8}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for {
		chunk, ok, err := src.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		for i := 0; i < chunk.Cardinality; i++ {
			v, valid := chunk.Columns[0].Int64(i)
			if !valid {
				t.Fatal("null id")
			}
			ids = append(ids, v)
		}
		chunk.Release()
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if fmt.Sprint(ids) != "[1 2 3]" {
		t.Fatalf("ids=%v", ids)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed leak=%d", scope.Used())
	}
	p, _ := mgr.Pool(key())
	_, leased, _ := p.Stats()
	if leased != 0 {
		t.Fatalf("leased=%d", leased)
	}
}

func TestParallelCapabilityRequiresPoolCapacity(t *testing.T) {
	factory := sftp.SessionFactoryFunc(func(context.Context, remote.ClientKey) (sftp.Session, error) { return &fakeSession{}, nil })
	one := sftp.NewManager(2, 1, factory)
	d1, _ := sftp.NewDriver(key(), one, 1)
	if d1.StaticCapabilities().Has(vfs.ParallelRandomRead) {
		t.Fatal("single-session pool advertised parallel random read")
	}
	many := sftp.NewManager(2, 2, factory)
	d2, _ := sftp.NewDriver(key(), many, 1)
	if !d2.StaticCapabilities().Has(vfs.ParallelRandomRead) {
		t.Fatal("multi-session pool missing parallel random read")
	}
}

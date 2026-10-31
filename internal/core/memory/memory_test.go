package memory

import (
	"errors"
	"testing"
)

func TestReserveResizeRelease(t *testing.T) {
	m := NewManager(100)
	s := m.NewScope()
	r, err := s.Reserve(TagVector, 40)
	if err != nil {
		t.Fatal(err)
	}
	if m.Used() != 40 || s.UsedByTag(TagVector) != 40 {
		t.Fatal("accounting")
	}
	if err := r.Resize(80); err != nil {
		t.Fatal(err)
	}
	if err := r.Resize(101); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("%v", err)
	}
	r.Release()
	r.Release()
	if m.Used() != 0 || s.Used() != 0 {
		t.Fatalf("leak m=%d s=%d", m.Used(), s.Used())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestAllocationFailureCleanup(t *testing.T) {
	m := NewManager(100)
	s := m.NewScope()
	sentinel := errors.New("boom")
	bm := NewBufferManager(func(int) ([]byte, error) { return nil, sentinel })
	if _, err := bm.Allocate(s, TagVector, 64); !errors.Is(err, sentinel) {
		t.Fatalf("%v", err)
	}
	if m.Used() != 0 || s.Used() != 0 {
		t.Fatalf("reservation leaked: %d/%d", m.Used(), s.Used())
	}
}
func TestBufferRelease(t *testing.T) {
	m := NewManager(64)
	s := m.NewScope()
	bm := NewBufferManager(nil)
	b, err := bm.Allocate(s, TagStringHeap, 64)
	if err != nil {
		t.Fatal(err)
	}
	if b.Len() != 64 || m.Peak() != 64 {
		t.Fatal("buffer accounting")
	}
	b.Release()
	if m.Used() != 0 {
		t.Fatal("buffer leak")
	}
}
func TestScopeRefusesCloseWithLeak(t *testing.T) {
	m := NewManager(64)
	s := m.NewScope()
	r, _ := s.Reserve(TagVector, 1)
	if err := s.Close(); err == nil {
		t.Fatal("expected leak assertion")
	}
	r.Release()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestResizeAllocationFailureRollsBackReservation(t *testing.T) {
	m := NewManager(256)
	s := m.NewScope()
	bm := NewBufferManager(nil)
	b, err := bm.Allocate(s, TagVector, 64)
	if err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("resize fail")
	if err := b.Resize(128, func(int) ([]byte, error) { return nil, sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("%v", err)
	}
	if b.Len() != 64 || m.Used() != 64 || s.Used() != 64 {
		t.Fatalf("rollback len=%d m=%d s=%d", b.Len(), m.Used(), s.Used())
	}
	b.Release()
	if m.Used() != 0 {
		t.Fatal("leak")
	}
}

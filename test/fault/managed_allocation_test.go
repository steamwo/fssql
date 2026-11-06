package fault

import (
	"errors"
	"github.com/steamwo/fssql/internal/core/memory"
	"testing"
)

func TestManagedAllocationFailureLeavesNoReservation(t *testing.T) {
	m := memory.NewManager(4096)
	s := m.NewScope()
	sentinel := errors.New("injected allocation failure")
	bm := memory.NewBufferManager(func(int) ([]byte, error) { return nil, sentinel })
	if _, err := bm.Allocate(s, memory.TagVector, 2048); !errors.Is(err, sentinel) {
		t.Fatalf("%v", err)
	}
	if m.Used() != 0 || s.Used() != 0 {
		t.Fatalf("leak manager=%d scope=%d", m.Used(), s.Used())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

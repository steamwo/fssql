package temp

import (
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
)

func TestPageListManagedGrowth(t *testing.T) {
	m := memory.NewManager(1 << 20)
	s := m.NewScope()
	list, err := NewPageList(s, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 1000; i++ {
		if err := list.Append(PageID(i)); err != nil {
			t.Fatal(err)
		}
	}
	if list.Count() != 1000 {
		t.Fatalf("count=%d", list.Count())
	}
	for _, i := range []int{0, 7, 8, 999} {
		id, ok := list.At(i)
		if !ok || id != PageID(i+1) {
			t.Fatalf("at %d=(%d,%v)", i, id, ok)
		}
	}
	if s.UsedByTag(memory.TagMetadata) == 0 {
		t.Fatal("page list is not accounted as metadata")
	}
	list.Release()
	if m.Used() != 0 {
		t.Fatalf("memory leak=%d", m.Used())
	}
}

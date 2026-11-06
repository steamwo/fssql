package join

import (
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
)

// TestPartitionedHashHeadsRetainDuplicateChains 验证低位相同的分区键在标量与批次探测中保持相同链头。
func TestPartitionedHashHeadsRetainDuplicateChains(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	index, err := newRowHashTable(scope, memory.NewBufferManager(nil), 513)
	if err != nil {
		t.Fatal(err)
	}
	hashes := make([]uint64, 512)
	rows := make([]int, 512)
	for i := range hashes {
		hashes[i] = uint64(i)<<32 | 5
		rows[i] = i
		if err := index.Insert(hashes[i], i); err != nil {
			t.Fatal(err)
		}
	}
	if err := index.Insert(hashes[0], 512); err != nil {
		t.Fatal(err)
	}
	heads, pending := make([]int, 512), make([]int, 512)
	index.heads(hashes, rows, heads, pending)
	for i, hash := range hashes {
		head, ok := index.Head(hash)
		want := i
		if i == 0 {
			want = 512
		}
		if !ok || head != want || heads[i] != want {
			t.Fatalf("key %d head=%d batch=%d valid=%v want %d", i, head, heads[i], ok, want)
		}
	}
	if next, ok := index.Next(512); !ok || next != 0 {
		t.Fatalf("duplicate next=%d valid=%v", next, ok)
	}
	if _, ok := index.Head(uint64(10000)<<32 | 5); ok {
		t.Fatal("unexpected absent hash match")
	}
	index.Release()
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

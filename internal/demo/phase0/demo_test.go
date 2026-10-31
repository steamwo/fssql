package phase0

import "testing"

func TestVector10M(t *testing.T) {
	const n = int64(10_000_000)
	r, err := Vector10M(n)
	if err != nil {
		t.Fatal(err)
	}
	want := n * (n - 1) / 2
	if r.Rows != n || r.Sum != want {
		t.Fatalf("rows=%d sum=%d want=%d", r.Rows, r.Sum, want)
	}
	if r.ManagedEnd != 0 {
		t.Fatalf("managed leak %d", r.ManagedEnd)
	}
	if r.ManagedPeak > 1<<20 {
		t.Fatalf("unexpected peak %d", r.ManagedPeak)
	}
}
func TestMemoryAccountingDemo(t *testing.T) {
	r, err := MemoryAccounting()
	if err != nil {
		t.Fatal(err)
	}
	if r.ManagedPeak != int64(2048*8) || r.ManagedEnd != 0 {
		t.Fatalf("%+v", r)
	}
}
func TestPushPipelineDemo(t *testing.T) {
	const n = int64(100_000)
	a, err := PushPipeline(n, 1)
	if err != nil {
		t.Fatal(err)
	}
	b, err := PushPipeline(n, 4)
	if err != nil {
		t.Fatal(err)
	}
	if a.Rows != n || b.Rows != n || a.Sum != b.Sum || a.ManagedEnd != 0 || b.ManagedEnd != 0 {
		t.Fatalf("single=%+v multi=%+v", a, b)
	}
}
func BenchmarkPushPipeline100K(b *testing.B) {
	for i := 0; i < b.N; i++ {
		r, err := PushPipeline(100_000, 4)
		if err != nil {
			b.Fatal(err)
		}
		if r.ManagedEnd != 0 {
			b.Fatal("managed leak")
		}
	}
}

package hash

import (
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/semantics"
)

func TestPartitionHistogramManagedAndMerge(t *testing.T) {
	m := memory.NewManager(1 << 20)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	a, err := NewPartitionHistogram(s, bm, 16)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewPartitionHistogram(s, bm, 16)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if err := a.Add(i&15, 10); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 50; i++ {
		if err := b.Add(i&15, 20); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}
	total := a.Total()
	if total.Rows != 150 || total.Bytes != 2000 {
		t.Fatalf("total=%+v", total)
	}
	a.Release()
	b.Release()
	if m.Used() != 0 {
		t.Fatalf("managed histogram leak=%d", m.Used())
	}
}

func TestSkewDetectorSyntheticDataset(t *testing.T) {
	m := memory.NewManager(1 << 20)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	p, _ := NewRadixPartitioner(4)
	h, err := NewPartitionHistogram(s, bm, p.Count())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 9500; i++ {
		if err := h.AddHash(semantics.HashInt64(42), 64, p, 0); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 500; i++ {
		if err := h.AddHash(semantics.HashInt64(int64(1000+i)), 64, p, 0); err != nil {
			t.Fatal(err)
		}
	}
	d, _ := NewSkewDetector(80, 100, 1)
	report, err := d.Detect(h)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Skewed {
		t.Fatalf("expected skew: %+v", report)
	}
	if report.Dominant.Rows < 9500 {
		t.Fatalf("dominant rows=%d", report.Dominant.Rows)
	}
	h.Release()
	if m.Used() != 0 {
		t.Fatalf("leak=%d", m.Used())
	}
}

func TestSkewDetectorDoesNotFlagUniformDataset(t *testing.T) {
	m := memory.NewManager(1 << 20)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	h, _ := NewPartitionHistogram(s, bm, 4)
	for part := 0; part < 4; part++ {
		for i := 0; i < 100; i++ {
			_ = h.Add(part, 64)
		}
	}
	d, _ := NewSkewDetector(60, 100, 1)
	report, _ := d.Detect(h)
	if report.Skewed {
		t.Fatalf("uniform dataset reported skew: %+v", report)
	}
	h.Release()
	if m.Used() != 0 {
		t.Fatalf("leak=%d", m.Used())
	}
}

func TestRepartitionPolicy(t *testing.T) {
	p, err := NewRepartitionPolicy(3, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Decide(0, 0, 80, 100); got.Action != PartitionFits {
		t.Fatalf("fit=%+v", got)
	}
	if got := p.Decide(0, 1000, 700, 100); got.Action != PartitionRepartition || got.NextDepth != 1 {
		t.Fatalf("repartition=%+v", got)
	}
	if got := p.Decide(1, 1000, 950, 100); got.Action != PartitionTerminalSkew || got.Reason != ReasonNoProgress {
		t.Fatalf("no progress=%+v", got)
	}
	if got := p.Decide(3, 1000, 700, 100); got.Action != PartitionTerminalSkew || got.Reason != ReasonMaxDepth {
		t.Fatalf("max depth=%+v", got)
	}
}

func TestSameKeySkewMakesNoProgressAcrossDepth(t *testing.T) {
	m := memory.NewManager(1 << 20)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	p, _ := NewRadixPartitioner(4)
	h0, _ := NewPartitionHistogram(s, bm, p.Count())
	h1, _ := NewPartitionHistogram(s, bm, p.Count())
	hash := semantics.HashInt64(7)
	for i := 0; i < 1000; i++ {
		_ = h0.AddHash(hash, 128, p, 0)
		_ = h1.AddHash(hash, 128, p, 1)
	}
	_, s0 := h0.Dominant()
	_, s1 := h1.Dominant()
	policy, _ := NewRepartitionPolicy(4, 10)
	decision := policy.Decide(1, s0.Bytes, s1.Bytes, 1024)
	if decision.Action != PartitionTerminalSkew || decision.Reason != ReasonNoProgress {
		t.Fatalf("decision=%+v", decision)
	}
	h0.Release()
	h1.Release()
	if m.Used() != 0 {
		t.Fatalf("leak=%d", m.Used())
	}
}

func TestPartitionHistogramAllocationFailure(t *testing.T) {
	m := memory.NewManager(32)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	h, err := NewPartitionHistogram(s, bm, 16)
	if h != nil {
		h.Release()
		t.Fatal("unexpected histogram")
	}
	if !errors.Is(err, memory.ErrLimitExceeded) {
		t.Fatalf("error=%v", err)
	}
	if m.Used() != 0 {
		t.Fatalf("reservation leak=%d", m.Used())
	}
}

func BenchmarkPartitionHistogramAddHash(b *testing.B) {
	m := memory.NewManager(1 << 20)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	p, _ := NewRadixPartitioner(8)
	h, _ := NewPartitionHistogram(s, bm, p.Count())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := h.AddHash(semantics.HashInt64(int64(i)), 64, p, 0); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	h.Release()
	if m.Used() != 0 {
		b.Fatal("leak")
	}
}

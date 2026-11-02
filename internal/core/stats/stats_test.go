package stats

import (
	"context"
	"testing"
)

func TestUnknownEstimateIsExplicit(t *testing.T) {
	p, err := NewStaticProvider(2)
	if err != nil {
		t.Fatal(err)
	}
	e, err := p.EstimateRelation(context.Background(), RelationKey{Name: "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if e.Source != SourceUnknown || e.Confidence != ConfidenceUnknown || e.Rows != 0 || !e.Valid() {
		t.Fatalf("unexpected unknown estimate: %+v", e)
	}
}

func TestPutAndEstimate(t *testing.T) {
	p, _ := NewStaticProvider(2)
	key := RelationKey{Catalog: "memory", Schema: "main", Name: "orders"}
	want := Estimate{Rows: 42, Confidence: ConfidenceHigh, Source: SourceCatalogPersistent, Fingerprint: "v1"}
	if err := p.Put(key, want); err != nil {
		t.Fatal(err)
	}
	got, err := p.EstimateRelation(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

func TestCapacityAndValidation(t *testing.T) {
	p, _ := NewStaticProvider(1)
	valid := Estimate{Rows: 1, Confidence: ConfidenceExact, Source: SourceStorageNative, Fingerprint: "fp"}
	if err := p.Put(RelationKey{Name: "a"}, valid); err != nil {
		t.Fatal(err)
	}
	if err := p.Put(RelationKey{Name: "b"}, valid); err == nil {
		t.Fatal("expected capacity error")
	}
	if err := p.Put(RelationKey{Name: "a"}, valid); err != nil {
		t.Fatalf("overwrite should fit: %v", err)
	}
	if err := p.Put(RelationKey{Name: "c"}, Estimate{Rows: 1, Source: SourceSampled}); err == nil {
		t.Fatal("expected invalid estimate")
	}
}

func TestCanceledContext(t *testing.T) {
	p, _ := NewStaticProvider(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.EstimateRelation(ctx, RelationKey{Name: "a"}); err == nil {
		t.Fatal("expected context error")
	}
}

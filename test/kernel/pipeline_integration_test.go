package kernel

import (
	kernel "github.com/steamwo/fssql/internal/demo/kernel"
	"testing"
)

func TestKernelEndToEnd(t *testing.T) {
	const n = int64(250_000)
	r, err := kernel.PushPipeline(n, 4)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows != n || r.Sum != n*(n-1)/2 {
		t.Fatalf("%+v", r)
	}
	if r.ManagedEnd != 0 {
		t.Fatalf("managed memory leak: %d", r.ManagedEnd)
	}
}

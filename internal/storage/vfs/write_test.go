package vfs

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
)

type testReadOnlyDriver struct{ caps CapabilitySet }

func (d *testReadOnlyDriver) Scheme() string { return "test" }
func (d *testReadOnlyDriver) StaticCapabilities() CapabilitySet { return d.caps }
func (d *testReadOnlyDriver) Probe(context.Context) (ProbeResult, error) {
	return ProbeResult{Supported: d.caps, Endpoint: "test://endpoint"}, nil
}
func (d *testReadOnlyDriver) Stat(context.Context, URI) (ObjectInfo, error) { return ObjectInfo{}, nil }
func (d *testReadOnlyDriver) List(context.Context, URI, ListOptions) (ListPage, error) { return ListPage{}, nil }
func (d *testReadOnlyDriver) ReadRange(context.Context, RangeRequest, *memory.Scope, *memory.BufferManager) (RangeResult, error) {
	return RangeResult{}, nil
}

type testWriteDriver struct {
	testReadOnlyDriver
	opened bool
}

func (d *testWriteDriver) OpenWrite(context.Context, URI, WriteOptions, *memory.Scope, *memory.BufferManager) (WriteHandle, error) {
	d.opened = true
	return testWriteHandle{}, nil
}
func (d *testWriteDriver) Delete(context.Context, URI, *ObjectIdentity) error { return nil }

type testWriteHandle struct{}

func (testWriteHandle) Write(context.Context, []byte) (int, error) { return 0, nil }
func (testWriteHandle) Close(context.Context) (ObjectIdentity, error) { return ObjectIdentity{}, nil }
func (testWriteHandle) Abort(context.Context) error { return nil }

func TestWriteDriverForRejectsReadOnlyDriver(t *testing.T) {
	r, err := NewRegistry(&testReadOnlyDriver{caps: Capabilities(RandomRead)})
	if err != nil {
		t.Fatal(err)
	}
	u, _ := ParseURI("test://host/object")
	_, _, err = r.WriteDriverFor(context.Background(), u)
	if !errors.Is(err, ErrWriteUnsupported) {
		t.Fatalf("expected write unsupported, got %v", err)
	}
}

func TestOpenWriteRequiresEffectiveCapabilityAndConditionalSupport(t *testing.T) {
	d := &testWriteDriver{testReadOnlyDriver: testReadOnlyDriver{caps: Capabilities(StreamingWrite)}}
	r, err := NewRegistry(d)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := ParseURI("test://host/object")
	mm := memory.NewManager(1024)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	if _, err := r.OpenWrite(context.Background(), u, WriteOptions{IfNoneMatch: true}, scope, bm); !errors.Is(err, ErrWriteUnsupported) {
		t.Fatalf("expected conditional capability error, got %v", err)
	}
	if d.opened {
		t.Fatal("driver must not be opened when capability gate rejects request")
	}

	d.caps = Capabilities(StreamingWrite, ConditionalWrite)
	if _, err := r.OpenWrite(context.Background(), u, WriteOptions{IfNoneMatch: true}, scope, bm); err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}
	if !d.opened {
		t.Fatal("expected driver OpenWrite to be called")
	}
}

func TestOpenWriteRequiresManagedMemory(t *testing.T) {
	d := &testWriteDriver{testReadOnlyDriver: testReadOnlyDriver{caps: Capabilities(StreamingWrite)}}
	r, _ := NewRegistry(d)
	u, _ := ParseURI("test://host/object")
	if _, err := r.OpenWrite(context.Background(), u, WriteOptions{}, nil, memory.NewBufferManager(nil)); err == nil {
		t.Fatal("expected nil memory scope rejection")
	}
}

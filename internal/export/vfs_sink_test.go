package export

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// sinkTestDriver 是测试用 WritableDriver，验证 export 只能经 VFS capability gate 打开目标。
type sinkTestDriver struct {
	handle *sinkTestHandle
	caps   vfs.CapabilitySet
}

func (d *sinkTestDriver) Scheme() string { return "sinktest" }
func (d *sinkTestDriver) StaticCapabilities() vfs.CapabilitySet { return d.caps }
func (d *sinkTestDriver) Probe(context.Context) (vfs.ProbeResult, error) {
	return vfs.ProbeResult{Supported: d.caps, Endpoint: "sinktest://endpoint"}, nil
}
func (*sinkTestDriver) Stat(context.Context, vfs.URI) (vfs.ObjectInfo, error) { return vfs.ObjectInfo{}, nil }
func (*sinkTestDriver) List(context.Context, vfs.URI, vfs.ListOptions) (vfs.ListPage, error) {
	return vfs.ListPage{}, nil
}
func (*sinkTestDriver) ReadRange(context.Context, vfs.RangeRequest, *memory.Scope, *memory.BufferManager) (vfs.RangeResult, error) {
	return vfs.RangeResult{}, nil
}
func (d *sinkTestDriver) OpenWrite(context.Context, vfs.URI, vfs.WriteOptions, *memory.Scope, *memory.BufferManager) (vfs.WriteHandle, error) {
	return d.handle, nil
}
func (*sinkTestDriver) Delete(context.Context, vfs.URI, *vfs.ObjectIdentity) error { return nil }

// sinkTestHandle 记录字节和终态，用于断言 Close 失败后仍能 Abort。
type sinkTestHandle struct {
	data      []byte
	short     bool
	closeErr  error
	aborted   bool
	closed    bool
}

func (h *sinkTestHandle) Write(_ context.Context, p []byte) (int, error) {
	if h.short && len(p) > 0 {
		n := len(p) - 1
		h.data = append(h.data, p[:n]...)
		return n, nil
	}
	h.data = append(h.data, p...)
	return len(p), nil
}
func (h *sinkTestHandle) Close(context.Context) (vfs.ObjectIdentity, error) {
	if h.closeErr != nil {
		return vfs.ObjectIdentity{}, h.closeErr
	}
	h.closed = true
	return vfs.ObjectIdentity{Size: int64(len(h.data))}, nil
}
func (h *sinkTestHandle) Abort(context.Context) error { h.aborted = true; return nil }

// TestVFSByteSinkChecksumAndLifecycle 验证字节数、SHA-256、终态和 VFS 路径。
func TestVFSByteSinkChecksumAndLifecycle(t *testing.T) {
	h := &sinkTestHandle{}
	d := &sinkTestDriver{handle: h, caps: vfs.Capabilities(vfs.StreamingWrite)}
	r, err := vfs.NewRegistry(d)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := vfs.ParseURI("sinktest://host/out")
	mm := memory.NewManager(1 << 20)
	sink, err := NewVFSByteSink(context.Background(), r, u, vfs.WriteOptions{}, mm.NewScope(), memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("abcdef")
	if n, err := sink.Write(context.Background(), payload); err != nil || n != len(payload) {
		t.Fatalf("write n=%d err=%v", n, err)
	}
	result, err := sink.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	if result.Bytes != int64(len(payload)) || result.Checksum != hex.EncodeToString(sum[:]) {
		t.Fatalf("result=%+v", result)
	}
	if !h.closed {
		t.Fatal("underlying handle not closed")
	}
	if _, err := sink.Write(context.Background(), []byte("x")); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("expected terminal rejection, got %v", err)
	}
}

// TestVFSByteSinkShortWriteFailsHard 验证 VFS short write 不会被格式层静默接受。
func TestVFSByteSinkShortWriteFailsHard(t *testing.T) {
	h := &sinkTestHandle{short: true}
	d := &sinkTestDriver{handle: h, caps: vfs.Capabilities(vfs.StreamingWrite)}
	r, _ := vfs.NewRegistry(d)
	u, _ := vfs.ParseURI("sinktest://host/out")
	mm := memory.NewManager(1 << 20)
	sink, err := NewVFSByteSink(context.Background(), r, u, vfs.WriteOptions{}, mm.NewScope(), memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sink.Write(context.Background(), []byte("abc")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("expected short write, got %v", err)
	}
	if err := sink.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !h.aborted {
		t.Fatal("underlying incomplete file was not aborted")
	}
}

// TestVFSByteSinkCloseFailurePreservesAbort 验证底层 Close 失败时 sink 不进入不可清理终态。
func TestVFSByteSinkCloseFailurePreservesAbort(t *testing.T) {
	boom := errors.New("close failed")
	h := &sinkTestHandle{closeErr: boom}
	d := &sinkTestDriver{handle: h, caps: vfs.Capabilities(vfs.StreamingWrite)}
	r, _ := vfs.NewRegistry(d)
	u, _ := vfs.ParseURI("sinktest://host/out")
	mm := memory.NewManager(1 << 20)
	sink, _ := NewVFSByteSink(context.Background(), r, u, vfs.WriteOptions{}, mm.NewScope(), memory.NewBufferManager(nil))
	_, _ = sink.Write(context.Background(), []byte("abc"))
	if _, err := sink.Close(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("expected close error, got %v", err)
	}
	if err := sink.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !h.aborted {
		t.Fatal("close failure lost abort path")
	}
}

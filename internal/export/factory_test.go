package export

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// factoryTestDriver 每次 OpenWrite 创建独立 handle，并保存最后一个对象便于检查最终字节。
type factoryTestDriver struct {
	caps   vfs.CapabilitySet
	last   *sinkTestHandle
	opens  int
}

func (d *factoryTestDriver) Scheme() string { return "factorytest" }
func (d *factoryTestDriver) StaticCapabilities() vfs.CapabilitySet { return d.caps }
func (d *factoryTestDriver) Probe(context.Context) (vfs.ProbeResult, error) {
	return vfs.ProbeResult{Supported: d.caps, Endpoint: "factorytest://endpoint"}, nil
}
func (*factoryTestDriver) Stat(context.Context, vfs.URI) (vfs.ObjectInfo, error) { return vfs.ObjectInfo{}, nil }
func (*factoryTestDriver) List(context.Context, vfs.URI, vfs.ListOptions) (vfs.ListPage, error) {
	return vfs.ListPage{}, nil
}
func (*factoryTestDriver) ReadRange(context.Context, vfs.RangeRequest, *memory.Scope, *memory.BufferManager) (vfs.RangeResult, error) {
	return vfs.RangeResult{}, nil
}
func (d *factoryTestDriver) OpenWrite(context.Context, vfs.URI, vfs.WriteOptions, *memory.Scope, *memory.BufferManager) (vfs.WriteHandle, error) {
	d.opens++
	d.last = &sinkTestHandle{}
	return d.last, nil
}
func (*factoryTestDriver) Delete(context.Context, vfs.URI, *vfs.ObjectIdentity) error { return nil }

// TestFileWriterFactoryCSVGzip 验证组合顺序确实是 CSV → gzip → VFS，而不是把协议逻辑嵌入格式 writer。
func TestFileWriterFactoryCSVGzip(t *testing.T) {
	mm := memory.NewManager(8 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	d := &factoryTestDriver{caps: vfs.Capabilities(vfs.StreamingWrite)}
	registry, _ := vfs.NewRegistry(d)
	pool, _ := NewWriterPool(2, 64<<10, scope)
	schema := []Column{{Name: "id", Type: types.LogicalType{ID: types.Integer}}, {Name: "name", Type: types.LogicalType{ID: types.Varchar}}}
	factory, err := NewFileWriterFactory(FileWriterConfig{Schema: schema, Format: FormatCSV, Compression: "gzip", CSV: CSVWriteOptions{Header: true}}, registry, scope, bm, pool)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := vfs.ParseURI("factorytest://host/out.csv.gz")
	writer, err := factory.Open(context.Background(), u, vfs.WriteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	id := newVector(t, scope, bm, schema[0].Type, 1)
	id.SetInt32(0, 7)
	name := newVector(t, scope, bm, schema[1].Type, 1)
	_ = name.SetString(0, "alpha")
	chunk, _ := vector.NewDataChunk(id, name)
	if err := writer.WriteChunk(context.Background(), chunk); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(d.last.data))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	_ = zr.Close()
	if got, want := string(decoded), "id,name\n7,alpha\n"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	chunk.Release()
	if pool.Active() != 0 || scope.Used() != 0 {
		t.Fatalf("ownership leak active=%d used=%d", pool.Active(), scope.Used())
	}
}

// TestFileWriterFactoryNDJSONIdentity 验证 NDJSON framing 由格式层决定，identity compression 不插入额外字节。
func TestFileWriterFactoryNDJSONIdentity(t *testing.T) {
	mm := memory.NewManager(4 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	d := &factoryTestDriver{caps: vfs.Capabilities(vfs.StreamingWrite)}
	registry, _ := vfs.NewRegistry(d)
	pool, _ := NewWriterPool(1, 32<<10, scope)
	typ := types.LogicalType{ID: types.BigInt}
	factory, err := NewFileWriterFactory(FileWriterConfig{Schema: []Column{{Name: "n", Type: typ}}, Format: FormatNDJSON, Compression: "none"}, registry, scope, bm, pool)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := vfs.ParseURI("factorytest://host/out.ndjson")
	writer, err := factory.Open(context.Background(), u, vfs.WriteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	v := newVector(t, scope, bm, typ, 1)
	v.SetInt64(0, 42)
	chunk, _ := vector.NewDataChunk(v)
	if err := writer.WriteChunk(context.Background(), chunk); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, want := string(d.last.data), "{\"n\":42}\n"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	chunk.Release()
	if scope.Used() != 0 {
		t.Fatalf("memory leak=%d", scope.Used())
	}
}

// TestFileWriterFactoryRejectsWholeFileParquetCompressionBeforeOpen 验证非法组合不会先创建远端 incomplete file。
func TestFileWriterFactoryRejectsWholeFileParquetCompressionBeforeOpen(t *testing.T) {
	mm := memory.NewManager(4 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	d := &factoryTestDriver{caps: vfs.Capabilities(vfs.StreamingWrite)}
	registry, _ := vfs.NewRegistry(d)
	pool, _ := NewWriterPool(1, 32<<10, scope)
	_, err := NewFileWriterFactory(FileWriterConfig{Schema: []Column{{Name: "id", Type: types.LogicalType{ID: types.Integer}}}, Format: FormatParquet, Compression: "gzip"}, registry, scope, bm, pool)
	if err == nil {
		t.Fatal("expected Parquet whole-file compression rejection")
	}
	if d.opens != 0 || pool.Active() != 0 || scope.Used() != 0 {
		t.Fatalf("illegal config created side effect opens=%d active=%d used=%d", d.opens, pool.Active(), scope.Used())
	}
}

// TestFileWriterFactoryConstructorFailureAbortsVFS 验证格式构造失败不会遗留已打开的远端文件。
func TestFileWriterFactoryConstructorFailureAbortsVFS(t *testing.T) {
	mm := memory.NewManager((64 << 10) + 64)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	d := &factoryTestDriver{caps: vfs.Capabilities(vfs.StreamingWrite)}
	registry, _ := vfs.NewRegistry(d)
	pool, _ := NewWriterPool(1, 64, scope)
	factory, err := NewFileWriterFactory(FileWriterConfig{Schema: []Column{{Name: "x", Type: types.LogicalType{ID: types.Varchar}}}, Format: FormatCSV, Compression: "identity", CSV: CSVWriteOptions{BufferSize: 128 << 10}}, registry, scope, bm, pool)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := vfs.ParseURI("factorytest://host/out.csv")
	_, err = factory.Open(context.Background(), u, vfs.WriteOptions{})
	if !errors.Is(err, memory.ErrLimitExceeded) {
		t.Fatalf("expected memory limit, got %v", err)
	}
	if d.last == nil || !d.last.aborted {
		t.Fatal("opened VFS sink was not aborted after format constructor failure")
	}
	if pool.Active() != 0 || scope.Used() != 0 {
		t.Fatalf("failure leak active=%d used=%d", pool.Active(), scope.Used())
	}
}

package parquet

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// runtimeBindDriver 在现有 recordingDriver 上补齐 FileSetPlanner 所需的 Stat。
type runtimeBindDriver struct {
	*recordingDriver
	uri vfs.URI
}

func (d *runtimeBindDriver) Stat(context.Context, vfs.URI) (vfs.ObjectInfo, error) {
	identity := vfs.ObjectIdentity{ETag: "runtime-bind", Size: int64(len(d.data))}
	return vfs.ObjectInfo{URI: d.uri, Size: int64(len(d.data)), Identity: identity}, nil
}

// TestNewRuntimeSourceReusesRealBindFingerprint 验证 production constructor 真正经过 FileSetPlanner/SourceBinder/MetadataCache，并在扫描前核对 frozen fingerprint。
func TestNewRuntimeSourceReusesRealBindFingerprint(t *testing.T) {
	data := parquetFile(minimalMetadata())
	uri, err := vfs.ParseURI("mem://x/runtime.parquet")
	if err != nil {
		t.Fatal(err)
	}
	driver := &runtimeBindDriver{recordingDriver: &recordingDriver{data: data}, uri: uri}
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := NewMetadataCache(4)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	binder, err := NewSourceBinder(registry, cache, scope, buffers)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := binder.Bind(context.Background(), uri.String(), SourceBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if binding.Fingerprint() == "" {
		t.Fatal("empty binding fingerprint")
	}

	if source, err := NewRuntimeSource(context.Background(), RuntimeSourceConfig{
		Binder: binder, Pattern: uri.String(), ExpectedFingerprint: "different-fingerprint",
	}); err == nil || source != nil {
		t.Fatalf("mismatched source=%v err=%v", source, err)
	}

	source, err := NewRuntimeSource(context.Background(), RuntimeSourceConfig{
		Binder: binder, Pattern: uri.String(), ExpectedFingerprint: binding.Fingerprint(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if chunk, ok, err := source.Next(context.Background()); err != nil || ok || chunk != nil {
		t.Fatalf("zero-row-group source chunk=%v ok=%v err=%v", chunk, ok, err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

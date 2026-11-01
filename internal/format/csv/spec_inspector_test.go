package csv

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/introspection"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func TestSpecInspectorMatchesDirectBindBackedInspection(t *testing.T) {
	data := []byte("id,name\n1,alpha\n2,beta\n")
	driver := newPhase3MemoryDriver(t, data, vfs.Capabilities(vfs.RandomRead))
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	binder, err := NewSourceBinder(registry, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	opts := SourceBindOptions{Inference: InferenceOptions{Header: true, MaxRows: 100}}
	adapter, err := NewSpecInspector(binder, opts)
	if err != nil {
		t.Fatal(err)
	}
	direct, err := binder.InspectSource(context.Background(), driver.uri.String(), opts)
	if err != nil {
		t.Fatal(err)
	}
	throughSpec, err := adapter.InspectSource(context.Background(), introspection.SourceSpec{Format: introspection.SourceFormatCSV, Pattern: driver.uri.String()})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := throughSpec.Columns(), direct.Columns(); len(got) != len(want) {
		t.Fatalf("columns len=%d want=%d", len(got), len(want))
	} else {
		for i := range got {
			if got[i].Name != want[i].Name || got[i].TypeName != want[i].TypeName {
				t.Fatalf("column %d got=%+v want=%+v", i, got[i], want[i])
			}
		}
	}
	if throughSpec.DatasetFingerprint != direct.DatasetFingerprint || throughSpec.SchemaSource != introspection.SchemaSourceSampleInferred {
		t.Fatalf("report=%+v direct=%+v", throughSpec, direct)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed bytes leaked: %d", scope.Used())
	}
}

func TestSpecInspectorRejectsParquetOnlyOptions(t *testing.T) {
	data := []byte("id\n1\n")
	driver := newPhase3MemoryDriver(t, data, vfs.Capabilities(vfs.RandomRead))
	registry, _ := vfs.NewRegistry(driver)
	manager := memory.NewManager(1 << 20)
	binder, _ := NewSourceBinder(registry, manager.NewScope(), memory.NewBufferManager(nil))
	adapter, _ := NewSpecInspector(binder, SourceBindOptions{Inference: InferenceOptions{Header: true}})
	_, err := adapter.InspectSource(context.Background(), introspection.SourceSpec{Format: introspection.SourceFormatCSV, Pattern: driver.uri.String(), FullValidation: true})
	if !errors.Is(err, introspection.ErrInvalidSourceSpec) {
		t.Fatalf("error=%v, want invalid source spec", err)
	}
}

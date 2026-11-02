package json

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func TestFrameJSONRecordsObjectArrayAndUnstructured(t *testing.T) {
	cases := []struct {
		input string
		want  int
	}{
		{`[{"id":1,"text":"} not structure"},{"id":2,"nested":{"x":[1,2]}}]`, 2},
		{"{\"id\":1}\n  {\"id\":2}", 2},
		{`{"id":1}`, 1},
	}
	// raw string fixtures above intentionally use JSON escapes only where Go string syntax requires them.
	cases[0].input = `[{"id":1,"text":"} not structure"},{"id":2,"nested":{"x":[1,2]}}]`
	cases[0].input = string([]byte{'[', '{', '"', 'i', 'd', '"', ':', '1', ',', '"', 't', 'e', 'x', 't', '"', ':', '"', '}', ' ', 'n', 'o', 't', ' ', 's', 't', 'r', 'u', 'c', 't', 'u', 'r', 'e', '"', '}', ',', '{', '"', 'i', 'd', '"', ':', '2', ',', '"', 'n', 'e', 's', 't', 'e', 'd', '"', ':', '{', '"', 'x', '"', ':', '[', '1', ',', '2', ']', '}', '}', ']'})
	cases[2].input = `{"id":1}`
	cases[2].input = `{"id":1}`[0:0] + `{"id":1}`
	// Replace escaped raw literals with the intended JSON bytes.
	cases[2].input = "{\"id\":1}"
	for _, tc := range cases {
		records, err := FrameJSONRecords([]byte(tc.input), true, 32)
		if err != nil {
			t.Fatalf("%s: %v", tc.input, err)
		}
		if len(records) != tc.want {
			t.Fatalf("%s records=%d want=%d", tc.input, len(records), tc.want)
		}
		if _, err := InferRecords(records, 32); err != nil {
			t.Fatalf("%s inference: %v", tc.input, err)
		}
	}
}

func TestFrameJSONRecordsBoundedPartialArray(t *testing.T) {
	records, err := FrameJSONRecords([]byte("[{\"id\":1},{\"id\":2"), false, 32)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("records=%d want=1", len(records))
	}
	if _, err := FrameJSONRecords([]byte("[{\"id\":"), false, 32); !errors.Is(err, errJSONNeedMoreData) {
		t.Fatalf("err=%v want need-more-data", err)
	}
}

func TestFrameJSONRecordsRejectsMalformedStructure(t *testing.T) {
	for _, input := range []string{
		"[{\"id\":1} {\"id\":2}]",
		"[1,2]",
		"{\"id\":1]",
	} {
		if _, err := FrameJSONRecords([]byte(input), true, 32); err == nil {
			t.Fatalf("expected malformed rejection: %s", input)
		}
	}
}

func TestJSONBinderUsesFrozenScanSetAndExistingInference(t *testing.T) {
	data := []byte("[{\"id\":1,\"name\":\"a\"},{\"id\":2,\"name\":\"b\"}]")
	driver := newMem(t, data, vfs.Capabilities(vfs.RandomRead))
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewManager(4 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	binder, err := NewJSONBinder(registry, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := binder.Bind(context.Background(), driver.uri.String(), JSONBindOptions{SampleBytes: 16, MaxSampleBytes: int64(len(data))})
	if err != nil {
		t.Fatal(err)
	}
	if binding.ScanSet() == nil || binding.Fingerprint() == "" || binding.MatchedFiles() != 1 {
		t.Fatalf("binding scan identity missing: %+v", binding)
	}
	schema := binding.Schema()
	if len(schema.Fields) != 2 || schema.Fields[0].Name != "id" || schema.Fields[0].Type.ID != types.BigInt || schema.Fields[1].Name != "name" || schema.Fields[1].Type.ID != types.Varchar {
		t.Fatalf("schema=%+v", schema.Fields)
	}
	report, err := binder.InspectSource(context.Background(), driver.uri.String(), JSONBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if report.DatasetFingerprint != binding.Fingerprint() || len(report.Columns()) != 2 {
		t.Fatalf("report=%+v", report)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed sample leak=%d", scope.Used())
	}
}

func TestJSONBinderHonorsMaxSampleWithoutWholeFileFallback(t *testing.T) {
	data := []byte("[{\"payload\":\"this value is deliberately longer than the bounded sample\"}]")
	driver := newMem(t, data, vfs.Capabilities(vfs.RandomRead))
	registry, _ := vfs.NewRegistry(driver)
	manager := memory.NewManager(4 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	binder, _ := NewJSONBinder(registry, scope, bm)
	if _, err := binder.Bind(context.Background(), driver.uri.String(), JSONBindOptions{SampleBytes: 8, MaxSampleBytes: 16}); !errors.Is(err, errJSONNeedMoreData) {
		t.Fatalf("err=%v want bounded need-more-data", err)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed sample leak=%d", scope.Used())
	}
}

func TestJSONBinderRequiresRandomRead(t *testing.T) {
	data := []byte("{\"id\":1}")
	driver := newMem(t, data, vfs.Capabilities(vfs.StreamingRead))
	registry, _ := vfs.NewRegistry(driver)
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	binder, _ := NewJSONBinder(registry, scope, memory.NewBufferManager(nil))
	if _, err := binder.Bind(context.Background(), driver.uri.String(), JSONBindOptions{}); err == nil {
		t.Fatal("expected RandomRead capability error")
	}
}

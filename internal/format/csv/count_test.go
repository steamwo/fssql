package csv

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/storage/vfs"
)

func TestCountRecordsQuotesWidthAndPartialTail(t *testing.T) {
	for _, tc := range []struct {
		input          string
		final          bool
		rows, consumed int
		bad            bool
	}{
		{"1,\"two\nlines\"\r\n2,\"a\"\"b\"", true, 2, 25, false},
		{"1,a\n2,\"partial", false, 1, 4, false},
		{"1,a,b\n", true, 0, 0, true},
		{"1\n", true, 0, 0, true},
		{"1,\"partial", true, 0, 0, true},
	} {
		n, used, err := countCSVRecords([]byte(tc.input), DefaultLexConfig(), tc.final, 2048, 2)
		if (err != nil) != tc.bad {
			t.Fatalf("input=%q err=%v", tc.input, err)
		}
		if tc.bad {
			continue
		}
		wantUsed := tc.consumed
		if tc.final {
			wantUsed = len(tc.input)
		}
		if n != tc.rows || used != wantUsed {
			t.Fatalf("rows=%d used=%d input=%q", n, used, tc.input)
		}
	}
}

func TestCSVCountSourceAvoidsPayloadVectors(t *testing.T) {
	driver := newPhase3MemoryDriver(t, makeParallelFixture(5000), vfs.Capabilities(vfs.RandomRead))
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	m, scope, bm := testMemory(8 << 20)
	binder, err := NewSourceBinder(registry, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := binder.Bind(context.Background(), driver.uri.String(), SourceBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewCountSource(context.Background(), RuntimeSourceConfig{Binder: binder, Pattern: driver.uri.String(), ExpectedFingerprint: binding.Fingerprint()})
	if err != nil {
		t.Fatal(err)
	}
	out, ok, err := source.Next(context.Background())
	if err != nil || !ok {
		t.Fatalf("next=%v %v", ok, err)
	}
	// Default Bind has no header option, so this fixture's header is a record.
	if count, valid := out.Columns[0].Int64(0); !valid || count != 5001 {
		t.Fatalf("count=%d valid=%v", count, valid)
	}
	out.Release()
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if m.Used() != 0 {
		t.Fatalf("managed leak=%d", m.Used())
	}
}

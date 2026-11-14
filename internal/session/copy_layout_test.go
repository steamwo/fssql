package session

import (
	"context"
	"testing"

	exportpkg "github.com/steamwo/fssql/internal/export"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func TestCopyParquetLayoutJobIdentity(t *testing.T) {
	ids := make(map[string]string)
	for _, tc := range []struct {
		name        string
		group, page int
	}{
		{"default", 0, 0},
		{"explicit_default", 2048, 512},
		{"large", 8192, 2048},
		{"group_only", 8192, 0},
		{"explicit_page_default", 8192, 512},
		{"page_only", 0, 2048},
	} {
		t.Run(tc.name, func(t *testing.T) {
			driver := &copyTestAtomicDriver{caps: vfs.Capabilities(vfs.StreamingWrite, vfs.AtomicPublishWrite, vfs.ConditionalWrite)}
			engine, manager, store := newCopyEngineHarness(t, driver)
			q := query.New(context.Background(), manager)
			defer q.Close()
			const sql = "SELECT 1 AS id"
			const destination = "copytest://host/layout.parquet"
			prepared, err := engine.Prepare(q.Context(), q, sql)
			if err != nil {
				t.Fatal(err)
			}
			result, err := engine.CopyToPrepared(q, prepared, CopyRequest{
				Destination: destination, Format: "parquet",
				ParquetRowGroupRows: tc.group, ParquetPageRows: tc.page,
			}, sql)
			if err != nil || result.Rows != 1 || result.Bytes == 0 {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if store.last == nil || store.last.ExportState != exportpkg.ExportStateCommitted {
				t.Fatalf("manifest=%+v", store.last)
			}
			ids[tc.name] = store.last.JobID
			if tc.name == "default" {
				snapshot, _ := q.SourceSnapshot()
				legacy := fingerprintText(fingerprintText(sql) + "\x00" + snapshot.Fingerprint + "\x00" + destination + "\x00parquet\x00snappy")
				if store.last.JobID != legacy {
					t.Fatal("default layout changed existing job identity")
				}
			}
			if err := q.Close(); err != nil || manager.Used() != 0 {
				t.Fatalf("close=%v used=%d", err, manager.Used())
			}
		})
	}
	if ids["default"] != ids["explicit_default"] || ids["group_only"] != ids["explicit_page_default"] {
		t.Fatal("equivalent layouts have different job identities")
	}
	seen := make(map[string]bool)
	for _, name := range []string{"default", "large", "group_only", "page_only"} {
		if ids[name] == "" || seen[ids[name]] {
			t.Fatalf("layout %s reused job identity %q", name, ids[name])
		}
		seen[ids[name]] = true
	}
}

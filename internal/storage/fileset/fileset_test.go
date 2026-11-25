package fileset

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/local"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type fakeDriver struct {
	pages    map[string]vfs.ListPage
	stat     vfs.ObjectInfo
	prefixes []string
	tokens   []string
}

func (f *fakeDriver) Scheme() string { return "s3" }
func (f *fakeDriver) StaticCapabilities() vfs.CapabilitySet {
	return vfs.Capabilities(vfs.RandomRead, vfs.ServerSideList, vfs.StableETag)
}
func (f *fakeDriver) Probe(context.Context) (vfs.ProbeResult, error) {
	return vfs.ProbeResult{Supported: f.StaticCapabilities(), Endpoint: "https://s3.test", CredentialScope: "x", SecurityProfile: "tls"}, nil
}
func (f *fakeDriver) Stat(context.Context, vfs.URI) (vfs.ObjectInfo, error) { return f.stat, nil }
func (f *fakeDriver) List(_ context.Context, _ vfs.URI, opts vfs.ListOptions) (vfs.ListPage, error) {
	f.prefixes = append(f.prefixes, opts.Prefix)
	f.tokens = append(f.tokens, opts.ContinuationToken)
	p, ok := f.pages[opts.ContinuationToken]
	if !ok {
		return vfs.ListPage{}, errors.New("missing fake page")
	}
	return p, nil
}
func (f *fakeDriver) ReadRange(context.Context, vfs.RangeRequest, *memory.Scope, *memory.BufferManager) (vfs.RangeResult, error) {
	return vfs.RangeResult{}, errors.New("unused")
}

func mustURI(t *testing.T, s string) vfs.URI {
	t.Helper()
	u, err := vfs.ParseURI(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestPlannerPaginatesPrunesGlobAndDiscoversHive(t *testing.T) {
	d := &fakeDriver{pages: map[string]vfs.ListPage{
		"": {
			Entries: []vfs.ListEntry{
				{URI: mustURI(t, "s3://bucket/data/year=2024/month=01/a.parquet"), Size: 10, Identity: vfs.ObjectIdentity{ETag: "a", Size: 10}},
				{URI: mustURI(t, "s3://bucket/data/year=2024/month=01/a.csv"), Size: 11, Identity: vfs.ObjectIdentity{ETag: "x", Size: 11}},
			},
			Truncated: true, NextContinuationToken: "next",
		},
		"next": {
			Entries: []vfs.ListEntry{
				{URI: mustURI(t, "s3://bucket/data/year=2024/month=02/b.parquet"), Size: 12, Identity: vfs.ObjectIdentity{ETag: "b", Size: 12}},
			},
		},
	}}
	reg, err := vfs.NewRegistry(d)
	if err != nil {
		t.Fatal(err)
	}
	set, err := NewPlanner(reg).Plan(context.Background(), "s3://bucket/data/year=2024/**/*.parquet", PlanOptions{HivePartitioning: true, MaxFiles: 10, ListPageSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if set.MatchedFiles() != 2 {
		t.Fatalf("matched=%d", set.MatchedFiles())
	}
	if got := set.ListingPrefix(); got != "data/year=2024/" {
		t.Fatalf("prefix=%q", got)
	}
	if len(d.prefixes) != 2 || d.prefixes[0] != "data/year=2024/" || d.tokens[1] != "next" {
		t.Fatalf("list calls prefixes=%v tokens=%v", d.prefixes, d.tokens)
	}
	files := set.Files()
	if files[0].Partitions["year"] != "2024" || files[0].Partitions["month"] != "01" {
		t.Fatalf("partitions=%v", files[0].Partitions)
	}
	if set.Fingerprint() == "" {
		t.Fatal("empty fingerprint")
	}
}

type walkTestDriver struct {
	vfs.Driver
	entries []vfs.ListEntry
	walks   int
	visits  int
}

func (d *walkTestDriver) WalkList(_ context.Context, _ vfs.URI, prefix string, visit func(vfs.ListEntry) error) error {
	if prefix != "data/" {
		return fmt.Errorf("unexpected walk prefix %q", prefix)
	}
	d.walks++
	for _, entry := range d.entries {
		d.visits++
		if err := visit(entry); err != nil {
			return err
		}
	}
	return nil
}

func TestPlannerWalksOnceAndStopsAtFileLimit(t *testing.T) {
	pages := &fakeDriver{}
	d := &walkTestDriver{Driver: pages}
	for _, name := range []string{"b.csv", "a.csv", "c.json"} {
		d.entries = append(d.entries, vfs.ListEntry{URI: mustURI(t, "s3://bucket/data/"+name)})
	}
	reg, err := vfs.NewRegistry(d)
	if err != nil {
		t.Fatal(err)
	}
	planner := NewPlanner(reg)
	set, err := planner.Plan(context.Background(), "s3://bucket/data/*.csv", PlanOptions{ListPageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if files := set.Files(); len(pages.tokens) != 0 || d.walks != 1 || len(files) != 2 || files[0].URI.ObjectKey() != "data/a.csv" {
		t.Fatalf("pages=%v, walks=%d, files=%v", pages.tokens, d.walks, files)
	}
	d.visits = 0
	_, err = planner.Plan(context.Background(), "s3://bucket/data/*.csv", PlanOptions{MaxFiles: 1})
	if !errors.Is(err, ErrFileLimit) || d.visits != 2 {
		t.Fatalf("visits=%d, err=%v", d.visits, err)
	}
}

func BenchmarkPlannerLocalGlob(b *testing.B) {
	root := b.TempDir()
	for _, dir := range []string{"orders", "unrelated"} {
		for day := 0; day < 22; day++ {
			folder := filepath.Join(root, dir, fmt.Sprintf("day-%02d", day))
			if err := os.MkdirAll(folder, 0o700); err != nil {
				b.Fatal(err)
			}
			for part := 0; part < 100; part++ {
				if err := os.WriteFile(filepath.Join(folder, fmt.Sprintf("part-%02d.csv", part)), nil, 0o600); err != nil {
					b.Fatal(err)
				}
			}
		}
	}
	driver, err := local.NewDriver(root)
	if err != nil {
		b.Fatal(err)
	}
	pattern, err := local.URIFromPath(filepath.Join(root, "orders", "**", "*.csv"))
	if err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		driver vfs.Driver
	}{
		{"walk", driver},
		{"pages", struct{ vfs.Driver }{driver}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			reg, err := vfs.NewRegistry(tc.driver)
			if err != nil {
				b.Fatal(err)
			}
			planner := NewPlanner(reg)
			ctx := context.Background()
			raw := pattern.String()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				set, err := planner.Plan(ctx, raw, PlanOptions{})
				if err != nil || set.MatchedFiles() != 2200 {
					b.Fatalf("unexpected local glob result: %v", err)
				}
			}
		})
	}
}

func TestScanSetIsImmutableToCallers(t *testing.T) {
	f := FileSnapshot{URI: mustURI(t, "s3://b/x.parquet"), Size: 1, Identity: vfs.ObjectIdentity{ETag: "e", Size: 1}, Partitions: map[string]string{"p": "1"}}
	set := newScanSet("x", "", []FileSnapshot{f})
	before := set.Fingerprint()
	got := set.Files()
	got[0].Partitions["p"] = "changed"
	got[0].Size = 99
	after := set.Files()
	if after[0].Partitions["p"] != "1" || after[0].Size != 1 {
		t.Fatalf("scan set mutated: %+v", after[0])
	}
	if set.Fingerprint() != before {
		t.Fatal("fingerprint changed")
	}
}

func TestPlannerDirectFileUsesStat(t *testing.T) {
	u := mustURI(t, "s3://b/a.parquet")
	d := &fakeDriver{stat: vfs.ObjectInfo{URI: u, Size: 5, Identity: vfs.ObjectIdentity{VersionID: "v1", Size: 5}}, pages: map[string]vfs.ListPage{}}
	reg, _ := vfs.NewRegistry(d)
	set, err := NewPlanner(reg).Plan(context.Background(), u.String(), PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if set.MatchedFiles() != 1 || set.Files()[0].Identity.VersionID != "v1" {
		t.Fatalf("set=%+v", set.Files())
	}
}

type httpDriver struct{ *fakeDriver }

func (*httpDriver) Scheme() string { return "https" }

func TestHTTPQueryIsNotAGlobAndSignaturesAffectFingerprint(t *testing.T) {
	var fingerprints []string
	for _, raw := range []string{"https://example.test/data?sig=one&literal=?*[", "https://example.test/data?sig=two&literal=?*["} {
		u := mustURI(t, raw)
		d := &httpDriver{&fakeDriver{stat: vfs.ObjectInfo{URI: u, Size: 5, Identity: vfs.ObjectIdentity{ETag: "same", Size: 5}}}}
		reg, _ := vfs.NewRegistry(d)
		set, err := NewPlanner(reg).Plan(context.Background(), raw, PlanOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(d.prefixes) != 0 || set.Files()[0].URI.RequestURL() != raw {
			t.Fatal("HTTP request interpreted as a glob")
		}
		fingerprints = append(fingerprints, set.Fingerprint())
	}
	if fingerprints[0] == fingerprints[1] {
		t.Fatal("signatures lost in fingerprint")
	}
	reg, _ := vfs.NewRegistry(&httpDriver{&fakeDriver{}})
	for _, raw := range []string{"https://example.test/*.csv?sig=secret", "https://example.test/[ab].csv?sig=secret"} {
		if _, err := NewPlanner(reg).Plan(context.Background(), raw, PlanOptions{}); err == nil {
			t.Fatal("HTTP glob accepted")
		}
	}
}

func TestFingerprintIncludesSnapshotAndViewSpec(t *testing.T) {
	f := FileSnapshot{URI: mustURI(t, "https://example.test/data?sig=one"), Size: 5, Identity: vfs.ObjectIdentity{Size: 5, SnapshotID: "sha256:one"}}
	base := datasetFingerprint([]FileSnapshot{f})
	f.Identity.SnapshotID = "sha256:two"
	if base == datasetFingerprint([]FileSnapshot{f}) {
		t.Fatal("SnapshotID omitted")
	}
	f.Identity.SnapshotID = "sha256:one"
	f.URI.ViewKey = "another-representation"
	if base == datasetFingerprint([]FileSnapshot{f}) {
		t.Fatal("view spec omitted")
	}
}

func TestPlannerRejectsRepeatedContinuationToken(t *testing.T) {
	u := mustURI(t, "s3://b/a.parquet")
	d := &fakeDriver{pages: map[string]vfs.ListPage{
		"":     {Entries: []vfs.ListEntry{{URI: u, Size: 1}}, Truncated: true, NextContinuationToken: "same"},
		"same": {Truncated: true, NextContinuationToken: "same"},
	}}
	reg, _ := vfs.NewRegistry(d)
	_, err := NewPlanner(reg).Plan(context.Background(), "s3://b/*.parquet", PlanOptions{})
	if !errors.Is(err, ErrInvalidListing) {
		t.Fatalf("got %v", err)
	}
}

func TestPlannerEnforcesFileLimit(t *testing.T) {
	d := &fakeDriver{pages: map[string]vfs.ListPage{"": {Entries: []vfs.ListEntry{
		{URI: mustURI(t, "s3://b/a.parquet")},
		{URI: mustURI(t, "s3://b/b.parquet")},
	}}}}
	reg, _ := vfs.NewRegistry(d)
	_, err := NewPlanner(reg).Plan(context.Background(), "s3://b/*.parquet", PlanOptions{MaxFiles: 1})
	if !errors.Is(err, ErrFileLimit) {
		t.Fatalf("got %v", err)
	}
}

func TestDoubleStarGlob(t *testing.T) {
	cases := []struct {
		pattern, key string
		want         bool
	}{
		{"data/**/*.parquet", "data/a.parquet", true},
		{"data/**/*.parquet", "data/x/y/a.parquet", true},
		{"data/*.parquet", "data/x/a.parquet", false},
	}
	for _, tc := range cases {
		got, err := matchPath(tc.pattern, tc.key)
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Fatalf("match %q %q=%v want %v", tc.pattern, tc.key, got, tc.want)
		}
	}
}

package fileset

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
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

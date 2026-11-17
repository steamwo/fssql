package parquet

import (
	"testing"

	"github.com/steamwo/fssql/internal/storage/vfs"
)

func cacheSource(t *testing.T, raw, etag string, size int64) FooterSource {
	t.Helper()
	u, err := vfs.ParseURI(raw)
	if err != nil {
		t.Fatal(err)
	}
	return FooterSource{URI: u, Size: size, Identity: vfs.ObjectIdentity{ETag: etag, Size: size}}
}

func TestMetadataCacheStableIdentityAndEviction(t *testing.T) {
	cache, err := NewMetadataCache(2)
	if err != nil {
		t.Fatal(err)
	}
	a := cacheSource(t, "s3://bucket/a.parquet", "a1", 100)
	b := cacheSource(t, "s3://bucket/b.parquet", "b1", 200)
	c := cacheSource(t, "s3://bucket/c.parquet", "c1", 300)

	if !cache.Put(a, Footer{MetadataLength: 11}) || !cache.Put(b, Footer{MetadataLength: 22}) {
		t.Fatal("stable objects must be cacheable")
	}
	if got, ok := cache.Get(a); !ok || got.MetadataLength != 11 {
		t.Fatalf("a hit=%v footer=%+v", ok, got)
	}
	// Touching a makes b the LRU entry; inserting c must evict b.
	cache.Put(c, Footer{MetadataLength: 33})
	if _, ok := cache.Get(b); ok {
		t.Fatal("b should have been evicted")
	}
	if got, ok := cache.Get(a); !ok || got.MetadataLength != 11 {
		t.Fatalf("a should remain cached: hit=%v footer=%+v", ok, got)
	}
	m := cache.Metrics()
	if m.Entries != 2 || m.Evictions != 1 || m.Hits != 2 {
		t.Fatalf("metrics=%+v", m)
	}
}

func TestMetadataCacheIdentityChangeMisses(t *testing.T) {
	cache, _ := NewMetadataCache(2)
	src := cacheSource(t, "s3://bucket/a.parquet", "etag-v1", 100)
	cache.Put(src, Footer{MetadataLength: 7})

	changedETag := src
	changedETag.Identity.ETag = "etag-v2"
	if _, ok := cache.Get(changedETag); ok {
		t.Fatal("changed ETag must miss")
	}
	changedSize := src
	changedSize.Size = 101
	changedSize.Identity.Size = 101
	if _, ok := cache.Get(changedSize); ok {
		t.Fatal("changed size must miss")
	}
}

func TestMetadataCacheRejectsUnstableIdentity(t *testing.T) {
	cache, _ := NewMetadataCache(1)
	u, err := vfs.ParseURI("s3://bucket/a.parquet")
	if err != nil {
		t.Fatal(err)
	}
	src := FooterSource{URI: u, Size: 100, Identity: vfs.ObjectIdentity{Size: 100}}
	if cache.Put(src, Footer{MetadataLength: 9}) {
		t.Fatal("unstable identity must bypass cache")
	}
	if _, ok := cache.Get(src); ok {
		t.Fatal("unstable identity must never hit")
	}
}

func TestHTTPMetadataIsQueryLocalAndUsesFullIdentity(t *testing.T) {
	shared, _ := NewMetadataCache(8)
	first, second := NewQueryMetadataCache(shared), NewQueryMetadataCache(shared)
	a := cacheSource(t, "https://example.test/data?sig=one", "same", 100)
	b := cacheSource(t, "https://example.test/data?sig=two", "same", 100)
	if shared.Put(a, Footer{}) {
		t.Fatal("HTTP footer entered cross-query cache")
	}
	if !first.Put(a, Footer{MetadataLength: 3}) {
		t.Fatal("query cache rejected HTTP footer")
	}
	if _, ok := first.Get(b); ok {
		t.Fatal("different signed URLs shared footer")
	}
	if _, ok := second.Get(a); ok {
		t.Fatal("queries shared HTTP footer")
	}
	a.Identity = vfs.ObjectIdentity{SnapshotID: "sha256:one", Size: 100}
	if !first.Put(a, Footer{MetadataLength: 5}) {
		t.Fatal("query cache rejected materialized identity")
	}
	a.Identity.SnapshotID = "sha256:two"
	if _, ok := first.Get(a); ok {
		t.Fatal("different content shared footer")
	}
	local := cacheSource(t, "s3://bucket/data", "stable", 100)
	first.Put(local, Footer{MetadataLength: 7})
	if footer, ok := second.Get(local); !ok || footer.MetadataLength != 7 {
		t.Fatal("existing cross-query cache bypassed")
	}
	first.Clear()
	if first.Metrics().Entries != 0 {
		t.Fatal("query cache not released")
	}
}

func TestMetadataCacheVersionIDSeparatesVersions(t *testing.T) {
	cache, _ := NewMetadataCache(2)
	u, err := vfs.ParseURI("s3://bucket/versioned.parquet")
	if err != nil {
		t.Fatal(err)
	}
	v1 := FooterSource{URI: u, Size: 100, Identity: vfs.ObjectIdentity{VersionID: "v1", Size: 100}}
	v2 := FooterSource{URI: u, Size: 100, Identity: vfs.ObjectIdentity{VersionID: "v2", Size: 100}}
	cache.Put(v1, Footer{MetadataLength: 1})
	cache.Put(v2, Footer{MetadataLength: 2})
	got1, ok1 := cache.Get(v1)
	got2, ok2 := cache.Get(v2)
	if !ok1 || !ok2 || got1.MetadataLength != 1 || got2.MetadataLength != 2 {
		t.Fatalf("versioned cache values: v1=%+v/%v v2=%+v/%v", got1, ok1, got2, ok2)
	}
}

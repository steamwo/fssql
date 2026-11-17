package vfs

import (
	"fmt"
	"strings"
	"testing"
)

func TestHTTPURITransportDisplayAndIdentity(t *testing.T) {
	for _, raw := range []string{
		"https://example.test/a%2fb//./c?b=secret&a=one&a=&wild=?*[%2f",
		"https://example.test?", "https://example.test/", "http://example.test/a%2Fb?token=secret",
	} {
		u, err := ParseURI(raw)
		if err != nil {
			t.Fatal(err)
		}
		if u.RequestURL() != raw {
			t.Fatalf("request changed: %q != %q", u.RequestURL(), raw)
		}
		if strings.Contains(u.String(), "secret") || strings.Contains(u.Raw, "secret") || strings.Contains(fmt.Sprintf("%#v", u), "secret") {
			t.Fatal("query value leaked")
		}
		u.ViewID, u.ViewKey = "query-view", "representation"
		if u.RequestURL() != raw || strings.Contains(u.String(), "query-view") {
			t.Fatal("view metadata serialized")
		}
		v, _ := ParseURI(raw)
		if u.IdentityKey() != v.IdentityKey() {
			t.Fatal("view metadata affected physical identity")
		}
	}
	a, _ := ParseURI("https://example.test/x?sig=one")
	b, _ := ParseURI("https://example.test/x?sig=two")
	if a.String() != b.String() || a.IdentityKey() == b.IdentityKey() {
		t.Fatal("signed resource identities collided")
	}
	a, _ = ParseURI("https://example.test/x")
	b, _ = ParseURI("https://example.test/x?")
	if a.IdentityKey() == b.IdentityKey() {
		t.Fatal("empty query separator lost")
	}
}

func TestHTTPURIRejectsUnsafeInputWithoutSecrets(t *testing.T) {
	for _, raw := range []string{
		"https://user:secret@example.test/x", "https://example.test/x?token=secret#",
		"https://example.test/x#secret", "https://example.test/%zz?token=secret", "https:///x?token=secret",
	} {
		_, err := ParseURI(raw)
		if err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("unsafe error: %v", err)
		}
	}
	u, err := ParseURI("s3://BUCKET/a//b/../c")
	if err != nil || u.String() != "s3://bucket/a/c" {
		t.Fatalf("legacy normalization: %v %v", u, err)
	}
}

func TestSnapshotIdentityNeverFallsBack(t *testing.T) {
	a := ObjectIdentity{SnapshotID: "sha256:a", Size: 8}
	for _, b := range []ObjectIdentity{{Size: 8}, {SnapshotID: "sha256:b", Size: 8}, {SnapshotID: a.SnapshotID, Size: 0}} {
		if a.Matches(b) || b.Matches(a) {
			t.Fatalf("snapshot matched: %+v", b)
		}
	}
	if !a.Matches(a) || a.Stable() {
		t.Fatal("snapshot identity contract violated")
	}
}

func FuzzHTTPURI(f *testing.F) {
	f.Add("https://example.test/a%2Fb//c?sig=secret&a=&a=1")
	f.Add("https://example.test?")
	f.Fuzz(func(t *testing.T, raw string) {
		u, err := ParseURI(raw)
		if err != nil || !u.IsHTTP() {
			return
		}
		v, err := ParseURI(u.RequestURL())
		if err != nil || v.IdentityKey() != u.IdentityKey() || v.RequestURL() != u.RequestURL() {
			t.Fatal("transport roundtrip changed identity")
		}
		if u.RawQuery != "" && u.String() == u.RequestURL() && strings.Contains(u.RawQuery, "=secret") {
			t.Fatal("unredacted display")
		}
	})
}

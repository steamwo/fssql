package webdav

import (
	"encoding/xml"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/storage/vfs"
)

func TestApachePublishLockResponseAndLeasePolicy(t *testing.T) {
	// Apache mod_dav_fs 2.4.65's LOCK response omits DAV:lockroot and ETag.
	body := `<D:prop xmlns:D="DAV:"><D:lockdiscovery><D:activelock><D:locktype><D:write/></D:locktype><D:lockscope><D:exclusive/></D:lockscope><D:depth>0</D:depth><D:timeout>Infinite</D:timeout><D:locktoken><D:href>opaquelocktoken:1150899b-62c5-411f-929a-e72339b33828</D:href></D:locktoken></D:activelock></D:lockdiscovery></D:prop>`
	var document struct {
		Locks []activePublishLock `xml:"DAV: lockdiscovery>activelock"`
	}
	if err := xml.Unmarshal([]byte(body), &document); err != nil {
		t.Fatal(err)
	}
	stage, _ := vfs.ParseURI("webdav://example.com/staging")
	g := &publishLock{driver: &Driver{SchemeName: "webdav"}, stage: stage}
	if len(document.Locks) != 1 || !g.valid(document.Locks[0]) {
		t.Fatal("Apache infinite lock rejected")
	}
	l := document.Locks[0]
	for _, bad := range []string{"Second-600", "", "Second-0"} {
		l.Timeout = bad
		if g.valid(l) {
			t.Fatalf("accepted lease %q", bad)
		}
	}
	l = document.Locks[0]
	l.Exclusive = nil
	if g.valid(l) {
		t.Fatal("accepted shared lock")
	}
	l = document.Locks[0]
	l.Depth = "infinity"
	if g.valid(l) {
		t.Fatal("accepted collection lock")
	}
	l = document.Locks[0]
	l.Token += ">) (Not <DAV:no-lock>"
	if g.valid(l) {
		t.Fatal("accepted injected If header")
	}
	for _, proof := range []string{"", "token", strings.Repeat("A", 64)} {
		if g.restore(stage, proof) == nil {
			t.Fatal("invalid ownership proof")
		}
	}
}

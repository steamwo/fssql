package sftp

import (
	"strings"
	"testing"
	"time"
)

func TestOpenSSHRequiresStrictKnownHosts(t *testing.T) {
	f := OpenSSHFactory{Config: OpenSSHConfig{User: "u"}}
	if _, _, err := f.buildArgs(testKey()); err == nil { t.Fatal("expected missing known_hosts failure") }
	f = OpenSSHFactory{Config: OpenSSHConfig{User: "u", KnownHostsFile: "/tmp/known_hosts", ConnectTimeout: 3 * time.Second}}
	_, args, err := f.buildArgs(testKey()); if err != nil { t.Fatal(err) }
	joined := strings.Join(args, " ")
	for _, want := range []string{"StrictHostKeyChecking=yes", "CheckHostIP=yes", "UserKnownHostsFile=/tmp/known_hosts", "BatchMode=yes"} {
		if !strings.Contains(joined, want) { t.Fatalf("args missing %q: %s", want, joined) }
	}
	if strings.Contains(strings.ToLower(joined), "stricthostkeychecking=no") { t.Fatalf("insecure fallback present: %s", joined) }
}
func TestClassifyWrongHostKey(t *testing.T) {
	err := classifySSHError("WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!", ErrConnection)
	if !strings.Contains(err.Error(), ErrHostKey.Error()) { t.Fatalf("err=%v", err) }
}

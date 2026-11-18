package sftp

import (
	"strings"
	"testing"
	"time"
)

func TestOpenSSHRequiresStrictKnownHosts(t *testing.T) {
	f := OpenSSHFactory{Config: OpenSSHConfig{User: "u"}}
	if _, _, err := f.buildArgs(testKey()); err == nil {
		t.Fatal("expected missing known_hosts failure")
	}
	f = OpenSSHFactory{Config: OpenSSHConfig{User: "u", KnownHostsFile: "/tmp/known_hosts", ConnectTimeout: 3 * time.Second}}
	_, args, err := f.buildArgs(testKey())
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(args, " ")
	for _, want := range []string{"StrictHostKeyChecking=yes", "CheckHostIP=yes", "UserKnownHostsFile=/tmp/known_hosts", "BatchMode=yes"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("args missing %q: %s", want, joined)
		}
	}
	if strings.Contains(strings.ToLower(joined), "stricthostkeychecking=no") {
		t.Fatalf("insecure fallback present: %s", joined)
	}
}
func TestClassifyWrongHostKey(t *testing.T) {
	err := classifySSHError("WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!", ErrConnection)
	if !strings.Contains(err.Error(), ErrHostKey.Error()) {
		t.Fatalf("err=%v", err)
	}
}

func TestSSHDiagnosticsRetainsBoundedStartupOutput(t *testing.T) {
	var diagnostics sshDiagnostics
	message := "WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!"
	_, _ = diagnostics.Write([]byte(message))
	later := strings.Repeat("x", 32<<10)
	for range 2 {
		if n, err := diagnostics.Write([]byte(later)); n != len(later) || err != nil {
			t.Fatalf("write = %d, %v", n, err)
		}
	}
	if output := diagnostics.String(); len(output) != 16<<10 || !strings.HasPrefix(output, message) {
		t.Fatalf("diagnostics lost its prefix or exceeded the limit: %d bytes", len(output))
	}
}

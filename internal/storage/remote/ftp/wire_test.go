package ftp

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/security"
	"github.com/steamwo/fssql/internal/storage/remote"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func fixedCredentials() CredentialProvider {
	return CredentialProviderFunc(func(context.Context, remote.ClientKey, string) (Credentials, error) {
		return Credentials{Username: "user", Password: "pass"}, nil
	})
}

func policyForServer(t *testing.T, addr string, allowPlain bool) *security.NetworkPolicy {
	t.Helper()
	_, portText, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	policy := security.NewNetworkPolicy(port)
	policy.AllowLoopback = true
	policy.AllowPlainFTP = allowPlain
	return policy
}

func TestWireSessionPlainFTPProtocol(t *testing.T) {
	server, _ := newProtocolServer(t, false)
	factory := &WireFactory{Scheme: "ftp", Policy: policyForServer(t, server.Addr(), true), Credentials: fixedCredentials()}
	key := remote.ClientKey{Endpoint: server.Addr(), CredentialScope: "test", CredentialVersion: "1", SecurityProfile: "plain-opt-in"}
	session, err := factory.Dial(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	features, err := session.Features(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !features.RestStream || !features.MLSD {
		t.Fatalf("features=%+v", features)
	}

	info, err := session.Stat(context.Background(), "/data.bin")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != 10 || info.ModUnix == 0 {
		t.Fatalf("info=%+v", info)
	}

	buf := make([]byte, 4)
	n, err := session.ReadAt(context.Background(), "/data.bin", buf, 2)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 || string(buf) != "cdef" {
		t.Fatalf("n=%d buf=%q", n, buf)
	}

	entries, err := session.List(context.Background(), "/")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name != "data.bin" || entries[0].Info.Size != 10 {
		t.Fatalf("entries=%+v", entries)
	}

	writer, err := session.OpenWrite(context.Background(), "/out.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(context.Background(), []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	out, err := session.Stat(context.Background(), "/out.bin")
	if err != nil || out.Size != 5 {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	if err := session.Delete(context.Background(), "/out.bin"); err != nil {
		t.Fatal(err)
	}
}

func TestWireFactoryPlainFTPRequiresExplicitPolicyOptIn(t *testing.T) {
	server, _ := newProtocolServer(t, false)
	factory := &WireFactory{Scheme: "ftp", Policy: policyForServer(t, server.Addr(), false), Credentials: fixedCredentials()}
	key := remote.ClientKey{Endpoint: server.Addr(), CredentialScope: "test", CredentialVersion: "1", SecurityProfile: "strict"}
	if _, err := factory.Dial(context.Background(), key); !errors.Is(err, security.ErrPlainFTPForbidden) {
		t.Fatalf("err=%v, want ErrPlainFTPForbidden", err)
	}
}

func TestWireSessionFTPSUsesTLSForControlAndData(t *testing.T) {
	server, roots := newProtocolServer(t, true)
	factory := &WireFactory{
		Scheme: "ftps", Policy: policyForServer(t, server.Addr(), false), Credentials: fixedCredentials(),
		TLSConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
	}
	key := remote.ClientKey{Endpoint: server.Addr(), CredentialScope: "test", CredentialVersion: "1", SecurityProfile: "strict"}
	session, err := factory.Dial(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	buf := make([]byte, 4)
	if n, err := session.ReadAt(context.Background(), "/data.bin", buf, 6); err != nil || n != 4 || string(buf) != "ghij" {
		t.Fatalf("n=%d buf=%q err=%v", n, buf, err)
	}
	writer, err := session.OpenWrite(context.Background(), "/secure.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(context.Background(), []byte("tls")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWireFactoryRejectsUntrustedFTPSCertificate(t *testing.T) {
	server, _ := newProtocolServer(t, true)
	factory := &WireFactory{Scheme: "ftps", Policy: policyForServer(t, server.Addr(), false), Credentials: fixedCredentials()}
	key := remote.ClientKey{Endpoint: server.Addr(), CredentialScope: "test", CredentialVersion: "1", SecurityProfile: "strict"}
	if _, err := factory.Dial(context.Background(), key); err == nil {
		t.Fatal("expected certificate validation failure")
	}
}

func TestWireControlCancellationReturnsContextError(t *testing.T) {
	server, _ := newProtocolServer(t, false)
	factory := &WireFactory{Scheme: "ftp", Policy: policyForServer(t, server.Addr(), true), Credentials: fixedCredentials()}
	key := remote.ClientKey{Endpoint: server.Addr(), CredentialScope: "test", CredentialVersion: "1", SecurityProfile: "plain-opt-in"}
	session, err := factory.Dial(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	// SIZE 先成功，MDTM /slow 不响应，确保取消发生在阻塞 control read 上。
	server.mu.Lock()
	server.files["/slow"] = []byte("x")
	server.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	_, err = session.Stat(ctx, "/slow")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, want context.Canceled", err)
	}
}

func TestSecretCredentialProviderRejectsRotatedVersion(t *testing.T) {
	manager := security.NewSecretManager(nil)
	spec := security.SecretSpec{
		Name: "warehouse", Values: map[string]string{"username": "u", "password": "p"},
		Scope: security.SecretScope{Subjects: []string{"svc"}, Schemes: []string{"ftps"}, Hosts: []string{"example.com"}},
	}
	if err := manager.Create(spec); err != nil {
		t.Fatal(err)
	}
	provider := SecretCredentialProvider{Manager: manager, Subject: "svc"}
	key := remote.ClientKey{Endpoint: "example.com:990", CredentialScope: "warehouse", CredentialVersion: "1", SecurityProfile: "strict"}
	credentials, err := provider.Credentials(context.Background(), key, "ftps")
	if err != nil || credentials.Username != "u" {
		t.Fatalf("credentials=%+v err=%v", credentials, err)
	}
	if _, err := manager.Rotate(spec); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Credentials(context.Background(), key, "ftps"); !errors.Is(err, vfs.ErrCredential) {
		t.Fatalf("err=%v, want vfs.ErrCredential", err)
	}
}

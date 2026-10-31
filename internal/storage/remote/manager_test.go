package remote

import (
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
)

func TestClientManagerReusesExactScope(t *testing.T) {
	var calls atomic.Int64
	m := NewClientManager(4, func(ClientKey) (*http.Client, error) {
		calls.Add(1)
		return &http.Client{}, nil
	})
	key := ClientKey{Endpoint: "https://s3.example", CredentialScope: "tenant-a", CredentialVersion: "v1", SecurityProfile: "tls"}
	a, err := m.Client(key)
	if err != nil {
		t.Fatal(err)
	}
	b, err := m.Client(key)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("expected client reuse")
	}
	if calls.Load() != 1 {
		t.Fatalf("factory calls=%d", calls.Load())
	}
}

func TestClientManagerCredentialVersionSeparatesClient(t *testing.T) {
	m := NewClientManager(4, nil)
	base := ClientKey{Endpoint: "https://s3.example", CredentialScope: "tenant-a", SecurityProfile: "tls"}
	base.CredentialVersion = "v1"
	a, err := m.Client(base)
	if err != nil {
		t.Fatal(err)
	}
	base.CredentialVersion = "v2"
	b, err := m.Client(base)
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("credential rotation must not reuse prior client")
	}
}

func TestClientManagerBounded(t *testing.T) {
	m := NewClientManager(1, nil)
	_, err := m.Client(ClientKey{Endpoint: "https://a", CredentialScope: "x", SecurityProfile: "tls"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.Client(ClientKey{Endpoint: "https://b", CredentialScope: "x", SecurityProfile: "tls"})
	if !errors.Is(err, ErrClientLimit) {
		t.Fatalf("got %v", err)
	}
}

package security

import (
	"errors"
	"sync"
	"testing"
)

type auditRecorder struct {
	mu     sync.Mutex
	events []AuditEvent
}

func (r *auditRecorder) Record(event AuditEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *auditRecorder) snapshot() []AuditEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]AuditEvent(nil), r.events...)
}

func testSecretSpec() SecretSpec {
	return SecretSpec{
		Name:   "warehouse",
		Values: map[string]string{"username": "alice", "password": "top-secret"},
		Scope: SecretScope{
			Subjects: []string{"tenant-a"},
			Schemes:  []string{"sftp", "ftps"},
			Hosts:    []string{"data.example.com", "*.archive.example.com"},
		},
	}
}

func TestSecretManagerEnforcesSubjectAndTargetACL(t *testing.T) {
	audit := &auditRecorder{}
	m := NewSecretManager(audit)
	if err := m.Create(testSecretSpec()); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Resolve("warehouse", "tenant-b", "sftp://data.example.com/file"); !errors.Is(err, ErrSecretUnauthorized) {
		t.Fatalf("subject error = %v, want ErrSecretUnauthorized", err)
	}
	if _, err := m.Resolve("warehouse", "tenant-a", "sftp://evil.example/file"); !errors.Is(err, ErrSecretUnauthorized) {
		t.Fatalf("host error = %v, want ErrSecretUnauthorized", err)
	}
	if _, err := m.Resolve("warehouse", "tenant-a", "ftp://data.example.com/file"); !errors.Is(err, ErrSecretUnauthorized) {
		t.Fatalf("scheme error = %v, want ErrSecretUnauthorized", err)
	}
	if _, err := m.Resolve("warehouse", "tenant-a", "sftp://day1.archive.example.com/file"); err != nil {
		t.Fatalf("wildcard host should be allowed: %v", err)
	}
}

func TestSecretRotationCreatesNewVersionWithoutMutatingOldSnapshot(t *testing.T) {
	m := NewSecretManager(nil)
	spec := testSecretSpec()
	if err := m.Create(spec); err != nil {
		t.Fatal(err)
	}
	old, err := m.Resolve("warehouse", "tenant-a", "sftp://data.example.com/file")
	if err != nil {
		t.Fatal(err)
	}
	if old.Version() != 1 || old.VersionString() != "1" {
		t.Fatalf("old version = %d/%s", old.Version(), old.VersionString())
	}

	spec.Values["password"] = "new-secret"
	version, err := m.Rotate(spec)
	if err != nil {
		t.Fatal(err)
	}
	if version != 2 {
		t.Fatalf("rotated version = %d, want 2", version)
	}
	current, err := m.Resolve("warehouse", "tenant-a", "sftp://data.example.com/file")
	if err != nil {
		t.Fatal(err)
	}
	oldPassword, _ := old.Value("password")
	newPassword, _ := current.Value("password")
	if oldPassword != "top-secret" || newPassword != "new-secret" {
		t.Fatalf("old/new passwords = %q/%q", oldPassword, newPassword)
	}
}

func TestSecretManagerCopiesCreateInput(t *testing.T) {
	m := NewSecretManager(nil)
	spec := testSecretSpec()
	if err := m.Create(spec); err != nil {
		t.Fatal(err)
	}
	spec.Values["password"] = "mutated"
	spec.Scope.Subjects[0] = "attacker"
	resolved, err := m.Resolve("warehouse", "tenant-a", "sftp://data.example.com/file")
	if err != nil {
		t.Fatal(err)
	}
	password, _ := resolved.Value("password")
	if password != "top-secret" {
		t.Fatalf("password = %q, want original snapshot", password)
	}
}

func TestSecretAuditNeverContainsCredentialValues(t *testing.T) {
	audit := &auditRecorder{}
	m := NewSecretManager(audit)
	if err := m.Create(testSecretSpec()); err != nil {
		t.Fatal(err)
	}
	_, _ = m.Resolve("warehouse", "tenant-b", "sftp://data.example.com/file")
	_, _ = m.Resolve("warehouse", "tenant-a", "sftp://data.example.com/file")
	for _, event := range audit.snapshot() {
		if event.Reason == "top-secret" || event.Target == "top-secret" || event.Resource == "top-secret" {
			t.Fatalf("credential leaked into audit event: %#v", event)
		}
	}
}

func TestSecretManagerRejectsDuplicateAndMissingRotation(t *testing.T) {
	m := NewSecretManager(nil)
	spec := testSecretSpec()
	if err := m.Create(spec); err != nil {
		t.Fatal(err)
	}
	if err := m.Create(spec); !errors.Is(err, ErrSecretExists) {
		t.Fatalf("duplicate error = %v, want ErrSecretExists", err)
	}
	spec.Name = "missing"
	if _, err := m.Rotate(spec); !errors.Is(err, ErrSecretNotFound) {
		t.Fatalf("rotate error = %v, want ErrSecretNotFound", err)
	}
}

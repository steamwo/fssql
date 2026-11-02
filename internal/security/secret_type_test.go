package security

import "testing"

func TestSecretTypeMetadataIsSeparateFromCredentialValues(t *testing.T) {
	manager := NewSecretManager(nil)
	spec := SecretSpec{
		Name:   "typed",
		Type:   "S3",
		Values: map[string]string{"key_id": "id", "secret_key": "secret"},
		Scope:  SecretScope{Subjects: []string{"alice"}},
	}
	if err := manager.Create(spec); err != nil {
		t.Fatal(err)
	}
	resolved, err := manager.Resolve("typed", "alice", "")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Type() != "s3" {
		t.Fatalf("type=%q, want s3", resolved.Type())
	}
	if _, ok := resolved.Value("type"); ok {
		t.Fatal("TYPE metadata must not appear in credential Values")
	}
}

func TestSecretRotationPreservesAndCannotChangeType(t *testing.T) {
	manager := NewSecretManager(nil)
	spec := SecretSpec{
		Name:   "typed",
		Type:   "s3",
		Values: map[string]string{"key": "v1"},
		Scope:  SecretScope{Subjects: []string{"alice"}},
	}
	if err := manager.Create(spec); err != nil {
		t.Fatal(err)
	}

	// 空 Type 表示 credential-only rotation，必须保留已有 adapter metadata。
	spec.Type = ""
	spec.Values["key"] = "v2"
	if _, err := manager.Rotate(spec); err != nil {
		t.Fatal(err)
	}
	resolved, err := manager.Resolve("typed", "alice", "")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Type() != "s3" {
		t.Fatalf("type after rotation=%q", resolved.Type())
	}

	// 同名 secret 不允许通过 credential rotation 静默切换 adapter 类型。
	spec.Type = "sftp"
	if _, err := manager.Rotate(spec); err == nil {
		t.Fatal("expected secret type change rejection")
	}
}

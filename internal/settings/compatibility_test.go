package settings

import (
	"errors"
	"testing"
)

// newCompatibilityRegistry 创建包含安全敏感 plain-FTP 开关的最小配置注册表。
func newCompatibilityRegistry(t *testing.T) *Registry {
	t.Helper()
	registry := NewRegistry()
	for _, definition := range []Definition{
		{Name: "allow_plain_ftp", Kind: KindBool, Default: Bool(false)},
		{Name: "max_workers", Kind: KindInt64, Default: Int64(4)},
	} {
		if err := registry.Register(definition); err != nil {
			t.Fatal(err)
		}
	}
	return registry
}

func TestConfigCompatibilityMigratesOldConfigThenResolvesTypedSettings(t *testing.T) {
	registry := newCompatibilityRegistry(t)
	migrator, err := NewMigrator(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := migrator.Register(1, RenameSetting("plain_ftp", "allow_plain_ftp")); err != nil {
		t.Fatal(err)
	}

	input := RawConfig{Version: 1, Values: map[string]string{
		"plain_ftp":  "true",
		"max_workers": "8",
	}}
	migrated, err := migrator.Migrate(input)
	if err != nil {
		t.Fatal(err)
	}
	layer, err := registry.ParseLayer(migrated.Values)
	if err != nil {
		t.Fatal(err)
	}
	querySettings, err := registry.Resolve(layer, Layer{}, Layer{}, Layer{})
	if err != nil {
		t.Fatal(err)
	}

	plainFTP, ok := querySettings.Get("allow_plain_ftp")
	if !ok {
		t.Fatal("allow_plain_ftp missing after migration")
	}
	allowed, err := plainFTP.BoolValue()
	if err != nil || !allowed {
		t.Fatalf("allow_plain_ftp=%v err=%v, want true", allowed, err)
	}
	workers, ok := querySettings.Get("max_workers")
	if !ok {
		t.Fatal("max_workers missing after migration")
	}
	workerCount, err := workers.Int64Value()
	if err != nil || workerCount != 8 {
		t.Fatalf("max_workers=%d err=%v, want 8", workerCount, err)
	}
	if input.Values["plain_ftp"] != "true" {
		t.Fatalf("migration mutated caller input: %#v", input.Values)
	}
}

func TestConfigCompatibilityPreservesSecureDefaultWhenLegacyKeyAbsent(t *testing.T) {
	registry := newCompatibilityRegistry(t)
	migrator, err := NewMigrator(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := migrator.Register(1, RenameSetting("plain_ftp", "allow_plain_ftp")); err != nil {
		t.Fatal(err)
	}
	migrated, err := migrator.Migrate(RawConfig{Version: 1, Values: map[string]string{"max_workers": "6"}})
	if err != nil {
		t.Fatal(err)
	}
	layer, err := registry.ParseLayer(migrated.Values)
	if err != nil {
		t.Fatal(err)
	}
	querySettings, err := registry.Resolve(layer, Layer{}, Layer{}, Layer{})
	if err != nil {
		t.Fatal(err)
	}
	plainFTP, _ := querySettings.Get("allow_plain_ftp")
	allowed, err := plainFTP.BoolValue()
	if err != nil {
		t.Fatal(err)
	}
	if allowed {
		t.Fatal("legacy migration silently weakened secure plain-FTP default")
	}
}

func TestConfigCompatibilityRejectsConflictFutureUnknownAndInvalidValues(t *testing.T) {
	registry := newCompatibilityRegistry(t)
	migrator, err := NewMigrator(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := migrator.Register(1, RenameSetting("plain_ftp", "allow_plain_ftp")); err != nil {
		t.Fatal(err)
	}

	if _, err := migrator.Migrate(RawConfig{Version: 1, Values: map[string]string{
		"plain_ftp":       "true",
		"allow_plain_ftp": "false",
	}}); !errors.Is(err, ErrConfigConflict) {
		t.Fatalf("conflict error=%v, want ErrConfigConflict", err)
	}
	if _, err := migrator.Migrate(RawConfig{Version: 3}); !errors.Is(err, ErrInvalidConfigVersion) {
		t.Fatalf("future version error=%v, want ErrInvalidConfigVersion", err)
	}
	if _, err := registry.ParseLayer(map[string]string{"removed_setting": "1"}); !errors.Is(err, ErrUnknownSetting) {
		t.Fatalf("unknown setting error=%v, want ErrUnknownSetting", err)
	}
	if _, err := registry.ParseLayer(map[string]string{"allow_plain_ftp": "sometimes"}); !errors.Is(err, ErrInvalidSettingValue) {
		t.Fatalf("invalid value error=%v, want ErrInvalidSettingValue", err)
	}
}

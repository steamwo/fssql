package settings

import (
	"errors"
	"testing"
)

func TestParseLayerUsesRegisteredTypes(t *testing.T) {
	r := mustRegistry(t)
	layer, err := r.ParseLayer(map[string]string{
		"max_workers":    "12",
		"enable_profile": "true",
	})
	if err != nil {
		t.Fatal(err)
	}
	query, err := r.Resolve(layer, Layer{}, Layer{}, Layer{})
	if err != nil {
		t.Fatal(err)
	}

	workers, ok := query.Get("max_workers")
	if !ok {
		t.Fatal("max_workers missing")
	}
	gotWorkers, err := workers.Int64Value()
	if err != nil || gotWorkers != 12 {
		t.Fatalf("max_workers = %d, err = %v, want 12", gotWorkers, err)
	}
	profile, ok := query.Get("enable_profile")
	if !ok {
		t.Fatal("enable_profile missing")
	}
	gotProfile, err := profile.BoolValue()
	if err != nil || !gotProfile {
		t.Fatalf("enable_profile = %v, err = %v, want true", gotProfile, err)
	}
}

func TestParseLayerRejectsUnknownAndInvalidValue(t *testing.T) {
	r := mustRegistry(t)
	if _, err := r.ParseLayer(map[string]string{"unknown": "1"}); !errors.Is(err, ErrUnknownSetting) {
		t.Fatalf("unknown error = %v, want ErrUnknownSetting", err)
	}
	if _, err := r.ParseLayer(map[string]string{"max_workers": "not-an-int"}); !errors.Is(err, ErrInvalidSettingValue) {
		t.Fatalf("parse error = %v, want ErrInvalidSettingValue", err)
	}
}

func TestMigratorSequentialRenameAndInputImmutability(t *testing.T) {
	m, err := NewMigrator(3)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Register(1, RenameSetting("threads", "workers")); err != nil {
		t.Fatal(err)
	}
	if err := m.Register(2, RenameSetting("workers", "max_workers")); err != nil {
		t.Fatal(err)
	}

	input := RawConfig{Version: 1, Values: map[string]string{"threads": "8", "mode": "safe"}}
	got, err := m.Migrate(input)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != 3 {
		t.Fatalf("version = %d, want 3", got.Version)
	}
	if got.Values["max_workers"] != "8" || got.Values["mode"] != "safe" {
		t.Fatalf("migrated values = %#v", got.Values)
	}
	if _, ok := got.Values["threads"]; ok {
		t.Fatalf("old setting still present: %#v", got.Values)
	}
	if input.Values["threads"] != "8" {
		t.Fatalf("input mutated: %#v", input.Values)
	}
}

func TestMigratorRejectsFutureVersionAndMissingStep(t *testing.T) {
	m, err := NewMigrator(3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Migrate(RawConfig{Version: 4}); !errors.Is(err, ErrInvalidConfigVersion) {
		t.Fatalf("future version error = %v, want ErrInvalidConfigVersion", err)
	}
	if _, err := m.Migrate(RawConfig{Version: 1}); !errors.Is(err, ErrMissingMigration) {
		t.Fatalf("missing migration error = %v, want ErrMissingMigration", err)
	}
}

func TestRenameSettingRejectsConflict(t *testing.T) {
	migration := RenameSetting("threads", "max_workers")
	_, err := migration(map[string]string{"threads": "4", "max_workers": "8"})
	if !errors.Is(err, ErrConfigConflict) {
		t.Fatalf("error = %v, want ErrConfigConflict", err)
	}
}

func TestRegisterMigrationRejectsDuplicate(t *testing.T) {
	m, err := NewMigrator(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Register(1, RenameSetting("a", "b")); err != nil {
		t.Fatal(err)
	}
	if err := m.Register(1, RenameSetting("x", "y")); !errors.Is(err, ErrDuplicateMigration) {
		t.Fatalf("error = %v, want ErrDuplicateMigration", err)
	}
}

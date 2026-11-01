package settings

import (
	"errors"
	"fmt"
	"testing"
)

func mustRegistry(t *testing.T) *Registry {
	t.Helper()
	r := NewRegistry()
	if err := r.Register(Definition{Name: "max_workers", Kind: KindInt64, Default: Int64(4)}); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(Definition{Name: "enable_profile", Kind: KindBool, Default: Bool(false)}); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestResolveScopePrecedence(t *testing.T) {
	r := mustRegistry(t)
	query, err := r.Resolve(
		NewLayer(map[string]Value{"max_workers": Int64(8)}),
		NewLayer(map[string]Value{"max_workers": Int64(6)}),
		NewLayer(map[string]Value{"max_workers": Int64(3)}),
		NewLayer(map[string]Value{"max_workers": Int64(1)}),
	)
	if err != nil {
		t.Fatal(err)
	}
	value, ok := query.Get("max_workers")
	if !ok {
		t.Fatal("max_workers missing")
	}
	got, err := value.Int64Value()
	if err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Fatalf("statement override = %d, want 1", got)
	}
}

func TestResolveUsesDefault(t *testing.T) {
	r := mustRegistry(t)
	query, err := r.Resolve(Layer{}, Layer{}, Layer{}, Layer{})
	if err != nil {
		t.Fatal(err)
	}
	value, ok := query.Get("max_workers")
	if !ok {
		t.Fatal("max_workers missing")
	}
	got, err := value.Int64Value()
	if err != nil {
		t.Fatal(err)
	}
	if got != 4 {
		t.Fatalf("default = %d, want 4", got)
	}
}

func TestResolveRejectsUnknownSetting(t *testing.T) {
	r := mustRegistry(t)
	_, err := r.Resolve(NewLayer(map[string]Value{"unknown": String("x")}), Layer{}, Layer{}, Layer{})
	if !errors.Is(err, ErrUnknownSetting) {
		t.Fatalf("error = %v, want ErrUnknownSetting", err)
	}
}

func TestResolveRejectsKindMismatch(t *testing.T) {
	r := mustRegistry(t)
	_, err := r.Resolve(NewLayer(map[string]Value{"max_workers": String("8")}), Layer{}, Layer{}, Layer{})
	if !errors.Is(err, ErrKindMismatch) {
		t.Fatalf("error = %v, want ErrKindMismatch", err)
	}
}

func TestRegisterRejectsDuplicate(t *testing.T) {
	r := mustRegistry(t)
	err := r.Register(Definition{Name: "max_workers", Kind: KindInt64, Default: Int64(8)})
	if !errors.Is(err, ErrDuplicateSetting) {
		t.Fatalf("error = %v, want ErrDuplicateSetting", err)
	}
}

func TestValidatorAppliesToDefaultAndOverride(t *testing.T) {
	positive := func(v Value) error {
		n, err := v.Int64Value()
		if err != nil {
			return err
		}
		if n <= 0 {
			return fmt.Errorf("must be positive")
		}
		return nil
	}

	r := NewRegistry()
	if err := r.Register(Definition{Name: "io_limit", Kind: KindInt64, Default: Int64(2), Validate: positive}); err != nil {
		t.Fatal(err)
	}
	_, err := r.Resolve(Layer{}, Layer{}, Layer{}, NewLayer(map[string]Value{"io_limit": Int64(0)}))
	if err == nil {
		t.Fatal("expected validation error")
	}

	err = r.Register(Definition{Name: "bad_default", Kind: KindInt64, Default: Int64(0), Validate: positive})
	if err == nil {
		t.Fatal("expected invalid default error")
	}
}

func TestLayerAndQuerySettingsAreSnapshots(t *testing.T) {
	r := mustRegistry(t)
	input := map[string]Value{"max_workers": Int64(7)}
	engine := NewLayer(input)
	input["max_workers"] = Int64(99)

	query, err := r.Resolve(engine, Layer{}, Layer{}, Layer{})
	if err != nil {
		t.Fatal(err)
	}
	engine = engine.With("max_workers", Int64(5))

	value, ok := query.Get("max_workers")
	if !ok {
		t.Fatal("max_workers missing")
	}
	got, err := value.Int64Value()
	if err != nil {
		t.Fatal(err)
	}
	if got != 7 {
		t.Fatalf("snapshot value = %d, want 7", got)
	}
}

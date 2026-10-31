package main

import "testing"

func TestForbiddenDependencies(t *testing.T) {
	cases := []struct {
		from, to string
		bad      bool
	}{{"internal/core/types", "github.com/steamwo/fssql/internal/execution/expression", true}, {"internal/core/vector", "github.com/steamwo/fssql/internal/format/parquet", true}, {"internal/execution/scheduler", "github.com/steamwo/fssql/internal/core/vector", false}, {"internal/core/memory", "github.com/steamwo/fssql/internal/core/types", false}}
	for _, tc := range cases {
		bad, _ := forbidden(tc.from, tc.to)
		if bad != tc.bad {
			t.Fatalf("%s -> %s: got %v", tc.from, tc.to, bad)
		}
	}
}
func TestRepositoryArchitecture(t *testing.T) {
	vs, err := scan("../..")
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != 0 {
		t.Fatalf("violations: %+v", vs)
	}
}

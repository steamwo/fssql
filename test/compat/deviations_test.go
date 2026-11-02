package compat_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
)

type deviationRegistry struct {
	SchemaVersion int `json:"schema_version"`
	Reference     struct {
		Engine  string `json:"engine"`
		Version string `json:"version"`
	} `json:"reference"`
	Entries []deviationEntry `json:"entries"`
}

type deviationEntry struct {
	ID                 string `json:"id"`
	Classification     string `json:"classification"`
	Surface            string `json:"surface"`
	StableSQLSemantics bool   `json:"stable_sql_semantics"`
	Summary            string `json:"summary"`
	DuckDB             string `json:"duckdb"`
	FSSQL              string `json:"fssql"`
	Rationale          string `json:"rationale"`
}

func loadDeviationRegistry(t *testing.T) deviationRegistry {
	t.Helper()
	data, err := os.ReadFile("../../compatibility/deviations.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var registry deviationRegistry
	if err := json.Unmarshal(data, &registry); err != nil {
		t.Fatalf("compatibility/deviations.yaml must remain JSON-compatible YAML: %v", err)
	}
	return registry
}

func TestDeviationRegistry(t *testing.T) {
	registry := loadDeviationRegistry(t)
	if registry.SchemaVersion != 1 {
		t.Fatalf("schema_version=%d want=1", registry.SchemaVersion)
	}
	if registry.Reference.Engine != "duckdb" || strings.TrimSpace(registry.Reference.Version) == "" {
		t.Fatalf("reference=%+v", registry.Reference)
	}
	seen := make(map[string]struct{}, len(registry.Entries))
	for i, entry := range registry.Entries {
		if entry.ID == "" {
			t.Fatalf("entry %d has empty id", i)
		}
		if _, exists := seen[entry.ID]; exists {
			t.Fatalf("duplicate deviation id %q", entry.ID)
		}
		seen[entry.ID] = struct{}{}
		switch entry.Classification {
		case "intentional_deviation", "unsupported_feature":
		case "bug":
			t.Fatalf("release registry contains unresolved bug %s", entry.ID)
		default:
			t.Fatalf("entry %s has invalid classification %q", entry.ID, entry.Classification)
		}
		if entry.StableSQLSemantics {
			t.Fatalf("entry %s attempts to waive frozen Stable SQL semantics", entry.ID)
		}
		if strings.TrimSpace(entry.Surface) == "" || strings.TrimSpace(entry.Summary) == "" || strings.TrimSpace(entry.DuckDB) == "" || strings.TrimSpace(entry.FSSQL) == "" || strings.TrimSpace(entry.Rationale) == "" {
			t.Fatalf("entry %s is missing required explanation fields", entry.ID)
		}
	}
}

func TestDuckDBReferenceMatchesDeviationRegistry(t *testing.T) {
	if os.Getenv("FSSQL_REQUIRE_DUCKDB") != "1" {
		t.Skip("set FSSQL_REQUIRE_DUCKDB=1 in the acceptance gate")
	}
	registry := loadDeviationRegistry(t)
	output, err := exec.Command("python3", "-c", "import duckdb; print(duckdb.__version__)").CombinedOutput()
	if err != nil {
		t.Fatalf("read DuckDB version: %v: %s", err, output)
	}
	actual := strings.TrimSpace(string(output))
	if actual != registry.Reference.Version {
		t.Fatalf("DuckDB reference version=%q registry=%q", actual, registry.Reference.Version)
	}
}

package compat_test

import (
	"encoding/json"
	"fmt"
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
	Case               string `json:"case,omitempty"`
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

func validateDeviationRegistry(t *testing.T, registry deviationRegistry, records []sltRecord) map[string]deviationEntry {
	t.Helper()
	if registry.SchemaVersion != 1 {
		t.Fatalf("schema_version=%d want=1", registry.SchemaVersion)
	}
	if registry.Reference.Engine != "duckdb" || strings.TrimSpace(registry.Reference.Version) == "" {
		t.Fatalf("reference=%+v", registry.Reference)
	}

	knownCases := make(map[string]struct{}, len(records))
	for _, record := range records {
		knownCases[record.caseID()] = struct{}{}
	}
	seenIDs := make(map[string]struct{}, len(registry.Entries))
	stable := make(map[string]deviationEntry)
	for i, entry := range registry.Entries {
		if strings.TrimSpace(entry.ID) == "" {
			t.Fatalf("entry %d has empty id", i)
		}
		if _, exists := seenIDs[entry.ID]; exists {
			t.Fatalf("duplicate deviation id %q", entry.ID)
		}
		seenIDs[entry.ID] = struct{}{}
		switch entry.Classification {
		case "bug", "intentional_deviation", "unsupported_feature":
		default:
			t.Fatalf("entry %s has invalid classification %q", entry.ID, entry.Classification)
		}
		if strings.TrimSpace(entry.Surface) == "" || strings.TrimSpace(entry.Summary) == "" || strings.TrimSpace(entry.DuckDB) == "" || strings.TrimSpace(entry.FSSQL) == "" || strings.TrimSpace(entry.Rationale) == "" {
			t.Fatalf("entry %s is missing required explanation fields", entry.ID)
		}
		if !entry.StableSQLSemantics {
			if strings.TrimSpace(entry.Case) != "" {
				t.Fatalf("entry %s is a non-SQL boundary deviation but declares corpus case %q", entry.ID, entry.Case)
			}
			continue
		}
		if strings.TrimSpace(entry.Case) == "" {
			t.Fatalf("entry %s classifies Stable SQL semantics but has no corpus case", entry.ID)
		}
		if _, ok := knownCases[entry.Case]; !ok {
			t.Fatalf("entry %s references unknown Stable corpus case %q", entry.ID, entry.Case)
		}
		if previous, exists := stable[entry.Case]; exists {
			t.Fatalf("Stable corpus case %q is classified twice by %s and %s", entry.Case, previous.ID, entry.ID)
		}
		stable[entry.Case] = entry
	}
	return stable
}

func TestDeviationRegistry(t *testing.T) {
	registry := loadDeviationRegistry(t)
	validateDeviationRegistry(t, registry, loadCorpus(t))
}

func TestDuckDBReferenceMatchesDeviationRegistry(t *testing.T) {
	if os.Getenv("FSSQL_REQUIRE_DUCKDB") != "1" {
		t.Skip("set FSSQL_REQUIRE_DUCKDB=1 in the acceptance gate")
	}
	registry := loadDeviationRegistry(t)
	validateDeviationRegistry(t, registry, loadCorpus(t))
	output, err := exec.Command("python3", "-c", "import duckdb; print(duckdb.__version__)").CombinedOutput()
	if err != nil {
		t.Fatalf("read DuckDB version: %v: %s", err, output)
	}
	actual := strings.TrimSpace(string(output))
	if actual != registry.Reference.Version {
		t.Fatalf("DuckDB reference version=%q registry=%q", actual, registry.Reference.Version)
	}
}

func formatDifferentialMismatch(record sltRecord, ours []string, oursErr error, duck duckDBResult) string {
	return fmt.Sprintf("SQL: %s\nfssql values=%v err=%v\nduckdb values=%v err=%s", record.sql, ours, oursErr, duck.Values, duck.Error)
}

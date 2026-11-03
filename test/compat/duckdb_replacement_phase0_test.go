package compat_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type phase0Matrix struct {
	SchemaVersion int `json:"schema_version"`
	Phase         int `json:"phase"`
	Baselines     struct {
		Stable struct {
			Corpus           string `json:"corpus"`
			ReferenceEngine  string `json:"reference_engine"`
			ReferenceVersion string `json:"reference_version"`
		} `json:"stable_v1"`
		Migration struct {
			DuckDBVersion string `json:"duckdb_version"`
			Extension     struct {
				Version             *string `json:"version"`
				RequiredFileMagic   string  `json:"required_file_magic"`
				RequiredMetadataKey string  `json:"required_metadata_key"`
				Status              string  `json:"status"`
				Blocker             string  `json:"blocker"`
			} `json:"extension"`
		} `json:"migration"`
	} `json:"baselines"`
	Requirements []struct {
		ID                   string         `json:"id"`
		Capability           string         `json:"capability"`
		InputTypes           []string       `json:"input_types"`
		SQLCases             []string       `json:"sql_cases"`
		ExpectedSchema       []string       `json:"expected_schema"`
		ExpectedResult       any            `json:"expected_result"`
		ResourceBudget       map[string]any `json:"resource_budget"`
		ImplementationStatus string         `json:"implementation_status"`
		AcceptanceStatus     string         `json:"acceptance_status"`
		Evidence             []string       `json:"evidence"`
		Blocker              string         `json:"blocker"`
	} `json:"requirements"`
	Risks []struct {
		ID              string `json:"id"`
		Implementation  string `json:"implementation"`
		SmallScaleTests string `json:"small_scale_tests"`
		LargeAcceptance string `json:"large_acceptance"`
		State           string `json:"state"`
	} `json:"risks"`
}

type phase0SQLCorpus struct {
	SchemaVersion int `json:"schema_version"`
	Cases         []struct {
		ID             string         `json:"id"`
		Source         string         `json:"source"`
		SQL            string         `json:"sql"`
		ExpectedSchema []string       `json:"expected_schema"`
		ExpectedResult any            `json:"expected_result"`
		ResourceBudget map[string]any `json:"resource_budget"`
		Status         string         `json:"status"`
	} `json:"cases"`
}

func readJSONFile(t *testing.T, path string, dst any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, dst); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}

func TestDuckDBReplacementPhase0Baseline(t *testing.T) {
	var matrix phase0Matrix
	readJSONFile(t, "../../compatibility/duckdb-replacement-phase0.json", &matrix)
	if matrix.SchemaVersion != 1 || matrix.Phase != 0 {
		t.Fatalf("phase0 matrix version=%d phase=%d", matrix.SchemaVersion, matrix.Phase)
	}

	registry := loadDeviationRegistry(t)
	if matrix.Baselines.Stable.ReferenceEngine != registry.Reference.Engine ||
		matrix.Baselines.Stable.ReferenceVersion != registry.Reference.Version {
		t.Fatalf("stable baseline=%s/%s registry=%s/%s",
			matrix.Baselines.Stable.ReferenceEngine,
			matrix.Baselines.Stable.ReferenceVersion,
			registry.Reference.Engine,
			registry.Reference.Version)
	}
	if matrix.Baselines.Stable.Corpus != "test/compat/stable.slt" {
		t.Fatalf("stable corpus=%q", matrix.Baselines.Stable.Corpus)
	}
	if matrix.Baselines.Migration.DuckDBVersion != registry.Reference.Version {
		t.Fatalf("migration DuckDB=%q registry=%q", matrix.Baselines.Migration.DuckDBVersion, registry.Reference.Version)
	}
	ext := matrix.Baselines.Migration.Extension
	if ext.RequiredFileMagic != "PARE" || ext.RequiredMetadataKey != "masterKeyID" {
		t.Fatalf("encryption contract magic=%q metadata=%q", ext.RequiredFileMagic, ext.RequiredMetadataKey)
	}
	if ext.Version == nil && (ext.Status != "blocked_external_baseline" || strings.TrimSpace(ext.Blocker) == "") {
		t.Fatal("missing extension version must remain an explicit external-baseline blocker")
	}
}

func TestDuckDBReplacementPhase0MatrixIsActionable(t *testing.T) {
	var matrix phase0Matrix
	readJSONFile(t, "../../compatibility/duckdb-replacement-phase0.json", &matrix)

	var corpus phase0SQLCorpus
	readJSONFile(t, "../corpus/duckdb-replacement-phase0/sql.json", &corpus)
	if corpus.SchemaVersion != 1 || len(corpus.Cases) == 0 {
		t.Fatal("phase0 SQL corpus is empty")
	}
	sqlCases := make(map[string]struct{}, len(corpus.Cases))
	for _, c := range corpus.Cases {
		if strings.TrimSpace(c.ID) == "" || strings.TrimSpace(c.Source) == "" ||
			strings.TrimSpace(c.SQL) == "" || len(c.ExpectedSchema) == 0 ||
			c.ExpectedResult == nil || len(c.ResourceBudget) == 0 ||
			strings.TrimSpace(c.Status) == "" {
			t.Fatalf("incomplete SQL corpus case %+v", c)
		}
		if _, exists := sqlCases[c.ID]; exists {
			t.Fatalf("duplicate SQL case %q", c.ID)
		}
		sqlCases[c.ID] = struct{}{}
	}

	seen := make(map[string]struct{}, len(matrix.Requirements))
	for _, r := range matrix.Requirements {
		if strings.TrimSpace(r.ID) == "" || strings.TrimSpace(r.Capability) == "" ||
			len(r.InputTypes) == 0 || len(r.SQLCases) == 0 ||
			len(r.ExpectedSchema) == 0 || r.ExpectedResult == nil ||
			len(r.ResourceBudget) == 0 || strings.TrimSpace(r.ImplementationStatus) == "" ||
			strings.TrimSpace(r.AcceptanceStatus) == "" {
			t.Fatalf("incomplete phase0 requirement %+v", r)
		}
		if _, exists := seen[r.ID]; exists {
			t.Fatalf("duplicate phase0 requirement %q", r.ID)
		}
		seen[r.ID] = struct{}{}
		for _, id := range r.SQLCases {
			if _, ok := sqlCases[id]; !ok {
				t.Fatalf("requirement %s references missing SQL case %q", r.ID, id)
			}
		}
		if r.AcceptanceStatus != "implemented_small_scale" &&
			r.AcceptanceStatus != "large_acceptance_pending" &&
			r.AcceptanceStatus != "pending_implementation" &&
			r.AcceptanceStatus != "blocked_external_baseline" {
			t.Fatalf("requirement %s has unknown acceptance status %q", r.ID, r.AcceptanceStatus)
		}
		if r.AcceptanceStatus != "implemented_small_scale" && strings.TrimSpace(r.Blocker) == "" {
			t.Fatalf("non-complete requirement %s must explain its blocker", r.ID)
		}
	}

	for _, required := range []string{
		"FILE-CSV-001", "FILE-NDJSON-001", "FILE-PARQUET-001",
		"ROWSTREAM-001", "TYPE-FIDELITY-001", "SAMPLE-HASH-001",
		"SAMPLE-ORDERED-001", "SAMPLE-RANDOM-001", "MASK-UDF-001",
		"EXPORT-CSV-001", "EXPORT-PARQUET-001", "EXPORT-PARQUET-ENC-001",
	} {
		if _, ok := seen[required]; !ok {
			t.Fatalf("phase0 matrix missing required capability %s", required)
		}
	}
}

func TestDuckDBReplacementPhase0RiskStates(t *testing.T) {
	var matrix phase0Matrix
	readJSONFile(t, "../../compatibility/duckdb-replacement-phase0.json", &matrix)
	risks := make(map[string]struct {
		implementation  string
		smallScaleTests string
		largeAcceptance string
		state           string
	}, len(matrix.Risks))
	for _, r := range matrix.Risks {
		risks[r.ID] = struct {
			implementation  string
			smallScaleTests string
			largeAcceptance string
			state           string
		}{r.Implementation, r.SmallScaleTests, r.LargeAcceptance, r.State}
	}
	for _, id := range []string{"MEM-001", "PERF-001", "SQL-SAMPLE-001"} {
		r, ok := risks[id]
		if !ok {
			t.Fatalf("missing risk %s", id)
		}
		if strings.TrimSpace(r.implementation) == "" ||
			strings.TrimSpace(r.smallScaleTests) == "" ||
			strings.TrimSpace(r.largeAcceptance) == "" ||
			r.state != "open" {
			t.Fatalf("risk %s is not fully classified: %+v", id, r)
		}
	}
	if risks["MEM-001"].largeAcceptance != "pending" ||
		risks["PERF-001"].largeAcceptance != "pending" {
		t.Fatal("MEM-001/PERF-001 cannot close before post-remediation large acceptance")
	}
}

func TestDuckDBReplacementPhase0TypeAndEncryptionManifests(t *testing.T) {
	for _, path := range []string{
		"../corpus/duckdb-replacement-phase0/types.json",
		"../corpus/duckdb-replacement-phase0/encryption/manifest.json",
	} {
		var value map[string]any
		readJSONFile(t, filepath.Clean(path), &value)
		if value["schema_version"] != float64(1) {
			t.Fatalf("%s schema_version=%v", path, value["schema_version"])
		}
	}
}

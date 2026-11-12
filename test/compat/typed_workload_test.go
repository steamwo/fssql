package compat_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/semantics"
	"github.com/steamwo/fssql/pkg/gosql"
)

type typedResult struct {
	Names         []string    `json:"names"`
	Types         []string    `json:"types"`
	Rows          [][]*string `json:"rows"`
	Error         string      `json:"error,omitempty"`
	ErrorCategory string      `json:"error_category,omitempty"`
}

type workloadCase struct {
	ID            string `json:"id"`
	Source        string `json:"source"`
	Capability    string `json:"capability"`
	Priority      string `json:"priority"`
	SQL           string `json:"sql"`
	Ordered       bool   `json:"ordered"`
	ErrorCategory string `json:"error_category,omitempty"`
}

type workloadEnvironment struct {
	FixtureSHA256    string `json:"fixture_sha256,omitempty"`
	ReferenceVersion string `json:"reference_version"`
	GoVersion        string `json:"go_version"`
	Platform         string `json:"platform"`
	BinarySHA256     string `json:"binary_sha256"`
	CorpusSHA256     string `json:"corpus_sha256"`
	Config           string `json:"config"`
}

// Unknown errors deliberately cannot establish compatibility, even on both sides.
func fssqlErrorCategory(err error) string {
	if err == nil {
		return ""
	}
	s := strings.ToLower(err.Error())
	switch {
	case strings.HasPrefix(s, "sql parser "):
		return "parser"
	case strings.Contains(s, "overflow"), strings.Contains(s, "out of range"):
		return "overflow"
	case strings.Contains(s, "strconv."), strings.Contains(s, "cannot parse"), strings.Contains(s, "invalid syntax"):
		return "conversion"
	case strings.HasPrefix(s, "binder "), strings.HasPrefix(s, "query binder "), strings.Contains(s, "unknown column"), strings.Contains(s, "unknown function"), strings.Contains(s, "expects numeric input"):
		return "binder"
	}
	return "unknown"
}

func queryTyped(ctx context.Context, db *sql.DB, text string) typedResult {
	result := typedResult{Rows: make([][]*string, 0)}
	rows, err := db.QueryContext(ctx, text)
	if err == nil {
		defer rows.Close()
		result.Names, err = rows.Columns()
		if err == nil {
			var columns []*sql.ColumnType
			columns, err = rows.ColumnTypes()
			for _, column := range columns {
				result.Types = append(result.Types, column.DatabaseTypeName())
			}
		}
		for err == nil && rows.Next() {
			values := make([]any, len(result.Names))
			dest := make([]any, len(values))
			for i := range values {
				dest[i] = &values[i]
			}
			err = rows.Scan(dest...)
			if err != nil {
				break
			}
			row := make([]*string, len(values))
			for i, v := range values {
				if v == nil {
					continue
				}
				var text string
				text, err = typedCell(v, result.Types[i])
				if err != nil {
					break
				}
				row[i] = &text
			}
			if err == nil {
				result.Rows = append(result.Rows, row)
			}
		}
		if err == nil {
			err = rows.Err()
		}
	}
	if err != nil {
		result.Error, result.ErrorCategory = err.Error(), fssqlErrorCategory(err)
	}
	return result
}

func typedCell(value any, typ string) (string, error) {
	switch v := value.(type) {
	case time.Time:
		if typ == "DATE" {
			return semantics.FormatCalendarDate(v.UTC()), nil
		}
		text := semantics.FormatCalendarDate(v.UTC()) + v.UTC().Format(" 15:04:05.999999999")
		if typ == "TIMESTAMP WITH TIME ZONE" {
			text += "+00"
		}
		return text, nil
	case []byte:
		if typ == "BLOB" {
			return strings.ToUpper(hex.EncodeToString(v)), nil
		}
		return string(v), nil
	case float64:
		switch {
		case math.IsNaN(v):
			return "nan", nil
		case math.IsInf(v, 1):
			return "inf", nil
		case math.IsInf(v, -1):
			return "-inf", nil
		case v == 0:
			return "0", nil // Numeric equality treats signed zeros alike.
		}
		return strconv.FormatFloat(v, 'g', 17, 64), nil
	default:
		return canonicalValue(value)
	}
}

func compareTyped(ours, reference typedResult, ordered bool, expectedError string) error {
	if expectedError != "" {
		if expectedError == "unknown" || ours.Error == "" || reference.Error == "" || ours.ErrorCategory != expectedError || reference.ErrorCategory != expectedError {
			return fmt.Errorf("error category mismatch: expected=%s fssql=%+v duckdb=%+v", expectedError, ours, reference)
		}
		return nil
	}
	if ours.Error != "" || reference.Error != "" {
		return fmt.Errorf("unexpected error: fssql=%s duckdb=%s", ours.Error, reference.Error)
	}
	if !reflect.DeepEqual(ours.Names, reference.Names) {
		return fmt.Errorf("column names: fssql=%v duckdb=%v", ours.Names, reference.Names)
	}
	if !reflect.DeepEqual(ours.Types, reference.Types) {
		return fmt.Errorf("column types: fssql=%v duckdb=%v", ours.Types, reference.Types)
	}
	keys := func(result typedResult) ([]string, error) {
		if len(result.Names) != len(result.Types) {
			return nil, fmt.Errorf("column name/type counts differ")
		}
		out := make([]string, len(result.Rows))
		for i, row := range result.Rows {
			if len(row) != len(result.Types) {
				return nil, fmt.Errorf("row %d has %d cells, expected %d", i, len(row), len(result.Types))
			}
			key, err := json.Marshal(row)
			if err != nil {
				return nil, err
			}
			out[i] = string(key)
		}
		if !ordered {
			sort.Strings(out)
		}
		return out, nil
	}
	a, err := keys(ours)
	if err != nil {
		return err
	}
	b, err := keys(reference)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(a, b) {
		return fmt.Errorf("rows: fssql=%v duckdb=%v", a, b)
	}
	return nil
}

func TestDuckDBTypedWorkload(t *testing.T) {
	runTypedWorkload(t, "typed_workload.json")
}

func TestDuckDBTypedNumericCasts(t *testing.T) {
	runTypedWorkload(t, "typed_numeric_cast_workload.json")
}

func TestDuckDBTypedSetOperations(t *testing.T) {
	runTypedWorkload(t, "typed_set_workload.json")
}

func TestDuckDBTypedBulk(t *testing.T) { runTypedWorkload(t, "typed_bulk_workload.json") }

func TestDuckDBTypedTemporal(t *testing.T) {
	runTypedWorkload(t, "typed_temporal_workload.json")
}

func runTypedWorkload(t *testing.T, defaultPath string) {
	if os.Getenv("FSSQL_REQUIRE_DUCKDB") != "1" {
		t.Skip("set FSSQL_REQUIRE_DUCKDB=1 in the acceptance gate")
	}
	path := os.Getenv("FSSQL_TYPED_WORKLOAD")
	if path == "" {
		path = defaultPath
	}
	corpus, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var cases []workloadCase
	if err := json.Unmarshal(corpus, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) == 0 {
		t.Fatal("empty workload")
	}
	fixtures := newTypedWorkloadFixtures(t, cases)
	request := duckDBRequest{Typed: true, Threads: 1}
	seen := make(map[string]bool)
	for _, c := range cases {
		if c.ID == "" || seen[c.ID] || c.Source == "" || c.Capability == "" || c.Priority == "" || c.SQL == "" {
			t.Fatalf("invalid workload metadata: %+v", c)
		}
		seen[c.ID] = true
		request.Queries = append(request.Queries, fixtures.expand(strings.ReplaceAll(c.SQL, "{batch}", unionFixtureSQL(batchFixtureRows)), true))
	}
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(duckDBPython(), "duckdb_runner.py")
	cmd.Stdin = bytes.NewReader(payload)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("DuckDB runner: %v: %s", err, output)
	}
	var reference struct {
		DuckDBVersion string        `json:"duckdb_version"`
		Results       []typedResult `json:"results"`
		Error         string        `json:"error"`
	}
	if err := json.Unmarshal(output, &reference); err != nil {
		t.Fatalf("decode reference: %v: %s", err, output)
	}
	if reference.Error != "" || reference.DuckDBVersion != loadDeviationRegistry(t).Reference.Version || len(reference.Results) != len(cases) {
		t.Fatalf("invalid reference: %+v", reference)
	}
	resetDiffEvidence(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.Open(executable)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	_, hashErr := io.Copy(hash, binary)
	closeErr := binary.Close()
	if hashErr != nil {
		t.Fatal(hashErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	environment := workloadEnvironment{FixtureSHA256: fixtures.sha256(), ReferenceVersion: reference.DuckDBVersion, GoVersion: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH, BinarySHA256: hex.EncodeToString(hash.Sum(nil)), CorpusSHA256: fmt.Sprintf("%x", sha256.Sum256(corpus)), Config: "workers:1,memory:64MiB,temp:128MiB,spill:2MiB,timezone:UTC"}
	db, err := gosql.OpenSQLDB(gosql.Config{LocalFiles: fixtures.localConfig(), MemoryLimitBytes: 64 << 20, TempLimitBytes: 128 << 20, SpillMemoryBudgetBytes: 2 << 20, TableFunctions: []gosql.TableFunction{batchRowsTableFunction()}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	t.Logf("environment=%+v corpus=%s", environment, path)
	passed := 0
	for i, c := range cases {
		t.Run(c.ID, func(t *testing.T) {
			ours := queryTyped(context.Background(), db, fixtures.expand(strings.ReplaceAll(c.SQL, "{batch}", "batch_rows()"), false))
			err := compareTyped(ours, reference.Results[i], c.Ordered, c.ErrorCategory)
			recordDiffEvidence(t, diffEvidence{Suite: "typed-workload", Case: c.ID, SQL: c.SQL, Source: c.Source, Capability: c.Capability, Priority: c.Priority, Environment: &environment, FSSQLTyped: &ours, DuckDBTyped: &reference.Results[i], Match: err == nil})
			if err != nil {
				t.Fatal(err)
			}
			passed++
		})
	}
	t.Logf("workload total=%d passed=%d failed=%d skipped=0 (synthetic cases do not measure production coverage)", len(cases), passed, len(cases)-passed)
}

package compat_test

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/steamwo/fssql/pkg/gosql"
)

type sltRecord struct {
	line     int
	kind     string
	types    string
	sql      string
	expected []string
}

func (r sltRecord) caseID() string { return fmt.Sprintf("stable.slt:%d", r.line) }

func loadCorpus(t *testing.T) []sltRecord {
	t.Helper()
	file, err := os.Open("stable.slt")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var lines []string
	s := bufio.NewScanner(file)
	for s.Scan() {
		lines = append(lines, s.Text())
	}
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
	var records []sltRecord
	for i := 0; i < len(lines); {
		line := strings.TrimSpace(lines[i])
		if line == "" || strings.HasPrefix(line, "#") {
			i++
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "query" {
			t.Fatalf("stable.slt:%d: unsupported directive %q", i+1, line)
		}
		record := sltRecord{line: i + 1, kind: fields[0], types: fields[1]}
		i++
		var sqlLines []string
		for i < len(lines) && strings.TrimSpace(lines[i]) != "----" {
			if strings.TrimSpace(lines[i]) == "" {
				t.Fatalf("stable.slt:%d: query missing ----", record.line)
			}
			sqlLines = append(sqlLines, lines[i])
			i++
		}
		if i == len(lines) {
			t.Fatalf("stable.slt:%d: query missing expected values", record.line)
		}
		i++
		for i < len(lines) && strings.TrimSpace(lines[i]) != "" {
			if !strings.HasPrefix(strings.TrimSpace(lines[i]), "#") {
				record.expected = append(record.expected, lines[i])
			}
			i++
		}
		record.sql = strings.Join(sqlLines, "\n")
		if record.sql == "" || record.types == "" {
			t.Fatalf("stable.slt:%d: empty query/type signature", record.line)
		}
		records = append(records, record)
	}
	if len(records) == 0 {
		t.Fatal("stable.slt contains no query records")
	}
	return records
}

func queryValues(ctx context.Context, db *sql.DB, sqlText string) ([]string, error) {
	rows, err := db.QueryContext(ctx, sqlText)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var values []string
	for rows.Next() {
		dest := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range dest {
			ptrs[i] = &dest[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		for _, value := range dest {
			canonical, err := canonicalValue(value)
			if err != nil {
				return nil, err
			}
			values = append(values, canonical)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return values, nil
}

func canonicalValue(value any) (string, error) {
	switch v := value.(type) {
	case nil:
		return "NULL", nil
	case bool:
		if v {
			return "true", nil
		}
		return "false", nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case float64:
		return strconv.FormatFloat(v, 'g', 17, 64), nil
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano), nil
	default:
		return "", fmt.Errorf("unsupported compatibility scalar %T", value)
	}
}

func openCompatibilityDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := gosql.OpenSQLDB(gosql.Config{
		MemoryLimitBytes:       64 << 20,
		TempLimitBytes:         128 << 20,
		SpillMemoryBudgetBytes: 2 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestStableSQLLogicCorpus(t *testing.T) {
	db := openCompatibilityDB(t)
	defer db.Close()
	for _, record := range loadCorpus(t) {
		record := record
		t.Run(fmt.Sprintf("line_%d", record.line), func(t *testing.T) {
			values, err := queryValues(context.Background(), db, record.sql)
			if err != nil {
				t.Fatalf("%s: %v", record.sql, err)
			}
			if !reflect.DeepEqual(values, record.expected) {
				t.Fatalf("%s\nvalues=%v\nwant=%v", record.sql, values, record.expected)
			}
		})
	}
}

type duckDBRequest struct {
	Queries []string `json:"queries"`
}

type duckDBResult struct {
	Values []string `json:"values"`
	Error  string   `json:"error"`
}

type duckDBResponse struct {
	DuckDBVersion string         `json:"duckdb_version"`
	Results       []duckDBResult `json:"results"`
	Error         string         `json:"error"`
}

func TestDuckDBDifferentialCorpus(t *testing.T) {
	if os.Getenv("FSSQL_REQUIRE_DUCKDB") != "1" {
		t.Skip("set FSSQL_REQUIRE_DUCKDB=1 in the acceptance gate")
	}
	records := loadCorpus(t)
	registry := loadDeviationRegistry(t)
	stableDeviations := validateDeviationRegistry(t, registry, records)
	request := duckDBRequest{Queries: make([]string, len(records))}
	for i := range records {
		request.Queries[i] = records[i].sql
	}
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(duckDBPython(), "duckdb_runner.py")
	cmd.Stdin = bytes.NewReader(payload)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("DuckDB reference runner: %v: %s", err, output)
	}
	var reference duckDBResponse
	if err := json.Unmarshal(output, &reference); err != nil {
		t.Fatalf("decode DuckDB response: %v: %s", err, output)
	}
	if reference.Error != "" {
		t.Fatal(reference.Error)
	}
	if reference.DuckDBVersion == "" || len(reference.Results) != len(records) {
		t.Fatalf("invalid DuckDB response version=%q results=%d want=%d", reference.DuckDBVersion, len(reference.Results), len(records))
	}
	if reference.DuckDBVersion != registry.Reference.Version {
		t.Fatalf("DuckDB runner version=%q registry=%q", reference.DuckDBVersion, registry.Reference.Version)
	}

	db := openCompatibilityDB(t)
	defer db.Close()
	for i, record := range records {
		ours, oursErr := queryValues(context.Background(), db, record.sql)
		duck := reference.Results[i]
		matches := oursErr == nil && duck.Error == "" && reflect.DeepEqual(ours, duck.Values)
		deviation, classified := stableDeviations[record.caseID()]
		if matches {
			if classified {
				t.Fatalf("%s: stale %s deviation %s; fssql now matches DuckDB %s", record.caseID(), deviation.Classification, deviation.ID, reference.DuckDBVersion)
			}
			continue
		}

		mismatch := formatDifferentialMismatch(record, ours, oursErr, duck)
		if !classified {
			t.Fatalf("%s DuckDB %s unclassified mismatch\n%s", record.caseID(), reference.DuckDBVersion, mismatch)
		}
		switch deviation.Classification {
		case "intentional_deviation":
			t.Logf("%s intentional DuckDB deviation %s: %s", record.caseID(), deviation.ID, deviation.Rationale)
		case "bug":
			t.Fatalf("%s known compatibility bug %s still fails acceptance: %s\n%s", record.caseID(), deviation.ID, deviation.Rationale, mismatch)
		case "unsupported_feature":
			t.Fatalf("%s unsupported_feature %s cannot waive a frozen Stable SQL mismatch: %s\n%s", record.caseID(), deviation.ID, deviation.Rationale, mismatch)
		default:
			t.Fatalf("%s invalid deviation classification %q", record.caseID(), deviation.Classification)
		}
	}
}

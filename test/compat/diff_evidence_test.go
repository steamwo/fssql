package compat_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// diffEvidence 是一条可复核的差分证据：同一用例在 fssql 与 DuckDB 的
// 规范化值、列数、错误和最终是否一致。
type diffEvidence struct {
	Suite         string               `json:"suite"`
	Case          string               `json:"case"`
	SQL           string               `json:"sql,omitempty"`
	FSSQLValues   []string             `json:"fssql_values,omitempty"`
	DuckDBValues  []string             `json:"duckdb_values,omitempty"`
	FSSQLColumns  int                  `json:"fssql_columns,omitempty"`
	DuckDBColumns int                  `json:"duckdb_columns,omitempty"`
	FSSQLError    string               `json:"fssql_error,omitempty"`
	DuckDBError   string               `json:"duckdb_error,omitempty"`
	Match         bool                 `json:"match"`
	Source        string               `json:"source,omitempty"`
	Capability    string               `json:"capability,omitempty"`
	Priority      string               `json:"priority,omitempty"`
	FSSQLTyped    *typedResult         `json:"fssql_typed,omitempty"`
	DuckDBTyped   *typedResult         `json:"duckdb_typed,omitempty"`
	Environment   *workloadEnvironment `json:"environment,omitempty"`
}

var (
	evidenceOnce sync.Once
	evidenceMu   sync.Mutex
)

// resetDiffEvidence 在每次测试进程首次调用时清空证据文件；未设置
// FSSQL_DIFF_EVIDENCE 时不产生任何文件。
func resetDiffEvidence(t *testing.T) {
	t.Helper()
	path := os.Getenv("FSSQL_DIFF_EVIDENCE")
	if path == "" {
		return
	}
	evidenceOnce.Do(func() {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("create evidence directory: %v", err)
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			t.Fatalf("reset evidence file: %v", err)
		}
	})
}

// recordDiffEvidence 追加一行 JSONL；测试失败前也必须先记录，避免证据只保留成功用例。
func recordDiffEvidence(t *testing.T, evidence diffEvidence) {
	t.Helper()
	path := os.Getenv("FSSQL_DIFF_EVIDENCE")
	if path == "" {
		return
	}
	evidenceMu.Lock()
	defer evidenceMu.Unlock()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open evidence file: %v", err)
	}
	defer file.Close()
	if err := json.NewEncoder(file).Encode(evidence); err != nil {
		t.Fatalf("write evidence: %v", err)
	}
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// logDiffValues 只在 FSSQL_DIFF_VERBOSE=1 时输出逐用例两侧值。
func logDiffValues(t *testing.T, id string, fssqlValues, duckValues []string) {
	t.Helper()
	if os.Getenv("FSSQL_DIFF_VERBOSE") != "1" {
		return
	}
	t.Logf("%s\n  fssql  = %v\n  duckdb = %v", id, fssqlValues, duckValues)
}

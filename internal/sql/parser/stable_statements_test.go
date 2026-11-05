package parser

import (
	"testing"

	"github.com/steamwo/fssql/internal/sql/ast"
)

func TestParseCreateView(t *testing.T) {
	stmt, err := Parse("CREATE VIEW analytics.v AS SELECT a, count(*) n FROM t GROUP BY a")
	if err != nil {
		t.Fatal(err)
	}
	s, ok := stmt.(*ast.CreateViewStmt)
	if !ok || len(s.Name.Parts) != 2 || s.Query == nil {
		t.Fatalf("stmt=%T %+v", stmt, stmt)
	}
}

func TestParseCreateExternalTable(t *testing.T) {
	stmt, err := Parse("CREATE EXTERNAL TABLE logs USING read_ndjson('https://example.test/logs/*.ndjson')")
	if err != nil {
		t.Fatal(err)
	}
	s, ok := stmt.(*ast.CreateExternalTableStmt)
	if !ok || s.Function.Parts[0].Name != "read_ndjson" || s.Source.Value != "https://example.test/logs/*.ndjson" {
		t.Fatalf("stmt=%T %+v", stmt, stmt)
	}
}

func TestParseCreateSecret(t *testing.T) {
	stmt, err := Parse("CREATE SECRET prod (TYPE s3, key_id 'abc', secret_key 'xyz', SUBJECT 'alice', SCHEME 's3', HOST '*.example.com')")
	if err != nil {
		t.Fatal(err)
	}
	s, ok := stmt.(*ast.CreateSecretStmt)
	if !ok || len(s.Options) != 6 {
		t.Fatalf("stmt=%T %+v", stmt, stmt)
	}
	if s.Options[0].Kind != ast.SecretType || s.Options[3].Kind != ast.SecretSubject {
		t.Fatalf("options=%+v", s.Options)
	}
}

func TestParseCopyRelationAndQuery(t *testing.T) {
	for _, sql := range []string{
		"COPY main.t TO 'out.parquet' (format parquet, compression 'zstd')",
		"COPY (SELECT a FROM t WHERE a > 0) TO 'out.csv' (header true)",
	} {
		stmt, err := Parse(sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		s, ok := stmt.(*ast.CopyStmt)
		if !ok || s.Destination == nil || len(s.Options) == 0 {
			t.Fatalf("stmt=%T %+v", stmt, stmt)
		}
	}
}

func TestParseDescribeRelationAndQuery(t *testing.T) {
	a, err := Parse("DESCRIBE main.t")
	if err != nil {
		t.Fatal(err)
	}
	if s, ok := a.(*ast.DescribeStmt); !ok || s.Relation == nil {
		t.Fatalf("relation=%T %+v", a, a)
	}
	b, err := Parse("DESCRIBE SELECT a FROM t")
	if err != nil {
		t.Fatal(err)
	}
	if s, ok := b.(*ast.DescribeStmt); !ok || s.Query == nil {
		t.Fatalf("query=%T %+v", b, b)
	}
}

func TestStableStatementRejectsUnfrozenExtensions(t *testing.T) {
	for _, sql := range []string{
		"CREATE EXTERNAL TABLE t USING read_csv(path)",
		"CREATE PERSISTENT SECRET x (TYPE s3, SUBJECT '*')",
		"COPY t FROM 'x.csv'",
		"DESC t",
	} {
		if _, err := Parse(sql); err == nil {
			t.Fatalf("expected rejection: %s", sql)
		}
	}
}

func TestStatementWordsStillUsableAsIdentifiers(t *testing.T) {
	s := mustSelect(t, "SELECT type, subject, host, view FROM t")
	if len(s.Projection) != 4 {
		t.Fatalf("projection=%d", len(s.Projection))
	}
}

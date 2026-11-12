package binder

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/sql/parser"
)

func bindSQL(t *testing.T, sql string) (BoundStatement, error) {
	t.Helper()
	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	return NewStatementBinder(nil, fakeRelationResolver{relations: map[string]ResolvedRelation{
		"t": {ID: "catalog:main.t", Columns: []RelationColumn{
			{Name: "a", Type: types.LogicalType{ID: types.BigInt}},
			{Name: "b", Type: types.LogicalType{ID: types.Varchar}, Nullable: true},
		}},
	}}, fakeTableFunctionResolver{}, sql).Bind(context.Background(), stmt)
}

func TestStatementBinderCreateViewReusesQueryBinder(t *testing.T) {
	sql := "CREATE VIEW analytics.v AS SELECT a, count(*) AS n FROM t GROUP BY a"
	bound, err := bindSQL(t, sql)
	if err != nil {
		t.Fatal(err)
	}
	view, ok := bound.(*BoundCreateView)
	if !ok || view.Name.Schema != "analytics" || view.Name.Name != "v" || len(view.Columns) != 2 {
		t.Fatalf("bound=%T %+v", bound, bound)
	}
	if view.SQL != "SELECT a, count(*) AS n FROM t GROUP BY a" {
		t.Fatalf("view sql=%q", view.SQL)
	}
}

func TestStatementBinderExternalTableUsesTableFunctionResolver(t *testing.T) {
	sql := "CREATE EXTERNAL TABLE logs USING read_ndjson('s3://bucket/logs.ndjson')"
	bound, err := bindSQL(t, sql)
	if err != nil {
		t.Fatal(err)
	}
	external, ok := bound.(*BoundCreateExternalTable)
	if !ok || external.Function != "read_ndjson" || external.Source != "s3://bucket/logs.ndjson" || len(external.Columns) != 2 || external.Fingerprint == "" {
		t.Fatalf("bound=%T %+v", bound, bound)
	}
}

func TestStatementBinderCreateSecretRequiresExplicitACLAndRedacts(t *testing.T) {
	sql := "CREATE SECRET prod (TYPE s3, key_id 'PUBLIC-ID', secret_key 'VERY-SECRET', SUBJECT 'alice', SCHEME 'S3', HOST '*.EXAMPLE.COM')"
	bound, err := bindSQL(t, sql)
	if err != nil {
		t.Fatal(err)
	}
	secret, ok := bound.(*BoundCreateSecret)
	if !ok || secret.Type != "s3" || len(secret.Credentials) != 2 || len(secret.Subjects) != 1 || secret.Schemes[0] != "s3" || secret.Hosts[0] != "*.example.com" {
		t.Fatalf("bound=%T %+v", bound, bound)
	}
	if secret.Credentials[1].Value() != "VERY-SECRET" {
		t.Fatal("credential value accessor mismatch")
	}
	if rendered := fmt.Sprint(secret); strings.Contains(rendered, "VERY-SECRET") || strings.Contains(rendered, "PUBLIC-ID") {
		t.Fatalf("secret String leaked credential: %s", rendered)
	}

	for _, bad := range []string{
		"CREATE SECRET x (key 'v', SUBJECT 'alice')",
		"CREATE SECRET x (TYPE s3, key 'v')",
		"CREATE SECRET x (TYPE s3, SUBJECT 'alice')",
		"CREATE SECRET x (TYPE s3, key 'v', key 'v2', SUBJECT 'alice')",
		"CREATE SECRET x (TYPE s3, TYPE http, key 'v', SUBJECT 'alice')",
	} {
		if _, err := bindSQL(t, bad); err == nil {
			t.Fatalf("expected secret rejection: %s", bad)
		}
	}
}

func TestStatementBinderCopyInferenceAndStableOptions(t *testing.T) {
	cases := []struct {
		sql         string
		format      CopyFormat
		compression string
		header      bool
		delimiter   byte
	}{
		{"COPY t TO 'out.csv'", CopyCSV, "identity", true, ','},
		{"COPY t TO 'out.csv.gz' (HEADER false, DELIMITER '|')", CopyCSV, "gzip", false, '|'},
		{"COPY t TO 'out.csv' (HEADER ON)", CopyCSV, "identity", true, ','},
		{"COPY t TO 'out.csv' (HEADER OFF)", CopyCSV, "identity", false, ','},
		{"COPY t TO 'out.csv' (HEADER 1)", CopyCSV, "identity", true, ','},
		{"COPY (SELECT a FROM t) TO 'out.json.zst'", CopyJSON, "zstd", true, ','},
		{"COPY t TO 'out.parquet'", CopyParquet, "snappy", true, ','},
		{"COPY t TO 'out.bin' (FORMAT parquet)", CopyParquet, "snappy", true, ','},
		{"COPY t TO 'out.csv' (FORMAT parquet)", CopyParquet, "snappy", true, ','},
		{"COPY t TO 'out.parquet' (FORMAT csv)", CopyCSV, "identity", true, ','},
		{"COPY t TO 'out.parquet' (COMPRESSION none)", CopyParquet, "identity", true, ','},
		{"COPY t TO 'out.parquet' (COMPRESSION identity)", CopyParquet, "identity", true, ','},
		{"COPY t TO 'out.parquet' (COMPRESSION '')", CopyParquet, "identity", true, ','},
		{"COPY t TO 'out.parquet' (COMPRESSION snappy)", CopyParquet, "snappy", true, ','},
		{"COPY t TO 'out.ndjson' (COMPRESSION zstd)", CopyNDJSON, "zstd", true, ','},
	}
	for _, tc := range cases {
		bound, err := bindSQL(t, tc.sql)
		if err != nil {
			t.Fatalf("%s: %v", tc.sql, err)
		}
		copyStmt, ok := bound.(*BoundCopyTo)
		if !ok || copyStmt.Options.Format != tc.format || copyStmt.Options.Compression != tc.compression || copyStmt.Options.Header != tc.header || copyStmt.Options.Delimiter != tc.delimiter || len(copyStmt.Query.Output) == 0 {
			t.Fatalf("%s bound=%T %+v", tc.sql, bound, bound)
		}
	}
}

func TestStatementBinderCopyRejectsUnsupportedOrLossyOptions(t *testing.T) {
	for _, bad := range []string{
		"COPY t TO 'out.parquet.gz'",
		"COPY t TO 'out.json' (HEADER)",
		"COPY t TO 'out.csv' (DELIMITER '||')",
		"COPY t TO 'out.csv' (DELIMITER ',', QUOTE ',')",
		"COPY t TO 'out.csv' (ESCAPE '\\')",
		"COPY t TO 'out.csv' (FORMAT avro)",
		"COPY t TO 'out.csv' (COMPRESSION brotli)",
	} {
		if _, err := bindSQL(t, bad); err == nil {
			t.Fatalf("expected COPY rejection: %s", bad)
		}
	}
}

func TestStatementBinderDescribeUsesActualBoundSchema(t *testing.T) {
	for _, sql := range []string{"DESCRIBE t", "DESCRIBE SELECT a, b FROM t"} {
		bound, err := bindSQL(t, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		describe, ok := bound.(*BoundDescribe)
		if !ok || len(describe.Columns) != 2 || describe.Columns[1].Name != "b" || !describe.Columns[1].Nullable {
			t.Fatalf("%s bound=%T %+v", sql, bound, bound)
		}
	}
}

func TestStatementBinderRejectsAttachedCatalogObjectName(t *testing.T) {
	if _, err := bindSQL(t, "CREATE VIEW attached.main.v AS SELECT a FROM t"); err == nil {
		t.Fatal("expected three-part Catalog object name rejection")
	}
}

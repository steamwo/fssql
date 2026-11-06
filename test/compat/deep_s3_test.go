package compat_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/steamwo/fssql/pkg/gosql"
)

// minioTestConfig 是本地 MinIO 测试配置；凭据只来自环境或已忽略的本地文件，
// 不写入日志、命令行参数或测试失败信息。
type minioTestConfig struct {
	Endpoint  string `json:"FSSQL_MINIO_ENDPOINT"`
	Bucket    string `json:"FSSQL_MINIO_BUCKET"`
	AccessKey string `json:"FSSQL_MINIO_ACCESS_KEY"`
	SecretKey string `json:"FSSQL_MINIO_SECRET_KEY"`
}

func loadMinIOTestConfig(t *testing.T) minioTestConfig {
	t.Helper()
	config := minioTestConfig{
		Endpoint:  os.Getenv("FSSQL_MINIO_ENDPOINT"),
		Bucket:    os.Getenv("FSSQL_MINIO_BUCKET"),
		AccessKey: os.Getenv("FSSQL_MINIO_ACCESS_KEY"),
		SecretKey: os.Getenv("FSSQL_MINIO_SECRET_KEY"),
	}
	if config.Endpoint == "" {
		// 本地验证便利入口；CI 应显式注入环境变量。
		data, err := os.ReadFile("../../.cache/agent-verification/minio-test-env.json")
		if err == nil {
			if err := json.Unmarshal(data, &config); err != nil {
				t.Fatalf("decode MinIO test config: %v", err)
			}
		}
	}
	if config.Endpoint == "" {
		t.Skip("set FSSQL_MINIO_ENDPOINT and FSSQL_REQUIRE_DUCKDB=1 to run MinIO differential")
	}
	if !strings.HasPrefix(config.Bucket, "fssql-agent-test-") || strings.ContainsAny(config.Bucket, "'/\\") {
		t.Fatalf("FSSQL_MINIO_BUCKET must be a dedicated fssql-agent-test-* bucket")
	}
	if config.AccessKey == "" || config.SecretKey == "" {
		t.Fatal("MinIO test credentials are required")
	}
	return config
}

// withEnv 覆盖指定环境变量，避免 Windows 上重复变量名的行为差异。
func withEnv(overrides map[string]string) []string {
	env := os.Environ()
	out := make([]string, 0, len(env)+len(overrides))
	for _, entry := range env {
		key := entry
		if index := strings.IndexByte(entry, '='); index >= 0 {
			key = entry[:index]
		}
		if _, replaced := overrides[key]; replaced {
			continue
		}
		out = append(out, entry)
	}
	for key, value := range overrides {
		out = append(out, key+"="+value)
	}
	return out
}

func runMinIOAdminDelete(t *testing.T, config minioTestConfig, bucket, prefix string) {
	t.Helper()
	cmd := exec.Command(duckDBPython(), "minio_admin.py", "delete-prefix", bucket, prefix)
	cmd.Env = withEnv(map[string]string{
		"FSSQL_MINIO_ENDPOINT":   config.Endpoint,
		"FSSQL_MINIO_ACCESS_KEY": config.AccessKey,
		"FSSQL_MINIO_SECRET_KEY": config.SecretKey,
	})
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Errorf("MinIO cleanup failed: %v: %s", err, output)
	}
}

type duckDBS3Request struct {
	Endpoint   string   `json:"endpoint"`
	Region     string   `json:"region"`
	AccessKey  string   `json:"access_key"`
	SecretKey  string   `json:"secret_key"`
	Statements []string `json:"statements"`
	Queries    []string `json:"queries"`
}

type duckDBS3Response struct {
	DuckDBVersion string `json:"duckdb_version"`
	Statements    []struct {
		Error string `json:"error"`
	} `json:"statements"`
	Results []duckDBFileResult `json:"results"`
	Error   string             `json:"error"`
}

func runDuckDBS3Runner(t *testing.T, config minioTestConfig, statements, queries []string) duckDBS3Response {
	t.Helper()
	payload, err := json.Marshal(duckDBS3Request{
		Endpoint:   config.Endpoint,
		Region:     "us-east-1",
		AccessKey:  config.AccessKey,
		SecretKey:  config.SecretKey,
		Statements: statements,
		Queries:    queries,
	})
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(duckDBPython(), "duckdb_s3_runner.py")
	cmd.Stdin = bytes.NewReader(payload)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("DuckDB S3 runner: %v: %s", err, output)
	}
	var response duckDBS3Response
	if err := json.Unmarshal(output, &response); err != nil {
		t.Fatalf("decode DuckDB S3 response: %v: %s", err, output)
	}
	if response.Error != "" {
		t.Fatal(response.Error)
	}
	return response
}

func s3ObjectURI(bucket, prefix, name string) string {
	return "s3://" + bucket + "/" + prefix + "/" + name
}

func simpleProjection(source string) string {
	return `SELECT id, label, amount, ratio,
	               CAST(day AS VARCHAR) AS day,
	               CAST(ts AS VARCHAR) AS ts,
	               flag
	        FROM ` + source + ` ORDER BY id`
}

func simpleCSVSource(uri string) string {
	return "read_csv(" + compatSQLString(uri) + `, header=true, columns={'id':'BIGINT','label':'VARCHAR','amount':'BIGINT','ratio':'DOUBLE','day':'DATE','ts':'TIMESTAMP','flag':'BOOLEAN'})`
}

func simpleNDJSONSource(uri string) string {
	return "read_ndjson(" + compatSQLString(uri) + ")"
}

func simpleParquetSource(uri string) string {
	return "read_parquet(" + compatSQLString(uri) + ")"
}

// interopFixtureSQL 是 DuckDB 侧的简单 fixture，值与 interopRows 一致。
func interopFixtureSQL() string {
	return `WITH simple AS (
		SELECT 1 AS id, 'east' AS label, CAST(10 AS BIGINT) AS amount,
		       CAST(0.5 AS DOUBLE) AS ratio, CAST('2026-01-02' AS DATE) AS day,
		       CAST('2026-01-02 03:04:05.123456' AS TIMESTAMP) AS ts, true AS flag
		UNION ALL SELECT 2, 'east', NULL, NULL, NULL, NULL, false
		UNION ALL SELECT 3, 'west', 30, 1.25, CAST('2026-01-03' AS DATE),
		       CAST('2026-01-03 04:05:06.000001' AS TIMESTAMP), true
		UNION ALL SELECT 4, '汉字😊', 0, 2.5, CAST('2024-02-29' AS DATE),
		       CAST('2024-02-29 23:59:59' AS TIMESTAMP), false
		UNION ALL SELECT 5, 'quote,"newline
', NULL, 0.001, CAST('1970-01-01' AS DATE),
		       CAST('1969-12-31 23:59:59.999999' AS TIMESTAMP), true
	) SELECT * FROM simple`
}

func batchAggregateExpected() []string {
	var sumID, sumAmount int64
	for i := 0; i < batchFixtureRows; i++ {
		sumID += int64(i)
		sumAmount += int64(i % 17)
	}
	return []string{strconv.Itoa(batchFixtureRows), strconv.FormatInt(sumID, 10), strconv.FormatInt(sumAmount, 10)}
}

func batchFilterExpected() []string {
	var values []string
	for id := 2048; id < 2052; id++ {
		values = append(values, strconv.Itoa(id), strconv.Itoa(id%17))
	}
	return values
}

func batchGroupExpected() []string {
	var values []string
	for grp := 0; grp < 5; grp++ {
		var count, sum int64
		for i := 0; i < batchFixtureRows; i++ {
			if i%5 == grp {
				count++
				sum += int64(i % 17)
			}
		}
		values = append(values, strconv.Itoa(grp), strconv.FormatInt(count, 10), strconv.FormatInt(sum, 10))
	}
	return values
}

func batchTopExpected() []string {
	var values []string
	for id := batchFixtureRows - 1; id >= batchFixtureRows-5; id-- {
		values = append(values, strconv.Itoa(id))
	}
	return values
}

type s3QueryCase struct {
	id          string
	fssqlSQL    string
	duckSQL     string
	expected    []string
	expectError bool
}

// TestDuckDBMinIODifferential 在真实 MinIO 上做双向差分：
//   - fssql 写 S3，DuckDB httpfs 读回；
//   - DuckDB 写 S3，fssql 读回；
//   - 两侧执行相同的 S3 扫描/过滤/聚合/多文件查询；
//   - 缺失对象两侧都必须失败。
//
// 所有对象位于每次运行唯一的 prefix 下，结束后通过 boto3 清理。
func TestDuckDBMinIODifferential(t *testing.T) {
	if os.Getenv("FSSQL_REQUIRE_DUCKDB") != "1" {
		t.Skip("set FSSQL_REQUIRE_DUCKDB=1 in the acceptance gate")
	}
	registry := loadDeviationRegistry(t)
	resetDiffEvidence(t)
	config := loadMinIOTestConfig(t)
	prefix := fmt.Sprintf("deep-%d", time.Now().UnixNano())
	t.Cleanup(func() { runMinIOAdminDelete(t, config, config.Bucket, prefix) })

	db, err := gosql.OpenSQLDB(gosql.Config{
		S3: &gosql.S3Config{
			Endpoint:          config.Endpoint,
			Region:            "us-east-1",
			ForcePathStyle:    true,
			AllowInsecureHTTP: true,
			AccessKey:         config.AccessKey,
			SecretKey:         config.SecretKey,
			MaxRetries:        2,
			RequestTimeout:    15 * time.Second,
		},
		TableFunctions:         []gosql.TableFunction{interopTableFunction(), batchRowsTableFunction()},
		TempDir:                filepath.Join(t.TempDir(), "spill"),
		ManifestDir:            filepath.Join(t.TempDir(), "manifests"),
		MemoryLimitBytes:       64 << 20,
		TempLimitBytes:         128 << 20,
		SpillMemoryBudgetBytes: 2 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	// fssql 写入 S3。
	fssqlSimpleCSV := s3ObjectURI(config.Bucket, prefix, "fssql/simple.csv")
	fssqlSimpleNDJSON := s3ObjectURI(config.Bucket, prefix, "fssql/simple.ndjson")
	fssqlTypedParquet := s3ObjectURI(config.Bucket, prefix, "fssql/typed.parquet")
	fssqlPart1 := s3ObjectURI(config.Bucket, prefix, "fssql/parts/part1.parquet")
	fssqlPart2 := s3ObjectURI(config.Bucket, prefix, "fssql/parts/part2.parquet")
	fssqlBatchParquet := s3ObjectURI(config.Bucket, prefix, "fssql/batch.parquet")
	fssqlStatements := []string{
		"COPY (SELECT * FROM interop_rows() ORDER BY id) TO " + compatSQLString(fssqlSimpleCSV) + " (FORMAT CSV, HEADER)",
		"COPY (SELECT * FROM interop_rows() ORDER BY id) TO " + compatSQLString(fssqlSimpleNDJSON) + " (FORMAT NDJSON)",
		"COPY (" + typedFixtureSQL() + ") TO " + compatSQLString(fssqlTypedParquet) + " (FORMAT PARQUET, COMPRESSION SNAPPY)",
		"COPY (SELECT * FROM interop_rows() WHERE id <= 2 ORDER BY id) TO " + compatSQLString(fssqlPart1) + " (FORMAT PARQUET, COMPRESSION SNAPPY)",
		"COPY (SELECT * FROM interop_rows() WHERE id > 2 ORDER BY id) TO " + compatSQLString(fssqlPart2) + " (FORMAT PARQUET, COMPRESSION SNAPPY)",
		"COPY (SELECT * FROM batch_rows() ORDER BY id) TO " + compatSQLString(fssqlBatchParquet) + " (FORMAT PARQUET, COMPRESSION SNAPPY)",
	}
	for index, statement := range fssqlStatements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatalf("fssql S3 COPY failed: %v", err)
		}
		recordDiffEvidence(t, diffEvidence{Suite: "minio-s3", Case: fmt.Sprintf("fssql-write-%d", index), SQL: statement, Match: true})
	}

	// DuckDB 写入 S3。
	duckSimpleCSV := s3ObjectURI(config.Bucket, prefix, "duckdb/simple.csv")
	duckSimpleNDJSON := s3ObjectURI(config.Bucket, prefix, "duckdb/simple.ndjson")
	duckTypedParquet := s3ObjectURI(config.Bucket, prefix, "duckdb/typed.parquet")
	duckStatements := []string{
		"COPY (" + interopFixtureSQL() + ") TO " + compatSQLString(duckSimpleCSV) + " (FORMAT CSV, HEADER)",
		"COPY (" + interopFixtureSQL() + ") TO " + compatSQLString(duckSimpleNDJSON) + " (FORMAT NDJSON)",
		"COPY (" + typedFixtureSQL() + ") TO " + compatSQLString(duckTypedParquet) + " (FORMAT PARQUET, COMPRESSION SNAPPY)",
	}

	simple := expectedInteropValues()
	typed := expectedTypedValues()
	partsGlob := s3ObjectURI(config.Bucket, prefix, "fssql/parts/*.parquet")
	missing := s3ObjectURI(config.Bucket, prefix, "missing.parquet")
	queries := []s3QueryCase{
		// fssql 导出 → 两侧读取。
		{id: "fssql-csv", fssqlSQL: simpleProjection(simpleCSVSource(fssqlSimpleCSV)), duckSQL: simpleProjection(simpleCSVSource(fssqlSimpleCSV)), expected: simple},
		{id: "fssql-ndjson", fssqlSQL: simpleProjection(simpleNDJSONSource(fssqlSimpleNDJSON)), duckSQL: simpleProjection(simpleNDJSONSource(fssqlSimpleNDJSON)), expected: simple},
		{id: "fssql-typed-parquet", fssqlSQL: typedProjection(simpleParquetSource(fssqlTypedParquet)), duckSQL: typedProjection(simpleParquetSource(fssqlTypedParquet)), expected: typed},
		{id: "fssql-multi-file", fssqlSQL: "SELECT count(*), sum(id) FROM read_parquet(" + compatSQLString(partsGlob) + ")", duckSQL: "SELECT count(*), sum(id) FROM read_parquet(" + compatSQLString(partsGlob) + ")", expected: []string{"5", "15"}},

		// DuckDB 导出 → 两侧读取。
		{id: "duckdb-csv", fssqlSQL: simpleProjection(simpleCSVSource(duckSimpleCSV)), duckSQL: simpleProjection(simpleCSVSource(duckSimpleCSV)), expected: simple},
		{id: "duckdb-ndjson", fssqlSQL: simpleProjection(simpleNDJSONSource(duckSimpleNDJSON)), duckSQL: simpleProjection(simpleNDJSONSource(duckSimpleNDJSON)), expected: simple},
		{id: "duckdb-typed-parquet", fssqlSQL: typedProjection(simpleParquetSource(duckTypedParquet)), duckSQL: typedProjection(simpleParquetSource(duckTypedParquet)), expected: typed},

		// 同一份 S3 Parquet 上的扫描/过滤/聚合/排序。
		{id: "batch-aggregate", fssqlSQL: "SELECT count(*), sum(id), sum(amount) FROM read_parquet(" + compatSQLString(fssqlBatchParquet) + ")", duckSQL: "SELECT count(*), sum(id), sum(amount) FROM read_parquet(" + compatSQLString(fssqlBatchParquet) + ")", expected: batchAggregateExpected()},
		{id: "batch-filter", fssqlSQL: "SELECT id, amount FROM read_parquet(" + compatSQLString(fssqlBatchParquet) + ") WHERE id >= 2048 AND id < 2052 ORDER BY id", duckSQL: "SELECT id, amount FROM read_parquet(" + compatSQLString(fssqlBatchParquet) + ") WHERE id >= 2048 AND id < 2052 ORDER BY id", expected: batchFilterExpected()},
		{id: "batch-group", fssqlSQL: "SELECT grp, count(*), sum(amount) FROM read_parquet(" + compatSQLString(fssqlBatchParquet) + ") GROUP BY grp ORDER BY grp", duckSQL: "SELECT grp, count(*), sum(amount) FROM read_parquet(" + compatSQLString(fssqlBatchParquet) + ") GROUP BY grp ORDER BY grp", expected: batchGroupExpected()},
		{id: "batch-topn", fssqlSQL: "SELECT id FROM read_parquet(" + compatSQLString(fssqlBatchParquet) + ") ORDER BY id DESC LIMIT 5", duckSQL: "SELECT id FROM read_parquet(" + compatSQLString(fssqlBatchParquet) + ") ORDER BY id DESC LIMIT 5", expected: batchTopExpected()},

		// 缺失对象必须两侧都失败。
		{id: "missing-object", fssqlSQL: "SELECT * FROM read_parquet(" + compatSQLString(missing) + ")", duckSQL: "SELECT * FROM read_parquet(" + compatSQLString(missing) + ")", expectError: true},
	}

	duckQueries := make([]string, len(queries))
	for i := range queries {
		duckQueries[i] = queries[i].duckSQL
	}
	reference := runDuckDBS3Runner(t, config, duckStatements, duckQueries)
	if reference.DuckDBVersion != registry.Reference.Version {
		t.Fatalf("DuckDB runner version=%q registry=%q", reference.DuckDBVersion, registry.Reference.Version)
	}
	if len(reference.Statements) != len(duckStatements) || len(reference.Results) != len(queries) {
		t.Fatalf("DuckDB response statements=%d queries=%d", len(reference.Statements), len(reference.Results))
	}
	for index, statement := range reference.Statements {
		if statement.Error != "" {
			t.Fatalf("DuckDB S3 statement %d failed: %s", index, statement.Error)
		}
		recordDiffEvidence(t, diffEvidence{Suite: "minio-s3", Case: fmt.Sprintf("duckdb-write-%d", index), SQL: duckStatements[index], Match: true})
	}

	for index, query := range queries {
		duck := reference.Results[index]
		ours, columns, ourErr := queryValuesWithColumns(ctx, db, query.fssqlSQL)
		evidence := diffEvidence{
			Suite:         "minio-s3",
			Case:          query.id,
			SQL:           query.fssqlSQL,
			FSSQLValues:   ours,
			DuckDBValues:  duck.Values,
			FSSQLColumns:  columns,
			DuckDBColumns: duck.Columns,
			FSSQLError:    errorText(ourErr),
			DuckDBError:   duck.Error,
		}
		if query.expectError {
			evidence.Match = ourErr != nil && duck.Error != ""
			recordDiffEvidence(t, evidence)
			if !evidence.Match {
				t.Fatalf("%s: expected both engines to fail; fssql error=%v DuckDB error=%q", query.id, ourErr, duck.Error)
			}
			if os.Getenv("FSSQL_DIFF_VERBOSE") == "1" {
				t.Logf("%s\n  fssql  error = %v\n  duckdb error = %s", query.id, ourErr, duck.Error)
			}
			continue
		}
		evidence.Match = ourErr == nil && duck.Error == "" && columns == duck.Columns &&
			reflect.DeepEqual(ours, query.expected) && reflect.DeepEqual(duck.Values, query.expected)
		recordDiffEvidence(t, evidence)
		if !evidence.Match {
			t.Fatalf("%s: fssql columns=%d values=%v err=%v; DuckDB columns=%d values=%v err=%s; expected=%v",
				query.id, columns, ours, ourErr, duck.Columns, duck.Values, duck.Error, query.expected)
		}
		logDiffValues(t, query.id, ours, duck.Values)
	}
}

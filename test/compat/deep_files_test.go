package compat_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

// interopRow 是文件互操作 fixture 的一行；指针字段表示可 NULL 列。
type interopRow struct {
	id     int64
	label  string
	amount *int64
	ratio  *float64
	day    *time.Time
	ts     *time.Time
	flag   bool
}

// interopRows 覆盖 NULL、Unicode、CSV/JSON 需要转义的字符、闰日和微秒边界。
var interopRows = []interopRow{
	{id: 1, label: "east", amount: interopInt64Ptr(10), ratio: interopFloat64Ptr(0.5), day: dayPtr(2026, 1, 2), ts: tsPtr(2026, 1, 2, 3, 4, 5, 123456000), flag: true},
	{id: 2, label: "east", amount: nil, ratio: nil, day: nil, ts: nil, flag: false},
	{id: 3, label: "west", amount: interopInt64Ptr(30), ratio: interopFloat64Ptr(1.25), day: dayPtr(2026, 1, 3), ts: tsPtr(2026, 1, 3, 4, 5, 6, 1000), flag: true},
	{id: 4, label: "汉字😊", amount: interopInt64Ptr(0), ratio: interopFloat64Ptr(2.5), day: dayPtr(2024, 2, 29), ts: tsPtr(2024, 2, 29, 23, 59, 59, 0), flag: false},
	{id: 5, label: "quote,\"newline\n", amount: nil, ratio: interopFloat64Ptr(0.001), day: dayPtr(1970, 1, 1), ts: tsPtr(1969, 12, 31, 23, 59, 59, 999999000), flag: true},
}

func interopInt64Ptr(value int64) *int64       { return &value }
func interopFloat64Ptr(value float64) *float64 { return &value }

func dayPtr(year int, month time.Month, day int) *time.Time {
	value := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	return &value
}

func tsPtr(year int, month time.Month, day, hour, minute, second, nanos int) *time.Time {
	value := time.Date(year, month, day, hour, minute, second, nanos, time.UTC)
	return &value
}

// interopTableFunction 把 typed fixture 交给引擎，保证导出文件包含真实
// BIGINT/DOUBLE/DATE/TIMESTAMP/BOOLEAN 列，而不是字符串。
func interopTableFunction() gosql.TableFunction {
	return gosql.TableFunction{
		Name: "interop_rows",
		Columns: []gosql.Column{
			{Name: "id", Type: gosql.Type{ID: "BIGINT"}},
			{Name: "label", Type: gosql.Type{ID: "VARCHAR"}},
			{Name: "amount", Type: gosql.Type{ID: "BIGINT"}, Nullable: true},
			{Name: "ratio", Type: gosql.Type{ID: "DOUBLE"}, Nullable: true},
			{Name: "day", Type: gosql.Type{ID: "DATE"}, Nullable: true},
			{Name: "ts", Type: gosql.Type{ID: "TIMESTAMP"}, Nullable: true},
			{Name: "flag", Type: gosql.Type{ID: "BOOLEAN"}},
		},
		Open: func(context.Context) (gosql.BatchReader, error) {
			return &interopReader{}, nil
		},
	}
}

type interopReader struct{ next int }

func (r *interopReader) Next(ctx context.Context, writer gosql.BatchWriter) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if r.next >= len(interopRows) {
		return 0, io.EOF
	}
	n := writer.Capacity()
	if remaining := len(interopRows) - r.next; n > remaining {
		n = remaining
	}
	for i := 0; i < n; i++ {
		row := interopRows[r.next+i]
		if err := writer.SetInt64(i, 0, row.id); err != nil {
			return 0, err
		}
		if err := writer.SetString(i, 1, row.label); err != nil {
			return 0, err
		}
		if row.amount == nil {
			if err := writer.SetNull(i, 2); err != nil {
				return 0, err
			}
		} else if err := writer.SetInt64(i, 2, *row.amount); err != nil {
			return 0, err
		}
		if row.ratio == nil {
			if err := writer.SetNull(i, 3); err != nil {
				return 0, err
			}
		} else if err := writer.SetFloat64(i, 3, *row.ratio); err != nil {
			return 0, err
		}
		if row.day == nil {
			if err := writer.SetNull(i, 4); err != nil {
				return 0, err
			}
		} else if err := writer.SetDate(i, 4, *row.day); err != nil {
			return 0, err
		}
		if row.ts == nil {
			if err := writer.SetNull(i, 5); err != nil {
				return 0, err
			}
		} else if err := writer.SetTimestamp(i, 5, *row.ts); err != nil {
			return 0, err
		}
		if err := writer.SetBool(i, 6, row.flag); err != nil {
			return 0, err
		}
	}
	r.next += n
	if r.next == len(interopRows) {
		return n, io.EOF
	}
	return n, nil
}

func (r *interopReader) Close() error { return nil }

// expectedInteropValues 返回与 queryValues 相同的扁平规范化值。
func expectedInteropValues() []string {
	var values []string
	for _, row := range interopRows {
		values = append(values, strconv.FormatInt(row.id, 10), row.label)
		if row.amount == nil {
			values = append(values, "NULL")
		} else {
			values = append(values, strconv.FormatInt(*row.amount, 10))
		}
		if row.ratio == nil {
			values = append(values, "NULL")
		} else {
			values = append(values, strconv.FormatFloat(*row.ratio, 'g', 17, 64))
		}
		if row.day == nil {
			values = append(values, "NULL")
		} else {
			values = append(values, row.day.Format("2006-01-02"))
		}
		if row.ts == nil {
			values = append(values, "NULL")
		} else {
			values = append(values, row.ts.Format("2006-01-02 15:04:05.999999"))
		}
		values = append(values, strconv.FormatBool(row.flag))
	}
	return values
}

func compatSQLString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// fssqlFileReadSQL 用显式 schema 读取 CSV，避免推断差异；时间列投影为
// VARCHAR，与 DuckDB runner 的规范化保持一致。
func fssqlFileReadSQL(format, uri string) string {
	var source string
	switch format {
	case "csv":
		source = "read_csv(" + compatSQLString(uri) + `, header=true, columns={'id':'BIGINT','label':'VARCHAR','amount':'BIGINT','ratio':'DOUBLE','day':'DATE','ts':'TIMESTAMP','flag':'BOOLEAN'})`
	case "json":
		source = "read_json(" + compatSQLString(uri) + ")"
	case "ndjson":
		source = "read_ndjson(" + compatSQLString(uri) + ")"
	case "parquet":
		source = "read_parquet(" + compatSQLString(uri) + ")"
	default:
		panic("unsupported format " + format)
	}
	return `SELECT id, label, amount, ratio,
	               CAST(day AS VARCHAR) AS day,
	               CAST(ts AS VARCHAR) AS ts,
	               flag
	        FROM ` + source + ` ORDER BY id`
}

type duckDBFileResult struct {
	Values  []string `json:"values"`
	Columns int      `json:"columns"`
	Error   string   `json:"error"`
}

type duckDBFileWriteResponse struct {
	Formats       map[string]duckDBFileResult `json:"formats"`
	DuckDBVersion string                      `json:"duckdb_version"`
	Error         string                      `json:"error"`
}

func runDuckDBFileRunner(t *testing.T, args ...string) []byte {
	t.Helper()
	cmd := exec.Command(duckDBPython(), append([]string{"duckdb_file_runner.py"}, args...)...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("DuckDB file runner %v: %v: %s", args, err, output)
	}
	return output
}

// TestDuckDBFileInterop 做双向文件互操作差分：
//  1. fssql 导出 CSV/JSON/NDJSON/Parquet，DuckDB 与 fssql 分别读回并比较；
//  2. DuckDB 导出 CSV/NDJSON/Parquet，fssql 读回并与 DuckDB 自身读回比较。
//
// 两侧结果还必须等于同一份 typed fixture 的规范值，避免两个引擎共享同一
// 读取错误却互相“对齐”。DuckDB 2.0 的 FORMAT JSON 输出是逐行 NDJSON，
// 因此反向方向使用 NDJSON 对应 fssql 的 read_ndjson。
func TestDuckDBFileInterop(t *testing.T) {
	if os.Getenv("FSSQL_REQUIRE_DUCKDB") != "1" {
		t.Skip("set FSSQL_REQUIRE_DUCKDB=1 in the acceptance gate")
	}
	registry := loadDeviationRegistry(t)
	resetDiffEvidence(t)
	dir := t.TempDir()
	db, err := gosql.OpenSQLDB(gosql.Config{
		LocalFiles:             &gosql.LocalFileConfig{RootDir: dir},
		TableFunctions:         []gosql.TableFunction{interopTableFunction()},
		TempDir:                filepath.Join(dir, "spill"),
		ManifestDir:            filepath.Join(dir, "manifests"),
		MemoryLimitBytes:       64 << 20,
		TempLimitBytes:         128 << 20,
		SpillMemoryBudgetBytes: 2 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	expected := expectedInteropValues()

	fssqlFormats := map[string]string{
		"csv":     "FORMAT CSV, HEADER",
		"json":    "FORMAT JSON",
		"ndjson":  "FORMAT NDJSON",
		"parquet": "FORMAT PARQUET, COMPRESSION SNAPPY",
	}
	extensions := map[string]string{"csv": "csv", "json": "json", "ndjson": "ndjson", "parquet": "parquet"}
	for format, options := range fssqlFormats {
		path := filepath.Join(dir, "fssql."+extensions[format])
		uri, err := gosql.FileURI(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf(
			"COPY (SELECT * FROM interop_rows() ORDER BY id) TO %s (%s)",
			compatSQLString(uri), options)); err != nil {
			t.Fatalf("fssql COPY %s: %v", format, err)
		}

		ours, columns, err := queryValuesWithColumns(ctx, db, fssqlFileReadSQL(format, uri))
		if err != nil {
			t.Fatalf("fssql read %s: %v", format, err)
		}
		if columns != 7 || !reflect.DeepEqual(ours, expected) {
			t.Fatalf("fssql read %s: columns=%d\nvalues=%v\nexpected=%v", format, columns, ours, expected)
		}

		output := runDuckDBFileRunner(t, "read", format, path)
		var reference duckDBFileResult
		if err := json.Unmarshal(output, &reference); err != nil {
			t.Fatalf("decode DuckDB %s: %v: %s", format, err, output)
		}
		if reference.Error != "" {
			t.Fatalf("DuckDB read %s: %s", format, reference.Error)
		}
		if reference.Columns != 7 || !reflect.DeepEqual(reference.Values, expected) {
			t.Fatalf("DuckDB read %s: columns=%d\nvalues=%v\nexpected=%v", format, reference.Columns, reference.Values, expected)
		}
		recordDiffEvidence(t, diffEvidence{
			Suite:         "file-interop",
			Case:          "fssql-export/" + format,
			SQL:           fssqlFileReadSQL(format, uri),
			FSSQLValues:   ours,
			DuckDBValues:  reference.Values,
			FSSQLColumns:  columns,
			DuckDBColumns: reference.Columns,
			Match:         reflect.DeepEqual(ours, reference.Values),
		})
		logDiffValues(t, "file-interop/fssql-export/"+format, ours, reference.Values)
	}

	output := runDuckDBFileRunner(t, "write", dir)
	var written duckDBFileWriteResponse
	if err := json.Unmarshal(output, &written); err != nil {
		t.Fatalf("decode DuckDB write: %v: %s", err, output)
	}
	if written.Error != "" {
		t.Fatalf("DuckDB write: %s", written.Error)
	}
	if written.DuckDBVersion != registry.Reference.Version {
		t.Fatalf("DuckDB runner version=%q registry=%q", written.DuckDBVersion, registry.Reference.Version)
	}
	for _, format := range []string{"csv", "ndjson", "parquet"} {
		reference, ok := written.Formats[format]
		if !ok {
			t.Fatalf("DuckDB write response missing %s", format)
		}
		if reference.Error != "" {
			t.Fatalf("DuckDB read-back %s: %s", format, reference.Error)
		}
		if reference.Columns != 7 || !reflect.DeepEqual(reference.Values, expected) {
			t.Fatalf("DuckDB read-back %s: columns=%d\nvalues=%v\nexpected=%v", format, reference.Columns, reference.Values, expected)
		}

		path := filepath.Join(dir, "duckdb."+format)
		uri, err := gosql.FileURI(path)
		if err != nil {
			t.Fatal(err)
		}
		ours, columns, err := queryValuesWithColumns(ctx, db, fssqlFileReadSQL(format, uri))
		if err != nil {
			t.Fatalf("fssql read DuckDB %s: %v", format, err)
		}
		if columns != 7 || !reflect.DeepEqual(ours, expected) {
			t.Fatalf("fssql read DuckDB %s: columns=%d\nvalues=%v\nexpected=%v", format, columns, ours, expected)
		}
		recordDiffEvidence(t, diffEvidence{
			Suite:         "file-interop",
			Case:          "duckdb-export/" + format,
			SQL:           fssqlFileReadSQL(format, uri),
			FSSQLValues:   ours,
			DuckDBValues:  reference.Values,
			FSSQLColumns:  columns,
			DuckDBColumns: reference.Columns,
			Match:         reflect.DeepEqual(ours, reference.Values),
		})
		logDiffValues(t, "file-interop/duckdb-export/"+format, ours, reference.Values)
	}
}

// typedFixtureSQL 提供 DECIMAL(18,2)、DECIMAL(38,6)、UBIGINT、TIMESTAMP_NS
// 和 BLOB 行；HUGEINT 不包含在内，因为 Parquet 导出映射当前明确拒绝它。
func typedFixtureSQL() string {
	return `WITH typed AS (
		SELECT 1 AS id,
		       CAST('12.34' AS DECIMAL(18,2)) AS dec_value,
		       CAST('123456789012345678901234567890.123456' AS DECIMAL(38,6)) AS wide_value,
		       CAST('18446744073709551615' AS UBIGINT) AS ubig_value,
		       CAST('2026-01-02 03:04:05.123456789' AS TIMESTAMP_NS) AS ns_value,
		       CAST('A\x00\xFF' AS BLOB) AS raw_value
		UNION ALL SELECT 2, NULL, NULL, NULL, NULL, NULL
		UNION ALL SELECT 3,
		       CAST('-0.01' AS DECIMAL(18,2)),
		       CAST('-0.000001' AS DECIMAL(38,6)),
		       CAST(0 AS UBIGINT),
		       CAST('1969-12-31 23:59:59.999999999' AS TIMESTAMP_NS),
		       CAST('' AS BLOB)
	) SELECT * FROM typed`
}

func typedProjection(source string) string {
	return `SELECT id,
	               CAST(dec_value AS VARCHAR) AS dec_value,
	               CAST(wide_value AS VARCHAR) AS wide_value,
	               CAST(ubig_value AS VARCHAR) AS ubig_value,
	               CAST(ns_value AS VARCHAR) AS ns_value,
	               hex(raw_value) AS raw_hex
	        FROM ` + source + ` ORDER BY id`
}

func expectedTypedValues() []string {
	return []string{
		"1", "12.34", "123456789012345678901234567890.123456", "18446744073709551615", "2026-01-02 03:04:05.123456789", "4100FF",
		"2", "NULL", "NULL", "NULL", "NULL", "NULL",
		"3", "-0.01", "-0.000001", "0", "1969-12-31 23:59:59.999999999", "",
	}
}

// TestDuckDBParquetTypeInterop 验证 Parquet 精确类型在 fssql 导出→DuckDB 读取
// 和 DuckDB 导出→fssql 读取两个方向上保持一致。
func TestDuckDBParquetTypeInterop(t *testing.T) {
	if os.Getenv("FSSQL_REQUIRE_DUCKDB") != "1" {
		t.Skip("set FSSQL_REQUIRE_DUCKDB=1 in the acceptance gate")
	}
	registry := loadDeviationRegistry(t)
	resetDiffEvidence(t)
	dir := t.TempDir()
	db, err := gosql.OpenSQLDB(gosql.Config{
		LocalFiles:             &gosql.LocalFileConfig{RootDir: dir},
		TempDir:                filepath.Join(dir, "spill"),
		ManifestDir:            filepath.Join(dir, "manifests"),
		MemoryLimitBytes:       64 << 20,
		TempLimitBytes:         128 << 20,
		SpillMemoryBudgetBytes: 2 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	expected := expectedTypedValues()

	fssqlPath := filepath.Join(dir, "fssql_typed.parquet")
	fssqlURI, err := gosql.FileURI(fssqlPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		"COPY (%s) TO %s (FORMAT PARQUET, COMPRESSION SNAPPY)",
		typedFixtureSQL(), compatSQLString(fssqlURI))); err != nil {
		t.Fatalf("fssql typed Parquet export: %v", err)
	}
	ours, columns, err := queryValuesWithColumns(ctx, db, typedProjection("read_parquet("+compatSQLString(fssqlURI)+")"))
	if err != nil {
		t.Fatalf("fssql typed Parquet read: %v", err)
	}
	if columns != 6 || !reflect.DeepEqual(ours, expected) {
		t.Fatalf("fssql typed Parquet: columns=%d\nvalues=%v\nexpected=%v", columns, ours, expected)
	}
	output := runDuckDBFileRunner(t, "read-typed", fssqlPath)
	var reference duckDBFileResult
	if err := json.Unmarshal(output, &reference); err != nil {
		t.Fatalf("decode DuckDB typed read: %v: %s", err, output)
	}
	if reference.Error != "" {
		t.Fatalf("DuckDB typed read: %s", reference.Error)
	}
	if reference.Columns != 6 || !reflect.DeepEqual(reference.Values, expected) {
		t.Fatalf("DuckDB typed read: columns=%d\nvalues=%v\nexpected=%v", reference.Columns, reference.Values, expected)
	}
	recordDiffEvidence(t, diffEvidence{
		Suite:         "parquet-types",
		Case:          "fssql-export",
		SQL:           typedProjection("read_parquet(<fssql-typed>)"),
		FSSQLValues:   ours,
		DuckDBValues:  reference.Values,
		FSSQLColumns:  columns,
		DuckDBColumns: reference.Columns,
		Match:         reflect.DeepEqual(ours, reference.Values),
	})
	logDiffValues(t, "parquet-types/fssql-export", ours, reference.Values)

	duckPath := filepath.Join(dir, "duckdb_typed.parquet")
	output = runDuckDBFileRunner(t, "write-typed", duckPath)
	var typedWrite struct {
		Values        []string `json:"values"`
		Columns       int      `json:"columns"`
		Error         string   `json:"error"`
		DuckDBVersion string   `json:"duckdb_version"`
	}
	if err := json.Unmarshal(output, &typedWrite); err != nil {
		t.Fatalf("decode DuckDB typed write: %v: %s", err, output)
	}
	if typedWrite.Error != "" {
		t.Fatalf("DuckDB typed write: %s", typedWrite.Error)
	}
	if typedWrite.DuckDBVersion != registry.Reference.Version {
		t.Fatalf("DuckDB typed version=%q registry=%q", typedWrite.DuckDBVersion, registry.Reference.Version)
	}
	if typedWrite.Columns != 6 || !reflect.DeepEqual(typedWrite.Values, expected) {
		t.Fatalf("DuckDB typed write read-back: columns=%d\nvalues=%v\nexpected=%v", typedWrite.Columns, typedWrite.Values, expected)
	}
	duckURI, err := gosql.FileURI(duckPath)
	if err != nil {
		t.Fatal(err)
	}
	ours, columns, err = queryValuesWithColumns(ctx, db, typedProjection("read_parquet("+compatSQLString(duckURI)+")"))
	if err != nil {
		t.Fatalf("fssql read DuckDB typed Parquet: %v", err)
	}
	if columns != 6 || !reflect.DeepEqual(ours, expected) {
		t.Fatalf("fssql read DuckDB typed Parquet: columns=%d\nvalues=%v\nexpected=%v", columns, ours, expected)
	}
	recordDiffEvidence(t, diffEvidence{
		Suite:         "parquet-types",
		Case:          "duckdb-export",
		SQL:           typedProjection("read_parquet(<duckdb-typed>)"),
		FSSQLValues:   ours,
		DuckDBValues:  typedWrite.Values,
		FSSQLColumns:  columns,
		DuckDBColumns: typedWrite.Columns,
		Match:         reflect.DeepEqual(ours, typedWrite.Values),
	})
	logDiffValues(t, "parquet-types/duckdb-export", ours, typedWrite.Values)
}

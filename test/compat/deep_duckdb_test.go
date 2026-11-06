package compat_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"github.com/steamwo/fssql/pkg/gosql"
)

// deepSQLCase 是一条深度差分用例。
//
// 查询刻意把 DECIMAL/HUGEINT/DATE/TIMESTAMP 投影为 VARCHAR 或精确文本，
// 避免不同语言绑定对尾零、时区和时间格式的规范化差异掩盖真正的语义差异。
// expectError 表示两侧都必须以 SQL 错误失败，而不是比较值。
type deepSQLCase struct {
	id          string
	sql         string
	expectError bool
}

// batchFixtureRows = 2 × 2048 + 1，跨过默认 vector 批次边界。
const batchFixtureRows = 4097

// unionFixtureSQL 生成 DuckDB 侧的 UNION ALL 内联表。
//
// fssql 侧使用 batch_rows() 表函数提供同样的 typed 行，避免让解析器的
// UNION ALL 线性链成为跨批次语义测试的瓶颈；DuckDB 侧仍用同一份内联数据，
// 保证两侧输入完全一致。
func unionFixtureSQL(rows int) string {
	var builder strings.Builder
	builder.WriteString("(SELECT CAST(0 AS BIGINT) AS id, CAST(0 AS BIGINT) AS grp, CAST(0 AS BIGINT) AS amount")
	for i := 1; i < rows; i++ {
		fmt.Fprintf(&builder, " UNION ALL SELECT %d, %d, %d", i, i%5, i%17)
	}
	builder.WriteString(")")
	return builder.String()
}

// batchRowsTableFunction 为 fssql 侧提供与 unionFixtureSQL 相同的 4097 行。
func batchRowsTableFunction() gosql.TableFunction {
	return gosql.TableFunction{
		Name: "batch_rows",
		Columns: []gosql.Column{
			{Name: "id", Type: gosql.Type{ID: "BIGINT"}},
			{Name: "grp", Type: gosql.Type{ID: "BIGINT"}},
			{Name: "amount", Type: gosql.Type{ID: "BIGINT"}},
		},
		Open: func(context.Context) (gosql.BatchReader, error) {
			return &batchRowsReader{}, nil
		},
	}
}

type batchRowsReader struct{ next int }

func (r *batchRowsReader) Next(ctx context.Context, writer gosql.BatchWriter) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if r.next >= batchFixtureRows {
		return 0, io.EOF
	}
	n := writer.Capacity()
	if remaining := batchFixtureRows - r.next; n > remaining {
		n = remaining
	}
	for i := 0; i < n; i++ {
		id := r.next + i
		if err := writer.SetInt64(i, 0, int64(id)); err != nil {
			return 0, err
		}
		if err := writer.SetInt64(i, 1, int64(id%5)); err != nil {
			return 0, err
		}
		if err := writer.SetInt64(i, 2, int64(id%17)); err != nil {
			return 0, err
		}
	}
	r.next += n
	if r.next == batchFixtureRows {
		return n, io.EOF
	}
	return n, nil
}

func (r *batchRowsReader) Close() error { return nil }

// deepSQLCases 覆盖现有 stable.slt 之外的窗口、采样、连接、聚合、函数、
// 类型转换、谓词、子查询与 CTE 组合。每条查询自包含，便于两侧独立执行。
func deepSQLCases() []deepSQLCase {
	const rows = `WITH t AS (
		SELECT 1 AS id, 'a' AS grp, CAST(10 AS BIGINT) AS amount
		UNION ALL SELECT 2, 'a', 20
		UNION ALL SELECT 3, 'b', 30
		UNION ALL SELECT 4, 'b', NULL
		UNION ALL SELECT 5, 'c', 50
	)`
	const leftRight = `WITH l AS (
		SELECT 1 AS k, 'l1' AS lv
		UNION ALL SELECT 2, 'l2'
		UNION ALL SELECT 3, 'l3'
		UNION ALL SELECT CAST(NULL AS BIGINT), 'ln'
	), r AS (
		SELECT 2 AS k, 'r2' AS rv
		UNION ALL SELECT 3, 'r3'
		UNION ALL SELECT 4, 'r4'
		UNION ALL SELECT CAST(NULL AS BIGINT), 'rn'
	)`
	const sampleRows = `(SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5)`

	return []deepSQLCase{
		// 窗口：分区编号、显式 frame、全局窗口、多窗口排序隔离。
		{id: "WINDOW-001", sql: rows + ` SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id) AS rn FROM t ORDER BY id`},
		{id: "WINDOW-002", sql: rows + ` SELECT id, SUM(amount) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prior FROM t ORDER BY id`},
		{id: "WINDOW-003", sql: rows + ` SELECT id, COUNT(*) OVER (), SUM(amount) OVER () FROM t ORDER BY id`},
		{id: "WINDOW-004", sql: rows + ` SELECT id, ROW_NUMBER() OVER (ORDER BY id DESC), ROW_NUMBER() OVER (ORDER BY id) FROM t ORDER BY id`},
		{id: "WINDOW-005", sql: rows + ` SELECT id, COUNT(*) OVER () + 1 FROM t WHERE amount IS NOT NULL ORDER BY id`},

		// 采样：固定行数/百分比、0/100 百分比与确定性成员。
		{id: "SAMPLE-001", sql: `SELECT count(*) FROM ` + sampleRows + ` s USING SAMPLE reservoir(3 ROWS) REPEATABLE(42)`},
		{id: "SAMPLE-002", sql: `SELECT count(*) FROM ` + sampleRows + ` s USING SAMPLE reservoir(60 PERCENT) REPEATABLE(7)`},
		{id: "SAMPLE-003", sql: `SELECT count(*) FROM ` + sampleRows + ` s USING SAMPLE bernoulli(100 PERCENT) REPEATABLE(42)`},
		{id: "SAMPLE-004", sql: `SELECT count(*) FROM ` + sampleRows + ` s USING SAMPLE bernoulli(0 PERCENT) REPEATABLE(42)`},
		{id: "SAMPLE-005", sql: `SELECT id FROM ` + sampleRows + ` s USING SAMPLE reservoir(3 ROWS) REPEATABLE(42) ORDER BY id`},
		{id: "SAMPLE-006", sql: `SELECT id FROM ` + sampleRows + ` s TABLESAMPLE reservoir(2 ROWS) REPEATABLE(42) ORDER BY id`},

		// JOIN：NULL 键、重复键、外连接、SEMI/ANTI 与 USING 合并键。
		{id: "JOIN-001", sql: leftRight + ` SELECT lv, rv FROM l JOIN r ON l.k = r.k ORDER BY lv`},
		{id: "JOIN-002", sql: leftRight + ` SELECT lv, rv FROM l LEFT JOIN r ON l.k = r.k ORDER BY lv`},
		{id: "JOIN-003", sql: leftRight + ` SELECT lv, rv FROM l RIGHT JOIN r ON l.k = r.k ORDER BY rv`},
		{id: "JOIN-004", sql: leftRight + ` SELECT lv, rv FROM l FULL JOIN r ON l.k = r.k ORDER BY lv NULLS LAST, rv NULLS LAST`},
		{id: "JOIN-005", sql: leftRight + ` SELECT count(*) FROM l CROSS JOIN r`},
		{id: "JOIN-006", sql: leftRight + ` SELECT lv FROM l SEMI JOIN r ON l.k = r.k ORDER BY lv`},
		{id: "JOIN-007", sql: leftRight + ` SELECT lv FROM l ANTI JOIN r ON l.k = r.k ORDER BY lv`},
		{id: "JOIN-008", sql: leftRight + ` SELECT k, l.k, r.k, lv, rv FROM l FULL JOIN r USING (k) ORDER BY 1 NULLS LAST, lv NULLS LAST, rv NULLS LAST`},

		// 聚合：NULL、空输入、分组、HAVING、DISTINCT 与宽整数。
		{id: "AGG-001", sql: rows + ` SELECT count(*), count(amount), sum(amount), avg(amount), min(amount), max(amount) FROM t`},
		{id: "AGG-002", sql: rows + ` SELECT count(*), sum(amount), avg(amount), min(amount), max(amount) FROM t WHERE FALSE`},
		{id: "AGG-003", sql: rows + ` SELECT grp, count(*) FROM t GROUP BY grp HAVING count(*) > 1 ORDER BY grp`},
		{id: "AGG-004", sql: rows + ` SELECT count(DISTINCT grp), count(DISTINCT amount), sum(DISTINCT amount) FROM t`},
		{id: "AGG-005", sql: rows + ` SELECT grp, count(*), sum(amount) FROM t GROUP BY grp ORDER BY grp`},
		{id: "AGG-006", sql: `SELECT avg(CAST(1 AS BIGINT)), sum(CAST(1 AS BIGINT))`},
		{id: "AGG-007", sql: `SELECT sum(CAST('9223372036854775808' AS HUGEINT))`},

		// 字符串、数值、条件与时间函数。
		{id: "FUNC-001", sql: `SELECT length('你好😊'), length('')`},
		{id: "FUNC-002", sql: `SELECT substr('abcdef', 2, 3), substr('你好世界', 2, 2)`},
		{id: "FUNC-003", sql: `SELECT strpos('abcabc', 'bc'), strpos('abc', 'z')`},
		{id: "FUNC-004", sql: `SELECT replace('a-b-c', '-', '_')`},
		{id: "FUNC-005", sql: `SELECT repeat('ab', 3)`},
		{id: "FUNC-006", sql: `SELECT lower('AbC')`},
		{id: "FUNC-007", sql: `SELECT hex('abc')`},
		{id: "FUNC-008", sql: `SELECT md5('abc')`},
		{id: "FUNC-009", sql: `SELECT floor(CAST(-1.5 AS DOUBLE)), floor(CAST(1.5 AS DOUBLE))`},
		{id: "FUNC-010", sql: `SELECT mod(7, 3), mod(-7, 3)`},
		{id: "FUNC-011", sql: `SELECT least(3, 1, 2), greatest(3, 1, 2)`},
		{id: "FUNC-012", sql: `SELECT coalesce(NULL, NULL, 5), nullif(1, 1), nullif(1, 2)`},
		{id: "FUNC-013", sql: `SELECT 'a' || 'b' || NULL`},
		{id: "FUNC-014", sql: `SELECT strftime(CAST('2026-01-02 03:04:05.123456' AS TIMESTAMP), '%Y-%m-%d %H:%M:%S.%f')`},
		{id: "FUNC-015", sql: `SELECT CAST(strptime('2026-01-02 03:04:05.123456', '%Y-%m-%d %H:%M:%S.%f') AS VARCHAR)`},
		{id: "FUNC-016", sql: `SELECT CAST(date_trunc('month', CAST('2026-01-02 03:04:05' AS TIMESTAMP)) AS VARCHAR)`},
		{id: "FUNC-017", sql: `SELECT CAST(timezone('UTC', CAST('2026-01-02 03:04:05.123456' AS TIMESTAMP)) AS VARCHAR)`},

		// 类型转换、精确数值与舍入。
		{id: "TYPE-001", sql: `SELECT CAST(CAST('12.5' AS DECIMAL(10,2)) AS BIGINT), CAST(CAST('-12.5' AS DECIMAL(10,2)) AS BIGINT), CAST(CAST('-0.4' AS DECIMAL(10,2)) AS UBIGINT)`},
		{id: "TYPE-002", sql: `SELECT CAST('170141183460469231731687303715884105727' AS HUGEINT), CAST('18446744073709551615' AS UBIGINT)`},
		{id: "TYPE-003", sql: `SELECT CAST(CAST('123' AS HUGEINT) AS BIGINT), CAST(CAST(CAST('123' AS HUGEINT) AS DECIMAL(10,2)) AS VARCHAR)`},
		{id: "TYPE-004", sql: `SELECT CAST(CAST(1 AS TINYINT) AS BIGINT), CAST(CAST(255 AS UTINYINT) AS INTEGER)`},
		{id: "TYPE-004B", sql: `SELECT CAST(TRUE AS INTEGER), CAST(FALSE AS BIGINT), CAST(TRUE AS UTINYINT)`},
		{id: "TYPE-004C", sql: `SELECT CAST(0 AS BOOLEAN), CAST(2 AS BOOLEAN), CAST(-3 AS BOOLEAN), CAST(0.5 AS BOOLEAN), CAST(0.0 AS BOOLEAN)`},
		{id: "TYPE-005", sql: `SELECT CAST(CAST('2026-01-02' AS DATE) AS VARCHAR), CAST(CAST('2026-01-02 03:04:05.123456' AS TIMESTAMP) AS VARCHAR)`},
		{id: "TYPE-006", sql: `SELECT hex(CAST('A\x00\xFF' AS BLOB)), hex(CAST('' AS BLOB))`},

		// 谓词、NULL 三值逻辑与 UNION ALL。
		{id: "PRED-001", sql: `SELECT 1 IN (1, NULL), 1 NOT IN (2, NULL), NULL IN (NULL)`},
		{id: "PRED-002", sql: `SELECT 5 BETWEEN 1 AND 10, NULL BETWEEN 1 AND 3, 0 BETWEEN 1 AND NULL`},
		{id: "PRED-003", sql: `SELECT 'a_b' LIKE 'a\_b' ESCAPE '\', '你好' LIKE '你_', 'abc' NOT LIKE 'a%'`},
		{id: "PRED-004", sql: `SELECT 1 IS DISTINCT FROM NULL, NULL IS NOT DISTINCT FROM NULL, 1 IS DISTINCT FROM 1`},
		{id: "PRED-005", sql: `SELECT 1 AS k UNION ALL SELECT 2 UNION ALL SELECT 2 ORDER BY k`},
		{id: "PRED-006", sql: `WITH a AS (SELECT 1 AS k) SELECT k FROM a UNION ALL SELECT k + 1 FROM a ORDER BY k`},

		// 子查询、CTE 与 LATERAL。
		{id: "SUB-001", sql: rows + ` SELECT id FROM t WHERE amount > (SELECT avg(amount) FROM t) ORDER BY id`},
		{id: "SUB-002", sql: rows + ` SELECT id, EXISTS (SELECT 1 FROM t x WHERE x.grp = d.grp AND x.amount > d.amount) FROM t d WHERE d.amount IS NOT NULL ORDER BY id`},
		{id: "SUB-003", sql: rows + ` SELECT id, (SELECT count(*) FROM t x WHERE x.grp = d.grp) FROM t d ORDER BY id`},
		{id: "SUB-004", sql: rows + ` SELECT d.id, y.n FROM t d LEFT JOIN LATERAL (SELECT count(*) AS n FROM t x WHERE x.grp = d.grp AND x.id <= d.id) y ON TRUE ORDER BY d.id`},
		{id: "SUB-005", sql: `WITH t AS (
			SELECT 1 AS id, 'a' AS grp, CAST(10 AS BIGINT) AS amount
			UNION ALL SELECT 2, 'a', 20
			UNION ALL SELECT 3, 'b', 30
			UNION ALL SELECT 4, 'b', NULL
			UNION ALL SELECT 5, 'c', 50
		), c AS (
			SELECT grp, count(*) AS n FROM t GROUP BY grp
		) SELECT a.grp, a.n, b.n FROM c a JOIN c b ON a.grp = b.grp ORDER BY a.grp`},
		{id: "SUB-006", sql: rows + ` SELECT d.id, d.id IN (SELECT x.id FROM t x WHERE x.grp = d.grp OR x.amount > d.amount) FROM t d WHERE d.amount IS NOT NULL ORDER BY d.id`},
		{id: "SUB-007", sql: rows + ` SELECT d.id, (SELECT x.id FROM t x WHERE x.grp = d.grp ORDER BY x.id DESC LIMIT 1) FROM t d ORDER BY d.id`},
		{id: "SUB-008", sql: rows + ` SELECT d.id, y.n, y.total FROM t d LEFT JOIN LATERAL (SELECT count(*) AS n, sum(x.amount) AS total FROM t x WHERE x.grp = d.grp AND x.amount > 100) y ON TRUE ORDER BY d.id`},

		// 多键/重复键/NULL 安全连接。
		{id: "JOIN-009", sql: `WITH a AS (
			SELECT 1 AS k1, 1 AS k2, 'a1' AS av
			UNION ALL SELECT 1, 1, 'a2'
			UNION ALL SELECT 1, 2, 'a3'
			UNION ALL SELECT 2, 1, 'a4'
		), b AS (
			SELECT 1 AS k1, 1 AS k2, 'b1' AS bv
			UNION ALL SELECT 1, 1, 'b2'
			UNION ALL SELECT 2, 1, 'b3'
		) SELECT av, bv FROM a JOIN b ON a.k1 = b.k1 AND a.k2 = b.k2 ORDER BY av, bv`},
		{id: "JOIN-010", sql: `WITH a AS (SELECT 1 AS k UNION ALL SELECT CAST(NULL AS BIGINT)), b AS (SELECT 1 AS k UNION ALL SELECT CAST(NULL AS BIGINT))
			SELECT a.k, b.k FROM a JOIN b ON a.k IS NOT DISTINCT FROM b.k ORDER BY 1 NULLS FIRST, 2 NULLS FIRST`},

		// 字符串与数值边界。
		{id: "FUNC-018", sql: `SELECT substr('abc', 0, 2), substr('abc', -2, 2), substr('abc', 2)`},
		{id: "FUNC-019", sql: `SELECT repeat('', 3), replace('aaa', 'aa', 'b')`},
		{id: "FUNC-020", sql: `SELECT length('😊'), hex(CAST('abc' AS BLOB))`},
		{id: "FUNC-021", sql: `SELECT strftime(CAST('2024-02-29' AS DATE), '%Y-%m-%d'), CAST(date_trunc('day', CAST('1969-12-31 23:59:59.999999' AS TIMESTAMP)) AS VARCHAR)`},
		{id: "NUM-001", sql: `SELECT CAST(2 AS BIGINT) * CAST(3 AS BIGINT), CAST(7 AS BIGINT) / CAST(2 AS BIGINT), CAST(7.0 AS DOUBLE) / CAST(2 AS DOUBLE)`},
		{id: "NUM-002", sql: `SELECT floor(CAST('-0.5' AS DOUBLE)), floor(CAST('0.5' AS DOUBLE)), mod(-7, 3), mod(7, -3)`},

		// 三值逻辑与 Unicode 模式匹配。
		{id: "PRED-007", sql: `SELECT NULL AND TRUE, NULL OR FALSE, NOT NULL`},
		{id: "PRED-008", sql: `SELECT 'abc' LIKE '%', '' LIKE '_', '😊' LIKE '__', '😊' LIKE '_'`},

		// 跨批次：4097 行 = 2 × 2048 + 1，覆盖 V-1/V/V+1/2V+1 边界。
		{id: "BATCH-001", sql: `SELECT count(*) FROM {batch} q`},
		{id: "BATCH-002", sql: `SELECT id, rn FROM (SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id) AS rn FROM {batch} q) w WHERE id IN (0, 1, 2047, 2048, 2049, 4096) ORDER BY id`},
		{id: "BATCH-003", sql: `SELECT grp, count(*), sum(amount) FROM {batch} q GROUP BY grp ORDER BY grp`},
		{id: "BATCH-004", sql: `SELECT id FROM {batch} q ORDER BY amount DESC, id LIMIT 5 OFFSET 2045`},
		{id: "BATCH-005", sql: `SELECT count(DISTINCT grp), count(*) FROM {batch} q`},
		{id: "BATCH-006", sql: `SELECT count(*) FROM {batch} q USING SAMPLE reservoir(100 ROWS) REPEATABLE(42)`},
		{id: "BATCH-007", sql: `SELECT id, rn, prior FROM (SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id) AS rn, SUM(amount) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prior FROM {batch} q) w WHERE id IN (0, 1, 2047, 2048, 2049, 4096) ORDER BY id`},

		// 错误类别：两侧都必须失败。
		{id: "ERR-001", sql: `SELECT CAST(300 AS UTINYINT)`, expectError: true},
		{id: "ERR-002", sql: `SELECT CAST('bad' AS INTEGER)`, expectError: true},
		{id: "ERR-003", sql: `SELECT CAST(7 AS BIGINT) % 0`, expectError: true},
		{id: "ERR-004", sql: `SELECT CAST('170141183460469231731687303715884105728' AS HUGEINT)`, expectError: true},
		{id: "ERR-005", sql: `SELECT CAST('A\x4' AS BLOB)`, expectError: true},
		{id: "ERR-006", sql: `SELECT CAST('A\B' AS BLOB)`, expectError: true},
	}
}

// queryValuesWithColumns 与 queryValues 使用相同的规范化，但额外返回列数，
// 以便在扁平值之外核对投影形状。
func queryValuesWithColumns(ctx context.Context, db *sql.DB, sqlText string) ([]string, int, error) {
	rows, err := db.QueryContext(ctx, sqlText)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, 0, err
	}
	var values []string
	for rows.Next() {
		dest := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range dest {
			ptrs[i] = &dest[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, 0, err
		}
		for _, value := range dest {
			canonical, err := canonicalValue(value)
			if err != nil {
				return nil, 0, err
			}
			values = append(values, canonical)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return values, len(columns), nil
}

// TestDuckDBDeepDifferential 对 stable.slt 之外的能力做端到端差分：
// 两侧执行同一 SQL，比较列数、行数和规范化值；错误用例要求两侧都失败。
//
// 参考引擎固定为 compatibility/deviations.yaml 的版本，线程数固定为 1，
// 以便采样等顺序敏感行为可复现。
func TestDuckDBDeepDifferential(t *testing.T) {
	if os.Getenv("FSSQL_REQUIRE_DUCKDB") != "1" {
		t.Skip("set FSSQL_REQUIRE_DUCKDB=1 in the acceptance gate")
	}
	registry := loadDeviationRegistry(t)
	resetDiffEvidence(t)
	cases := deepSQLCases()
	request := duckDBRequest{Queries: make([]string, len(cases)), Threads: 1}
	for i := range cases {
		request.Queries[i] = strings.ReplaceAll(cases[i].sql, "{batch}", unionFixtureSQL(batchFixtureRows))
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
	if reference.DuckDBVersion != registry.Reference.Version {
		t.Fatalf("DuckDB runner version=%q registry=%q", reference.DuckDBVersion, registry.Reference.Version)
	}
	if len(reference.Results) != len(cases) {
		t.Fatalf("DuckDB results=%d want=%d", len(reference.Results), len(cases))
	}

	// SampleCompatibility=duckdb 让 reservoir/系统采样按参考引擎的顺序语义执行。
	db, err := gosql.OpenSQLDB(gosql.Config{
		MemoryLimitBytes:       64 << 20,
		TempLimitBytes:         128 << 20,
		SpillMemoryBudgetBytes: 2 << 20,
		SampleCompatibility:    "duckdb",
		TableFunctions:         []gosql.TableFunction{batchRowsTableFunction()},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i, testCase := range cases {
		testCase := testCase
		t.Run(testCase.id, func(t *testing.T) {
			oursSQL := strings.ReplaceAll(testCase.sql, "{batch}", "batch_rows()")
			ours, ourColumns, ourErr := queryValuesWithColumns(context.Background(), db, oursSQL)
			duck := reference.Results[i]
			evidence := diffEvidence{
				Suite:         "deep-sql",
				Case:          testCase.id,
				SQL:           oursSQL,
				FSSQLValues:   ours,
				DuckDBValues:  duck.Values,
				FSSQLColumns:  ourColumns,
				DuckDBColumns: duck.Columns,
				FSSQLError:    errorText(ourErr),
				DuckDBError:   duck.Error,
			}
			if testCase.expectError {
				evidence.Match = ourErr != nil && duck.Error != ""
				recordDiffEvidence(t, evidence)
				if !evidence.Match {
					t.Fatalf("错误用例结果不一致：fssql error=%v values=%v；DuckDB error=%q values=%v", ourErr, ours, duck.Error, duck.Values)
				}
				if os.Getenv("FSSQL_DIFF_VERBOSE") == "1" {
					t.Logf("%s\n  fssql  error = %v\n  duckdb error = %s", testCase.id, ourErr, duck.Error)
				}
				return
			}
			evidence.Match = ourErr == nil && duck.Error == "" && ourColumns == duck.Columns && reflect.DeepEqual(ours, duck.Values)
			recordDiffEvidence(t, evidence)
			if !evidence.Match {
				t.Fatalf("值不一致：\nfssql  columns=%d values=%v err=%v\nDuckDB columns=%d values=%v err=%s\nSQL: %s",
					ourColumns, ours, ourErr, duck.Columns, duck.Values, duck.Error, oursSQL)
			}
			logDiffValues(t, testCase.id, ours, duck.Values)
		})
	}
}

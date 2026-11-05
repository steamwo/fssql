package gosql

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
)

func TestNumericCompletionAndNoFromAggregate(t *testing.T) {
	for _, budget := range []int64{1 << 20, 1} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			db, err := OpenSQLDB(Config{TempDir: t.TempDir(), ManifestDir: t.TempDir(), SpillMemoryBudgetBytes: budget})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			cases := []struct {
				sql  string
				want []any
			}{
				{"SELECT count(*), sum(1), avg(1), min(1), max(1)", []any{int64(1), "1", float64(1), int64(1), int64(1)}},
				{"SELECT count(*), sum(1), avg(1), min(1), max(1) WHERE FALSE", []any{int64(0), nil, nil, nil, nil}},
				{"SELECT count(*) HAVING count(*) = 1", []any{int64(1)}},
				{"SELECT sum(1) + (SELECT 3)", []any{"4"}},
				{"SELECT (SELECT count(*))", []any{int64(1)}},
				{"SELECT CAST(1 AS TINYINT) + CAST(2 AS TINYINT), CAST(-7 AS SMALLINT) % CAST(3 AS SMALLINT)", []any{int64(3), int64(-1)}},
				{"SELECT -CAST(7 AS SMALLINT), +CAST(7 AS TINYINT)", []any{int64(-7), int64(7)}},
				{"SELECT CAST(9 AS TINYINT) / CAST(2 AS TINYINT)", []any{float64(4.5)}},
				{"SELECT CAST('1.5' AS FLOAT) + 2, -CAST('1.5' AS FLOAT), CAST('1.5' AS FLOAT) < CAST('2.0' AS FLOAT)", []any{float64(3.5), float64(-1.5), true}},
				{"SELECT count(*) WHERE FALSE", []any{int64(0)}},
				{"SELECT 1 HAVING TRUE", []any{int64(1)}},
				{"SELECT CAST('9223372036854775808' AS HUGEINT) + 1, CAST('9223372036854775808' AS HUGEINT) - 1, CAST('9223372036854775808' AS HUGEINT) * 2", []any{"9223372036854775809", "9223372036854775807", "18446744073709551616"}},
				{"SELECT CAST('-9223372036854775809' AS HUGEINT) % 3, -CAST('9223372036854775808' AS HUGEINT)", []any{"0", "-9223372036854775808"}},
				{"SELECT CAST('9223372036854775808' AS HUGEINT) > 1, CAST('9223372036854775808' AS HUGEINT) / 2", []any{true, float64(4611686018427387904)}},
				{"SELECT CAST(NULL AS TINYINT) + CAST(2 AS TINYINT), CAST(NULL AS HUGEINT) * 3", []any{nil, nil}},
				{"SELECT CAST(250 AS UTINYINT) + CAST(5 AS UTINYINT), CAST(9 AS USMALLINT) * CAST(2 AS USMALLINT)", []any{int64(255), int64(18)}},
				{"SELECT -CAST(1 AS UTINYINT), -CAST(1 AS UBIGINT)", []any{int64(255), "18446744073709551615"}},
				{"SELECT CAST(1 AS UTINYINT) + CAST(2 AS TINYINT), CAST('18446744073709551615' AS UBIGINT) + CAST(-1 AS BIGINT)", []any{int64(3), "18446744073709551614"}},
				{"SELECT CAST('4294967295' AS UINTEGER) + CAST(1 AS INTEGER), CAST('4294967295' AS UINTEGER) / 1", []any{int64(4294967296), float64(4294967295)}},
				{"SELECT CAST('18446744073709551615' AS UBIGINT) > CAST(-1 AS BIGINT), CAST('18446744073709551615' AS UBIGINT) + CAST(0 AS UBIGINT)", []any{true, "18446744073709551615"}},
				{"SELECT CAST(CAST('12.50' AS DECIMAL(10,2)) AS BIGINT), CAST(CAST('-12.50' AS DECIMAL(10,2)) AS BIGINT), CAST(CAST('-0.40' AS DECIMAL(10,2)) AS UBIGINT)", []any{int64(13), int64(-13), "0"}},
				{"SELECT CAST(CAST('18446744073709551615.00' AS DECIMAL(38,2)) AS UBIGINT), CAST(CAST('12.34' AS DECIMAL(10,2)) AS FLOAT)", []any{"18446744073709551615", float64(float32(12.34))}},
				{"SELECT CAST(CAST(NULL AS DECIMAL(10,2)) AS BIGINT), CAST(CAST('127.49' AS DECIMAL(10,2)) AS TINYINT)", []any{nil, int64(127)}},
			}
			for _, tc := range cases {
				t.Run(tc.sql, func(t *testing.T) {
					got := make([]any, len(tc.want))
					dest := make([]any, len(tc.want))
					for i := range dest {
						dest[i] = &got[i]
					}
					if err := db.QueryRowContext(context.Background(), tc.sql).Scan(dest...); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(got, tc.want) {
						t.Fatalf("got %#v want %#v", got, tc.want)
					}
				})
			}
			for _, sql := range []string{
				"SELECT CAST(127 AS TINYINT) + CAST(1 AS TINYINT)",
				"SELECT CAST(-32768 AS SMALLINT) - CAST(1 AS SMALLINT)",
				"SELECT -CAST(-128 AS TINYINT)",
				"SELECT CAST(255 AS UTINYINT) + CAST(1 AS UTINYINT)",
				"SELECT CAST(0 AS UINTEGER) - CAST(1 AS UINTEGER)",
				"SELECT CAST('18446744073709551615' AS UBIGINT) * CAST(2 AS UBIGINT)",
				"SELECT CAST('170141183460469231731687303715884105727' AS HUGEINT) + 1",
				"SELECT CAST('-170141183460469231731687303715884105728' AS HUGEINT) - 1",
				"SELECT CAST('170141183460469231731687303715884105727' AS HUGEINT) * 2",
				"SELECT -CAST('-170141183460469231731687303715884105728' AS HUGEINT)",
				"SELECT CAST(1 AS HUGEINT) % 0",
				"SELECT CAST(-128 AS TINYINT) % CAST(-1 AS TINYINT)",
				"SELECT CAST(-32768 AS SMALLINT) % CAST(-1 AS SMALLINT)",
				"SELECT CAST('-2147483648' AS INTEGER) % CAST(-1 AS INTEGER)",
				"SELECT CAST('-170141183460469231731687303715884105728' AS HUGEINT) % -1",
				"SELECT CAST(CAST('127.50' AS DECIMAL(10,2)) AS TINYINT)",
				"SELECT CAST(CAST('-0.50' AS DECIMAL(10,2)) AS UBIGINT)",
				"SELECT CAST(CAST('9223372036854775807.50' AS DECIMAL(38,2)) AS BIGINT)",
			} {
				var value any
				if err := db.QueryRowContext(context.Background(), sql).Scan(&value); err == nil {
					t.Errorf("expected overflow/domain error: %s", sql)
				}
			}
		})
	}
}

func TestNumericWideningEvaluatesVolatileChildOnce(t *testing.T) {
	var calls atomic.Int64
	fn := ScalarFunction{Name: "numeric_once", Result: Type{ID: "INTEGER"}, Eval: func([]ScalarValue) (ScalarValue, error) {
		return ScalarValue{Type: Type{ID: "INTEGER"}, Valid: true, Int64: calls.Add(1)}, nil
	}}
	db, err := OpenSQLDB(Config{ScalarFunctions: []ScalarFunction{fn}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var value string
	if err := db.QueryRowContext(context.Background(), "SELECT numeric_once() + CAST(1 AS HUGEINT)").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "2" || calls.Load() != 1 {
		t.Fatalf("value=%q calls=%d", value, calls.Load())
	}
}

func TestExactIntegerExpressionsAcrossRows(t *testing.T) {
	db := phase5DB(t, 1<<20)
	var got [][3]string
	_, err := db.QueryChunks(context.Background(), "SELECT CAST(id AS TINYINT) * CAST(2 AS TINYINT), CAST(id AS HUGEINT) + CAST('9223372036854775808' AS HUGEINT), CAST(id AS UTINYINT) + CAST(1 AS UTINYINT) FROM phase5_rows() ORDER BY id", func(chunk *Chunk) error {
		for row := 0; row < chunk.RowCount(); row++ {
			signed, ok, err := chunk.Int64(row, 0)
			if err != nil || !ok {
				return fmt.Errorf("signed: %v", err)
			}
			wide, ok, err := chunk.Int128(row, 1)
			if err != nil || !ok {
				return fmt.Errorf("wide: %v", err)
			}
			unsigned, ok, err := chunk.Uint64(row, 2)
			if err != nil || !ok {
				return fmt.Errorf("unsigned: %v", err)
			}
			got = append(got, [3]string{fmt.Sprint(signed), wide.String(), fmt.Sprint(unsigned)})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, row := range got {
		id := i + 1
		if row != [3]string{fmt.Sprint(id * 2), fmt.Sprint(uint64(9223372036854775808) + uint64(id)), fmt.Sprint(id + 1)} {
			t.Fatalf("row %d: %v", i, row)
		}
	}
	if len(got) != 5 {
		t.Fatalf("rows=%d", len(got))
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("managed memory leak: %d", used)
	}
	for _, sql := range []string{
		"SELECT CAST(id AS TINYINT) + CAST(127 AS TINYINT) FROM phase5_rows()",
		"SELECT CAST(id AS HUGEINT) + CAST('170141183460469231731687303715884105727' AS HUGEINT) FROM phase5_rows()",
		"SELECT CAST(id AS UTINYINT) * CAST(255 AS UTINYINT) FROM phase5_rows()",
	} {
		if _, err := db.QueryChunks(context.Background(), sql, func(*Chunk) error { return nil }); err == nil {
			t.Fatalf("expected arithmetic overflow: %s", sql)
		}
		if used := db.MemoryUsed(); used != 0 {
			t.Fatalf("error path managed memory leak: %d", used)
		}
	}
}

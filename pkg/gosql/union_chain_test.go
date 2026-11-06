package gosql

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

// TestUnionAllLongChain 验证长链重关联后的分支顺序、LIMIT、排序、失败回收和物化所有权。
func TestUnionAllLongChain(t *testing.T) {
	db, err := Open(Config{MemoryLimitBytes: 64 << 20, TempDir: t.TempDir(), ManifestDir: t.TempDir(), Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const terms = 4097
	query := unionChainSQL(terms)
	for _, tc := range []struct {
		name   string // name 标识长链消费方式。
		query  string // query 是待执行的完整 SQL。
		rows   int    // rows 是预期结果行数。
		first  int64  // first 是首行值。
		stride int64  // stride 是后续行值的步长。
	}{
		{name: "all", query: query, rows: terms, first: 0, stride: 1},
		{name: "limit", query: query + " LIMIT 3", rows: 3, first: 0, stride: 1},
		{name: "ordered", query: query + " ORDER BY value DESC LIMIT 3 OFFSET 1", rows: 3, first: terms - 2, stride: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := 0
			_, err := db.QueryChunks(context.Background(), tc.query, func(chunk *Chunk) error {
				for row := 0; row < chunk.RowCount(); row++ {
					got, valid, err := chunk.Int64(row, 0)
					want := tc.first + int64(rows)*tc.stride
					if err != nil || !valid || got != want {
						return fmt.Errorf("row %d=%d valid=%v err=%v want %d", rows, got, valid, err, want)
					}
					rows++
				}
				return nil
			})
			if err != nil || rows != tc.rows {
				t.Fatalf("rows=%d want %d err=%v", rows, tc.rows, err)
			}
		})
	}
	failure := errors.New("stop union callback")
	_, err = db.QueryChunks(context.Background(), query, func(*Chunk) error { return failure })
	if !errors.Is(err, failure) {
		t.Fatalf("callback error=%v want %v", err, failure)
	}
	materialized, err := db.Materialize(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	defer materialized.Close()
	rows := 0
	_, err = db.QueryChunks(context.Background(), materialized.SQL(), func(chunk *Chunk) error {
		rows += chunk.RowCount()
		return nil
	})
	if err != nil || rows != terms {
		t.Fatalf("materialized rows=%d want %d err=%v", rows, terms, err)
	}
	if err := materialized.Close(); err != nil {
		t.Fatal(err)
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("managed memory leak: %d", used)
	}
}

// TestUnionAllReassociationBoundaries 验证逐层 CAST、NULL、派生表修饰符和共享 CTE 不被穿透。
func TestUnionAllReassociationBoundaries(t *testing.T) {
	db, err := OpenSQLDB(Config{TempDir: t.TempDir(), ManifestDir: t.TempDir(), Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, tc := range []struct {
		query string // query 覆盖不可跨越的语义边界。
		want  []any  // want 是逐行期望值，nil 表示 NULL。
	}{
		{query: "SELECT CAST(16777217 AS BIGINT) AS value UNION ALL SELECT CAST(1 AS FLOAT) UNION ALL SELECT CAST(2 AS DOUBLE)", want: []any{float64(16777216), float64(1), float64(2)}},
		{query: "SELECT NULL AS value UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 2", want: []any{nil, int64(1), int64(1), int64(2)}},
		{query: "SELECT value FROM (SELECT 3 AS value UNION ALL SELECT 1 ORDER BY value LIMIT 1) t UNION ALL SELECT 2 UNION ALL SELECT 4", want: []any{int64(1), int64(2), int64(4)}},
		{query: "SELECT value FROM (SELECT 3 AS value UNION ALL SELECT 1) t WHERE value > 1 UNION ALL SELECT 2 UNION ALL SELECT 4", want: []any{int64(3), int64(2), int64(4)}},
		{query: "SELECT value FROM (SELECT DISTINCT value FROM (SELECT 1 AS value UNION ALL SELECT 1) t) d UNION ALL SELECT 2 UNION ALL SELECT 3", want: []any{int64(1), int64(2), int64(3)}},
		{query: "WITH t AS (SELECT 1 AS value UNION ALL SELECT 2) SELECT value FROM t UNION ALL SELECT value FROM t UNION ALL SELECT 3 ORDER BY value", want: []any{int64(1), int64(1), int64(2), int64(2), int64(3)}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			rows, err := db.QueryContext(context.Background(), tc.query)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var got []any
			for rows.Next() {
				var value any
				if err := rows.Scan(&value); err != nil {
					t.Fatal(err)
				}
				got = append(got, value)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %#v want %#v", got, tc.want)
			}
		})
	}
	// 当前执行路径仍会求值 LIMIT 之外的分支；重关联不能静默屏蔽已有的转换错误。
	rows, err := db.QueryContext(context.Background(), "SELECT 1 AS value UNION ALL SELECT 2 UNION ALL SELECT CAST('bad' AS INTEGER) LIMIT 2")
	if err == nil {
		for rows.Next() {
		}
		err = rows.Err()
		_ = rows.Close()
	}
	if err == nil {
		t.Fatal("expected invalid CAST error after LIMIT")
	}
}

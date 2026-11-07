package gosql

import (
	"context"
	"crypto/md5"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
)

// TestMigrationWindowExpressionKeys 验证计算分区键和哈希排序键在内存及 spill 路径中的排名
func TestMigrationWindowExpressionKeys(t *testing.T) {
	for _, budget := range []int64{1 << 20, 1} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			db := groupedRowsDB(t, budget)
			var got [][3]int64
			_, err := db.QueryChunks(context.Background(), `SELECT id,
ROW_NUMBER() OVER (PARTITION BY CASE WHEN grp=1 THEN 0 ELSE 1 END ORDER BY LOWER(MD5(CAST(id AS VARCHAR))),id DESC NULLS LAST),
ROW_NUMBER() OVER (ORDER BY -id) FROM grouped_rows() ORDER BY id`, func(c *Chunk) error {
				for row := 0; row < c.RowCount(); row++ {
					var values [3]int64
					for col := range values {
						v, ok, err := c.Int64(row, col)
						if err != nil || !ok {
							return fmt.Errorf("row=%d col=%d valid=%v err=%v", row, col, ok, err)
						}
						values[col] = v
					}
					got = append(got, values)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			ranks := map[int64]int64{}
			for _, ids := range [][]int64{{1, 2, 3}, {4, 5}} {
				sort.Slice(ids, func(i, j int) bool {
					a := md5.Sum([]byte(strconv.FormatInt(ids[i], 10)))
					b := md5.Sum([]byte(strconv.FormatInt(ids[j], 10)))
					return fmt.Sprintf("%x", a) < fmt.Sprintf("%x", b)
				})
				for i, id := range ids {
					ranks[id] = int64(i + 1)
				}
			}
			want := make([][3]int64, 5)
			for i := range want {
				id := int64(i + 1)
				want[i] = [3]int64{id, ranks[id], 6 - id}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("rows=%v want=%v", got, want)
			}
			if used := db.MemoryUsed(); used != 0 {
				t.Fatalf("memory leak=%d", used)
			}
		})
	}
}

// TestMigrationQualifiedStar 验证 CTE 星号与 JOIN 中的限定星号只展开对应关系
func TestMigrationQualifiedStar(t *testing.T) {
	db := groupedRowsDB(t, 1<<20)
	var got [][3]int64
	cols, err := db.QueryChunks(context.Background(), `WITH s AS (SELECT grp,id FROM grouped_rows()) SELECT s.*,COUNT(*) OVER () FROM s ORDER BY id`, func(c *Chunk) error {
		for row := 0; row < c.RowCount(); row++ {
			var values [3]int64
			for col := range values {
				v, ok, err := c.Int64(row, col)
				if err != nil || !ok {
					return fmt.Errorf("row=%d col=%d valid=%v err=%v", row, col, ok, err)
				}
				values[col] = v
			}
			got = append(got, values)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := [][3]int64{{1, 1, 5}, {1, 2, 5}, {1, 3, 5}, {2, 4, 5}, {2, 5, 5}}
	if len(cols) != 3 || cols[0].Name != "grp" || cols[1].Name != "id" || !reflect.DeepEqual(got, want) {
		t.Fatalf("cols=%v rows=%v", cols, got)
	}
	for queryIndex, query := range []string{
		`SELECT a.*,b.* FROM grouped_rows() a JOIN grouped_rows() b ON a.id=b.id ORDER BY a.id`,
		`SELECT a.*,b.* FROM (SELECT id,grp FROM grouped_rows()) a JOIN (SELECT id,grp FROM grouped_rows()) b USING(id) ORDER BY a.id`,
	} {
		rows := 0
		columns, queryErr := db.QueryChunks(context.Background(), query, func(c *Chunk) error {
			if c.ColumnCount() != 4 {
				return fmt.Errorf("columns=%d", c.ColumnCount())
			}
			for row := 0; row < c.RowCount(); row++ {
				for col := 0; col < 2; col++ {
					a, ok, e := c.Int64(row, col)
					if e != nil || !ok {
						return fmt.Errorf("left valid=%v err=%v", ok, e)
					}
					b, ok, e := c.Int64(row, col+2)
					if e != nil || !ok || a != b {
						return fmt.Errorf("pair=%d/%d valid=%v err=%v", a, b, ok, e)
					}
				}
				rows++
			}
			return nil
		})
		if queryErr != nil || rows != 5 {
			t.Fatalf("query=%s rows=%d err=%v", query, rows, queryErr)
		}
		wantNames := []string{"grp", "id", "grp", "id"}
		if queryIndex == 1 {
			wantNames = []string{"id", "grp", "id", "grp"}
		}
		for i, column := range columns {
			if column.Name != wantNames[i] {
				t.Fatalf("column %d=%s want=%s", i, column.Name, wantNames[i])
			}
		}
	}
	if _, err = db.QueryChunks(context.Background(), `SELECT missing.* FROM grouped_rows()`, func(*Chunk) error { return nil }); err == nil {
		t.Fatal("unknown qualifier accepted")
	}
}

// TestMigrationCSVDefaultNullDate 验证 CSV 默认空字段保持 NULL 并可用于日期截断
func TestMigrationCSVDefaultNullDate(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "dates.csv")
	if err := os.WriteFile(path, []byte("id,dt,note\n1,2026-10-09 13:24:56,x\n2,,\n3,\"\",\"\"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	uri, err := FileURI(path)
	if err != nil {
		t.Fatal(err)
	}
	db, err := OpenSQLDB(Config{LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, reader := range []string{"read_csv_auto", "read_csv"} {
		rows, err := db.Query("SELECT id,CAST(DATE_TRUNC('month',CAST(dt AS TIMESTAMP)) AS VARCHAR),note IS NULL FROM " + reader + "('" + uri + "',header=true) ORDER BY id")
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for rows.Next() {
			var id int64
			var date sql.NullString
			var noteNull bool
			if err := rows.Scan(&id, &date, &noteNull); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			if id == 1 {
				if !date.Valid || date.String != "2026-10-01 00:00:00" || noteNull {
					rows.Close()
					t.Fatalf("date=%+v noteNull=%v", date, noteNull)
				}
			} else if date.Valid || !noteNull {
				rows.Close()
				t.Fatalf("id=%d date=%+v noteNull=%v", id, date, noteNull)
			}
			count++
		}
		err = rows.Err()
		rows.Close()
		if err != nil || count != 3 {
			t.Fatalf("reader=%s rows=%d err=%v", reader, count, err)
		}
	}
	rows, err := db.Query("SELECT id,dt FROM read_csv('" + uri + "',header=true,auto_detect=false,allow_quoted_nulls=false,columns={'id':'BIGINT','dt':'VARCHAR','note':'VARCHAR'}) ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var id int64
		var value sql.NullString
		if err := rows.Scan(&id, &value); err != nil {
			t.Fatal(err)
		}
		if id == 2 && value.Valid || id == 3 && (!value.Valid || value.String != "") {
			t.Fatalf("id=%d value=%+v", id, value)
		}
		count++
	}
	if err := rows.Err(); err != nil || count != 3 {
		t.Fatalf("rows=%d err=%v", count, err)
	}
}

// TestMigrationCSVQuotedNullInferenceMatchesExecution 验证自动推断与执行共用引号 NULL 语义。
func TestMigrationCSVQuotedNullInferenceMatchesExecution(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "quoted-empty.csv")
	if err := os.WriteFile(path, []byte("v\r\n1\r\n\"\""), 0600); err != nil {
		t.Fatal(err)
	}
	uri, err := FileURI(path)
	if err != nil {
		t.Fatal(err)
	}
	db, err := OpenSQLDB(Config{LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tests := []struct {
		name     string
		options  string
		wantType string
		want     []sql.NullString
	}{
		{
			name:     "auto_disallow_quoted_nulls",
			options:  "header=true,allow_quoted_nulls=false",
			wantType: "VARCHAR",
			want:     []sql.NullString{{String: "1", Valid: true}, {String: "", Valid: true}},
		},
		{
			name:     "explicit_disallow_quoted_nulls",
			options:  "header=true,auto_detect=false,allow_quoted_nulls=false,columns={'v':'VARCHAR'}",
			wantType: "VARCHAR",
			want:     []sql.NullString{{String: "1", Valid: true}, {String: "", Valid: true}},
		},
		{
			name:     "default_allow_quoted_nulls",
			options:  "header=true",
			wantType: "BIGINT",
			want:     []sql.NullString{{String: "1", Valid: true}, {}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := db.Query("SELECT v FROM read_csv('" + uri + "'," + tc.options + ")")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			columnTypes, err := rows.ColumnTypes()
			if err != nil {
				t.Fatal(err)
			}
			if len(columnTypes) != 1 || columnTypes[0].DatabaseTypeName() != tc.wantType {
				t.Fatalf("column types=%+v want=%s", columnTypes, tc.wantType)
			}
			var got []sql.NullString
			for rows.Next() {
				var value sql.NullString
				if err := rows.Scan(&value); err != nil {
					t.Fatal(err)
				}
				got = append(got, value)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("values=%+v want=%+v", got, tc.want)
			}
		})
	}
}

// TestMigrationWindowPartitionEvaluatesOnce 验证含 NULL 的分区计算在排序与窗口执行之间只求值一次
func TestMigrationWindowPartitionEvaluatesOnce(t *testing.T) {
	var calls atomic.Int64
	fn := ScalarFunction{
		Name:         "partition_key",
		Arguments:    []Type{{ID: "BIGINT"}},
		Result:       Type{ID: "BIGINT"},
		NullHandling: ScalarNullPropagate,
		Eval: func(args []ScalarValue) (ScalarValue, error) {
			calls.Add(1)
			id := args[0].Int64
			return ScalarValue{Type: Type{ID: "BIGINT"}, Valid: id < 4, Int64: id % 2}, nil
		},
	}
	db, err := Open(Config{TableFunctions: []TableFunction{groupedRowsFunction()}, ScalarFunctions: []ScalarFunction{fn}, TempDir: t.TempDir(), ManifestDir: t.TempDir(), SpillMemoryBudgetBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	got := collectInt64Column(t, db, "SELECT ROW_NUMBER() OVER (PARTITION BY partition_key(id) ORDER BY -id) FROM grouped_rows() ORDER BY id", 0)
	if !reflect.DeepEqual(got, []int64{2, 1, 1, 2, 1}) || calls.Load() != 5 {
		t.Fatalf("ranks=%v calls=%d", got, calls.Load())
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("memory leak=%d", used)
	}
}

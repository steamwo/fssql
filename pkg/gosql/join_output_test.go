package gosql

import (
	"context"
	"io"
	"reflect"
	"testing"
)

func TestJoinRequiredOutputKeepsResidualSortAndAggregateInputs(t *testing.T) {
	db := groupedRowsDB(t, 1<<20)
	for _, test := range []struct {
		sql  string
		want []int64
	}{
		{"SELECT COUNT(*) FROM grouped_rows() a JOIN grouped_rows() b ON a.grp=b.grp", []int64{13}},
		{"SELECT COUNT(b.id) FROM grouped_rows() a JOIN grouped_rows() b ON a.grp=b.grp WHERE b.id<3", []int64{6}},
		{"SELECT a.id FROM grouped_rows() a JOIN grouped_rows() b ON a.grp=b.grp WHERE b.id=2 ORDER BY a.id", []int64{1, 2, 3}},
		{"SELECT a.id FROM grouped_rows() a JOIN grouped_rows() b ON a.grp=b.grp WHERE a.grp=1 AND b.id<3 ORDER BY b.id DESC,a.id", []int64{1, 2, 3, 1, 2, 3}},
	} {
		got := collectInt64Column(t, db, test.sql, 0)
		if !reflect.DeepEqual(got, test.want) {
			t.Fatalf("%s: rows=%v want=%v", test.sql, got, test.want)
		}
		if db.MemoryUsed() != 0 {
			t.Fatalf("managed memory leak=%d", db.MemoryUsed())
		}
	}
}

func TestJoinLimitStopsProbeAndReleasesSpill(t *testing.T) {
	for _, tc := range []struct {
		name, join, tail string
		buildRows        string
		budget           int64
		wantReads        int
		want             []int64
	}{
		{name: "inner", join: "JOIN", tail: "LIMIT 3 OFFSET 2", buildRows: "64", wantReads: 1, want: []int64{1, 1, 1}},
		{name: "full", join: "FULL JOIN", tail: "LIMIT 3", buildRows: "64", wantReads: 1, want: []int64{1, 1, 1}},
		{name: "ordered", join: "JOIN", tail: "ORDER BY a.batch DESC LIMIT 3", buildRows: "64", wantReads: 4, want: []int64{4, 4, 4}},
		{name: "zero with spill", join: "LEFT JOIN", tail: "LIMIT 0", buildRows: "8192", budget: 32 << 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reads, closes := 0, 0
			bigint := Type{ID: "BIGINT"}
			probe := TableFunction{
				Name:    "join_limit_probe",
				Columns: []Column{{Name: "key", Type: bigint}, {Name: "batch", Type: bigint}},
				Open: func(context.Context) (BatchReader, error) {
					return &batchReaderFunc{
						next: func(_ context.Context, w BatchWriter) (int, error) {
							reads++
							for i := 0; i < 64; i++ {
								if err := w.SetInt64(i, 0, 7); err != nil {
									return 0, err
								}
								if err := w.SetInt64(i, 1, int64(reads)); err != nil {
									return 0, err
								}
							}
							if reads == 4 {
								return 64, io.EOF
							}
							return 64, nil
						},
						close: func() error { closes++; return nil },
					}, nil
				},
			}
			db, err := Open(Config{
				Workers: 1, MemoryLimitBytes: 64 << 20, SpillMemoryBudgetBytes: tc.budget,
				TempDir: t.TempDir(), ManifestDir: t.TempDir(), TableFunctions: []TableFunction{probe},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			sql := "SELECT a.batch FROM join_limit_probe() a " + tc.join +
				" range(" + tc.buildRows + ") AS b(id) ON a.key=b.id " + tc.tail
			if got := collectInt64Column(t, db, sql, 0); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("rows=%v want=%v", got, tc.want)
			}
			wantCloses := 0
			if tc.wantReads > 0 {
				wantCloses = 1
			}
			if reads != tc.wantReads || closes != wantCloses {
				t.Fatalf("probe reads/closes=%d/%d want=%d/%d", reads, closes, tc.wantReads, wantCloses)
			}
			if tc.budget != 0 && db.temporary.Peak() == 0 {
				t.Fatal("the low-budget join did not spill")
			}
			if mem, temp := db.MemoryUsed(), db.temporary.Used(); mem != 0 || temp != 0 {
				t.Fatalf("resources retained after join, memory=%d temp=%d", mem, temp)
			}
		})
	}
}

func TestSQLTopNComposedOperators(t *testing.T) {
	db := groupedRowsDB(t, 1<<20)
	for _, tc := range []struct {
		name, sql string
		want      []int64
	}{
		{"outer join with hidden nullable key", "SELECT a.id FROM range(8) a(id) LEFT JOIN range(3) b(id) ON a.id=b.id ORDER BY b.id DESC NULLS LAST,a.id DESC LIMIT 4 OFFSET 1", []int64{1, 0, 7, 6}},
		{"join with independent inner order", "SELECT a.id FROM range(12) a(id) JOIN (SELECT id FROM range(12) t(id) ORDER BY id LIMIT 6) b ON a.id=b.id ORDER BY b.id DESC LIMIT 2 OFFSET 1", []int64{4, 3}},
		{"hidden aggregate order", "SELECT grp FROM grouped_rows() GROUP BY grp ORDER BY COUNT(*) DESC LIMIT 1", []int64{1}},
		{"distinct order", "SELECT DISTINCT id%3 AS k FROM range(15) t(id) ORDER BY k DESC LIMIT 2 OFFSET 1", []int64{1, 0}},
		{"union branch orders", "SELECT id FROM (SELECT id FROM range(10) t(id) ORDER BY id DESC LIMIT 4) a UNION ALL SELECT id FROM (SELECT id FROM range(10,20) t(id) ORDER BY id DESC LIMIT 4) b ORDER BY id DESC LIMIT 3 OFFSET 2", []int64{17, 16, 9}},
		{"filter after sort", "SELECT id FROM (SELECT id FROM range(20) t(id) ORDER BY id DESC) a WHERE id<5 LIMIT 2", []int64{4, 3}},
		{"hidden string key", "SELECT id FROM range(8) t(id) ORDER BY CASE WHEN id%2=0 THEN 'a' ELSE 'z' END DESC,id DESC LIMIT 2", []int64{7, 5}},
		{"huge limit", "SELECT id FROM range(3) t(id) ORDER BY id DESC LIMIT 9223372036854775807", []int64{2, 1, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := collectInt64Column(t, db, tc.sql, 0); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("rows=%v want=%v", got, tc.want)
			}
			if mem, temp := db.MemoryUsed(), db.temporary.Used(); mem != 0 || temp != 0 {
				t.Fatalf("resources retained after TopN, memory=%d temp=%d", mem, temp)
			}
		})
	}
}

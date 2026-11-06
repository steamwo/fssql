package gosql

import (
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

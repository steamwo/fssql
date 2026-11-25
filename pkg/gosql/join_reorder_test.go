package gosql

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestFileJoinReorderPreservesBoundColumns(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	db, err := OpenSQLDB(Config{LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	csv := func(file, data, schema string) string {
		t.Helper()
		path := filepath.Join(root, file+".csv")
		if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
		uri, err := FileURI(path)
		if err != nil {
			t.Fatal(err)
		}
		return "read_csv('" + strings.ReplaceAll(uri, "'", "''") + "', header=true, columns='" + schema + "') " + file
	}
	airlines := csv("l", "code,name\nA,Alpha\nB,Beta\n", "code VARCHAR, name VARCHAR")
	routes := csv("r", "carrier,origin,zone,route\nA,X,1,first\nA,X,2,wrong_zone\nB,X,1,second\nA,Y,1,wrong_country\n,X,1,null_carrier\n"+strings.Repeat("B,Y,1,unselected\n", 80),
		"carrier VARCHAR, origin VARCHAR, zone VARCHAR, route VARCHAR")
	airports := csv("a", "airport,area,country\nX,1,China\nY,1,Japan\n", "airport VARCHAR, area VARCHAR, country VARCHAR")
	join := airlines + " JOIN " + routes + " ON l.code=r.carrier JOIN " + airports + " ON r.origin=a.airport AND r.zone=a.area"
	cross := airlines + " CROSS JOIN " + routes + " CROSS JOIN " + airports
	for _, tc := range []struct{ name, fromWhere string }{
		{"inner", join + " WHERE a.country='China'"},
		{"cross_with_keys", cross + " WHERE l.code=r.carrier AND r.origin=a.airport AND r.zone=a.area AND a.country='China'"},
		{"residual", join + " WHERE a.country='China' OR r.route='absent'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := db.QueryContext(ctx, "SELECT * FROM "+tc.fromWhere+" ORDER BY r.route")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			columns, err := rows.Columns()
			wantCols := []string{"code", "name", "carrier", "origin", "zone", "route", "airport", "area", "country"}
			if err != nil || !reflect.DeepEqual(columns, wantCols) {
				t.Fatalf("columns=%v, err=%v", columns, err)
			}
			var got [][]string
			for rows.Next() {
				values := make([]string, len(columns))
				dest := make([]any, len(values))
				for i := range values {
					dest[i] = &values[i]
				}
				if err := rows.Scan(dest...); err != nil {
					t.Fatal(err)
				}
				got = append(got, values)
			}
			want := [][]string{
				{"A", "Alpha", "A", "X", "1", "first", "X", "1", "China"},
				{"B", "Beta", "B", "X", "1", "second", "X", "1", "China"},
			}
			if err := rows.Err(); err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("rows=%v, err=%v", got, err)
			}
		})
	}
}

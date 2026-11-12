package compat_test

import (
	"crypto/sha256"
	"fmt"
	"github.com/steamwo/fssql/pkg/gosql"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

type typedWorkloadFixtures struct {
	root  string
	files map[string]string
	hash  string
}

func newTypedWorkloadFixtures(t *testing.T, cases []workloadCase) *typedWorkloadFixtures {
	t.Helper()
	f := &typedWorkloadFixtures{}
	needed := false
	for _, c := range cases {
		needed = needed || strings.Contains(c.SQL, "{csv_")
	}
	if !needed {
		return f
	}
	f.root = t.TempDir()
	f.files = map[string]string{"csv_dates": "d,ts\n31/01/2024,31/01/2024 12:13:14\n01/02/2024,01/02/2024 01:02:03\n", "csv_numbers": "n\n1\n2\n", "csv_mixed": "d\n31/01/2024\n2024-02-01\n", "csv_decimal": "n\n1.250\n-2.500\n"}
	names := []string{}
	for name := range f.files {
		names = append(names, name)
	}
	sort.Strings(names)
	hash := sha256.New()
	for _, name := range names {
		data := f.files[name]
		if err := os.WriteFile(filepath.Join(f.root, name+".csv"), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(hash, "%s\x00%s\x00", name, data)
	}
	f.hash = fmt.Sprintf("%x", hash.Sum(nil))
	return f
}
func (f *typedWorkloadFixtures) localConfig() *gosql.LocalFileConfig {
	if f.root == "" {
		return nil
	}
	return &gosql.LocalFileConfig{RootDir: f.root}
}
func (f *typedWorkloadFixtures) sha256() string { return f.hash }
func (f *typedWorkloadFixtures) expand(sql string, reference bool) string {
	for name := range f.files {
		path := filepath.Join(f.root, name+".csv")
		if !reference {
			uri, err := gosql.FileURI(path)
			if err != nil {
				panic(err)
			}
			path = uri
		} else {
			path = filepath.ToSlash(path)
		}
		sql = strings.ReplaceAll(sql, "{"+name+"}", strings.ReplaceAll(path, "'", "''"))
	}
	return sql
}

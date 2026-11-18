package gosql

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestS3GlobWithListOnlyPermissions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Query().Get("list-type") != "2" {
			t.Errorf("glob requested an object: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_, _ = io.WriteString(w, `<ListBucketResult><Contents><Key>item.csv</Key><Size>10</Size><ETag>"tag"</ETag></Contents></ListBucketResult>`)
	}))
	defer server.Close()
	db, err := OpenSQLDB(Config{
		TempDir: t.TempDir(),
		S3: &S3Config{
			Endpoint: server.URL, Region: "test", ForcePathStyle: true,
			AllowInsecureHTTP: true, AccessKey: "test", SecretKey: "test",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var file string
	if err := db.QueryRowContext(context.Background(), "SELECT file FROM glob(?)", "s3://bucket/*.csv").Scan(&file); err != nil {
		t.Fatal(err)
	}
	if file != "s3://bucket/item.csv" {
		t.Fatalf("file = %q", file)
	}
}

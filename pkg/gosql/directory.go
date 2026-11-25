package gosql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/steamwo/fssql/internal/tablefunc"
)

const directoryCatalogFile = "fssql.json"

type directoryCatalog struct {
	Tables []struct {
		Name    string                 `json:"name"`
		Format  string                 `json:"format"`
		Pattern string                 `json:"pattern"`
		Options *directoryTableOptions `json:"options"`
	} `json:"tables"`
}

type directoryTableOptions struct {
	Header           *bool   `json:"header"`
	Columns          *string `json:"columns"`
	ValidateSchema   bool    `json:"validate_schema"`
	PartitionColumns *string `json:"partition_columns"`
}

func (opts *directoryTableOptions) source(uri string) string {
	if opts == nil {
		return uri
	}
	values := make(map[string]string, 2)
	if opts.Header != nil {
		values["header"] = strconv.FormatBool(*opts.Header)
	}
	if opts.Columns != nil {
		values["columns"] = *opts.Columns
	}
	if opts.PartitionColumns != nil {
		values["partition_columns"] = *opts.PartitionColumns
	}
	if len(values) == 0 {
		return uri
	}
	return tablefunc.Encode(uri, values)
}

// OpenDirectorySQLDB opens a database/sql handle with external tables
// declared in fssql.json under root. Patterns are relative to root. The local
// data files are read-only, and each open rebuilds the in-memory catalog from
// the file. Leave config.LocalFiles unset because root configures local access.
func OpenDirectorySQLDB(ctx context.Context, root string, config Config) (*sql.DB, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if root == "" || config.LocalFiles != nil {
		return nil, errors.New("gosql: directory root is required and Config.LocalFiles must be unset")
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("gosql: resolve directory root: %w", err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return nil, fmt.Errorf("gosql: resolve directory root: %w", err)
	}
	// The fileset planner treats these characters as glob syntax in the URI.
	if strings.ContainsAny(root, "*?[") {
		return nil, fmt.Errorf("gosql: directory root contains glob metacharacters: %q", root)
	}
	catalog, err := loadDirectoryCatalog(filepath.Join(root, directoryCatalogFile))
	if err != nil {
		return nil, err
	}

	config.LocalFiles = &LocalFileConfig{RootDir: root}
	native, err := open(config, true)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(&sqlConnector{db: native})
	for _, table := range catalog.Tables {
		uri, err := FileURI(filepath.Join(root, filepath.FromSlash(table.Pattern)))
		if err != nil {
			return nil, errors.Join(fmt.Errorf("gosql: resolve directory table %q: %w", table.Name, err), db.Close())
		}
		identifier := `"` + strings.ReplaceAll(table.Name, `"`, `""`) + `"`
		if _, err := db.ExecContext(ctx, "CREATE EXTERNAL TABLE "+identifier+" USING read_"+table.Format+"(?)", table.Options.source(uri)); err != nil {
			return nil, errors.Join(fmt.Errorf("gosql: register directory table %q: %w", table.Name, err), db.Close())
		}
		if table.Options != nil && table.Options.ValidateSchema {
			// SELECT * decodes every declared column. Exec drains the query without materializing its output.
			if _, err := db.ExecContext(ctx, "SELECT * FROM "+identifier); err != nil {
				return nil, errors.Join(fmt.Errorf("gosql: validate schema of directory table %q: %w", table.Name, err), db.Close())
			}
		}
	}
	return db, nil
}

func loadDirectoryCatalog(filename string) (directoryCatalog, error) {
	var catalog directoryCatalog
	info, err := os.Lstat(filename)
	if err != nil {
		return catalog, fmt.Errorf("gosql: read directory catalog: %w", err)
	}
	if !info.Mode().IsRegular() {
		return catalog, fmt.Errorf("gosql: directory catalog must be a regular file: %q", filename)
	}
	file, err := os.Open(filename)
	if err != nil {
		return catalog, fmt.Errorf("gosql: read directory catalog: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&catalog); err != nil {
		return catalog, fmt.Errorf("gosql: decode directory catalog: %w", err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return catalog, errors.New("gosql: directory catalog must contain one JSON object")
	}
	if len(catalog.Tables) == 0 {
		return catalog, errors.New("gosql: directory catalog must declare at least one table")
	}
	for i, table := range catalog.Tables {
		if strings.TrimSpace(table.Name) == "" || strings.ContainsRune(table.Name, 0) {
			return catalog, fmt.Errorf("gosql: directory catalog table %d requires a valid name", i+1)
		}
		switch table.Format {
		case "parquet", "csv", "json", "ndjson":
		default:
			return catalog, fmt.Errorf("gosql: directory table %q has unsupported format %q", table.Name, table.Format)
		}
		if table.Options != nil {
			if table.Options.Header != nil && table.Format != "csv" {
				return catalog, fmt.Errorf("gosql: directory table %q option header requires format csv", table.Name)
			}
			if table.Options.Columns != nil && table.Format == "parquet" {
				return catalog, fmt.Errorf("gosql: directory table %q option columns requires a text format", table.Name)
			}
			if table.Options.ValidateSchema && (table.Options.Columns == nil || strings.TrimSpace(*table.Options.Columns) == "") {
				return catalog, fmt.Errorf("gosql: directory table %q option validate_schema requires explicit text columns", table.Name)
			}
			if table.Options.PartitionColumns != nil && table.Format != "parquet" {
				return catalog, fmt.Errorf("gosql: directory table %q option partition_columns requires format parquet", table.Name)
			}
		}
		if !fs.ValidPath(table.Pattern) || table.Pattern == "." || strings.ContainsAny(table.Pattern, "\\:\x00") {
			return catalog, fmt.Errorf("gosql: directory table %q requires a relative file pattern: %q", table.Name, table.Pattern)
		}
	}
	return catalog, nil
}

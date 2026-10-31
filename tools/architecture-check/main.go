package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const module = "github.com/steamwo/fssql/"

type violation struct{ File, Import, Reason string }

func forbidden(importer, imp string) (bool, string) {
	if !strings.HasPrefix(imp, module+"internal/") {
		return false, ""
	}
	local := strings.TrimPrefix(imp, module)
	switch {
	case strings.HasPrefix(importer, "internal/core/types"):
		if strings.Contains(local, "/binder") || strings.HasPrefix(local, "internal/optimizer") || strings.HasPrefix(local, "internal/format/parquet") || strings.HasPrefix(local, "internal/execution/") {
			return true, "core/types must not depend on binder/optimizer/parquet/execution"
		}
	case strings.HasPrefix(importer, "internal/core/vector"):
		if strings.HasPrefix(local, "internal/format/") || strings.HasPrefix(local, "internal/sql/") || strings.HasPrefix(local, "internal/optimizer") {
			return true, "core/vector must not depend on format/sql/optimizer"
		}
	case strings.HasPrefix(importer, "internal/core/memory"):
		if strings.HasPrefix(local, "internal/execution/join") || strings.HasPrefix(local, "internal/execution/sort") || strings.HasPrefix(local, "internal/execution/aggregate") {
			return true, "core/memory must not depend on concrete join/sort/aggregate"
		}
	case strings.HasPrefix(importer, "internal/optimizer"):
		if strings.Contains(local, "parquet") || strings.Contains(local, "s3") || strings.Contains(local, "sftp") {
			return true, "optimizer must not depend on storage formats/protocols"
		}
	case strings.HasPrefix(importer, "internal/storage/vfs"):
		if strings.HasPrefix(local, "internal/format/parquet") || strings.HasPrefix(local, "internal/format/csv") || strings.HasPrefix(local, "internal/format/json") {
			return true, "VFS must not depend on formats"
		}
	case strings.HasPrefix(importer, "internal/execution/scheduler"):
		if strings.Contains(local, "parquet") || strings.Contains(local, "csv") {
			return true, "scheduler must only know generic Task"
		}
	case strings.HasPrefix(importer, "internal/catalog"):
		if strings.HasPrefix(local, "internal/execution/") || strings.HasPrefix(local, "internal/core/vector") || strings.HasPrefix(local, "internal/storage/remote") {
			return true, "catalog must not depend on runtime execution/vector/remote client"
		}
	case strings.HasPrefix(importer, "internal/format/"):
		if strings.HasPrefix(local, "internal/optimizer") {
			return true, "format readers must not depend on optimizer implementation"
		}
	}
	return false, ""
}

func scan(root string) ([]violation, error) {
	var out []violation
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		importer := filepath.ToSlash(filepath.Dir(rel))
		for _, spec := range f.Imports {
			raw := spec.Path.Value
			imp, err := strconv.Unquote(raw)
			if err != nil {
				return err
			}
			if bad, reason := forbidden(importer, imp); bad {
				out = append(out, violation{File: filepath.ToSlash(rel), Import: imp, Reason: reason})
			}
		}
		return nil
	})
	return out, err
}
func main() {
	root := "."
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	vs, err := scan(root)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if len(vs) > 0 {
		for _, v := range vs {
			fmt.Fprintf(os.Stderr, "%s imports %s: %s\n", v.File, v.Import, v.Reason)
		}
		os.Exit(1)
	}
	fmt.Println("architecture-check: OK")
}

var _ ast.Node

package binder

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/parser"
)

func TestCopyEncryptionBinding(t *testing.T) {
	for _, n := range []int{16, 32} {
		key := bytes.Repeat([]byte{0x79}, n)
		encoded := base64.StdEncoding.EncodeToString(key)
		stmt, err := parser.Parse("COPY (SELECT 1) TO 'out.parquet' (ENCRYPTION_CONFIG {footer_key_value: from_base64('" + encoded + "'), master_key_id: 'key-001'}, FORMAT ENCRYPTED_PARQUET)")
		if err != nil {
			t.Fatal(err)
		}
		options, err := bindCopyOptions("out.parquet", stmt.(*ast.CopyStmt).Options)
		if err != nil {
			t.Fatal(err)
		}
		if options.Encryption.Algorithm != fmt.Sprintf("AES-%d-GCM", n*8) || !bytes.Equal(options.Encryption.Key(), key) {
			t.Fatal("raw key was transformed")
		}
		if options.Compression != "snappy" {
			t.Fatalf("encrypted Parquet default compression=%q", options.Compression)
		}
		for _, formatted := range []string{fmt.Sprintf("%+v", options), fmt.Sprintf("%#v", options.Encryption), fmt.Sprintf("%+v", stmt)} {
			if strings.Contains(formatted, encoded) || strings.Contains(formatted, string(key)) {
				t.Fatal("key exposed in formatting")
			}
		}
		owned := options.Encryption.Key()
		options.Encryption.Clear()
		options.Encryption.Clear()
		if !bytes.Equal(owned, make([]byte, n)) || len(options.Encryption.Key()) != 0 {
			t.Fatal("owned key was not cleared")
		}
	}
}

func TestCopyEncryptionRejectsInvalidOptions(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{'z'}, 16))
	fields := "footer_key_value: from_base64('" + key + "'), master_key_id: 'key-001'"
	for _, options := range []string{
		"FORMAT ENCRYPTED_PARQUET", "FORMAT PARQUET, ENCRYPTION_CONFIG {" + fields + "}", "FORMAT CSV, ENCRYPTION_CONFIG {" + fields + "}",
		"FORMAT ENCRYPTED_PARQUET, HEADER, ENCRYPTION_CONFIG {" + fields + "}",
		"FORMAT ENCRYPTED_PARQUET, COMPRESSION gzip, ENCRYPTION_CONFIG {" + fields + "}",
		"FORMAT ENCRYPTED_PARQUET, ENCRYPTION_CONFIG {" + fields + "}, ENCRYPTION_CONFIG {" + fields + "}",
	} {
		rejectCopyEncryption(t, options, key)
	}
	for _, fields := range []string{
		"", "master_key_id: 'key-001'", "footer_key_value: from_base64('" + key + "')",
		fields + ", master_key_id: 'other'", fields + ", footer_key_value: from_base64('" + key + "')", fields + ", mystery: '" + key + "'",
		"footer_key_value: '" + key + "', master_key_id: 'id'",
		"footer_key_value: from_base64('" + key + "'), master_key_id: ''",
		"footer_key_value: from_base64('" + strings.Repeat("!", 24) + "'), master_key_id: 'id'",
		"footer_key_value: from_base64('" + base64.StdEncoding.EncodeToString(make([]byte, 24)) + "'), master_key_id: 'id'",
		"footer_key_value: from_base64('" + key + "', 'extra'), master_key_id: 'id'",
		"footer_key_value: from_base64(other()), master_key_id: 'id'",
		"footer_key_value: random(), master_key_id: 'id'",
		"footer_key_value: from_base64('" + key + "') || 'x', master_key_id: 'id'",
	} {
		rejectCopyEncryption(t, "FORMAT ENCRYPTED_PARQUET, ENCRYPTION_CONFIG {"+fields+"}", key)
	}
	for _, suffix := range []string{" '" + key + "'", " ; '" + key + "'"} {
		rejectCopyEncryption(t, "FORMAT ENCRYPTED_PARQUET, ENCRYPTION_CONFIG {"+fields+"})"+suffix+"--", key)
	}
}

func rejectCopyEncryption(t *testing.T, options, key string) {
	t.Helper()
	sql := "COPY (SELECT 1) TO 'out.parquet' (" + options + ")"
	for _, compile := range []bool{false, true} {
		var stmt ast.Statement
		var err error
		if compile {
			var template *parser.Template
			template, err = parser.Compile(sql)
			if err == nil {
				stmt, err = template.Bind(nil, nil)
			}
		} else {
			stmt, err = parser.Parse(sql)
		}
		if err == nil {
			_, err = bindCopyOptions("out.parquet", stmt.(*ast.CopyStmt).Options)
		}
		if err == nil {
			t.Fatal("invalid encrypted COPY was accepted")
		}
		if strings.Contains(err.Error(), key) {
			t.Fatal("key exposed in diagnostic")
		}
	}
}

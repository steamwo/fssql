package session

import (
	"bytes"
	"context"
	"encoding/base64"
	"testing"

	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func TestCopyEncryptionQueryAndExecutionCleanup(t *testing.T) {
	engine, _, _, manager, _ := newStatementEngineHarness(t)
	engine.copy = CopyExecutorFunc(func(*query.Context, *binder.BoundCopyTo, *PreparedSQLQuery, string) error { return nil })
	encoded := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 16))
	sql := "COPY (SELECT 1 AS id) TO 'mem://out.parquet' (FORMAT ENCRYPTED_PARQUET, ENCRYPTION_CONFIG {footer_key_value: from_base64('" + encoded + "'), master_key_id: 'master'})"
	q := query.New(context.Background(), manager)
	prepared, err := engine.Prepare(context.Background(), q, sql)
	if err != nil {
		t.Fatal(err)
	}
	bound := prepared.(*preparedCopyStatement).bound
	owned := bound.Options.Encryption.Key()
	if len(owned) != 16 {
		t.Fatal("missing bound key")
	}
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(owned, make([]byte, 16)) || len(bound.Options.Encryption.Key()) != 0 {
		t.Fatal("unexecuted query retained raw key")
	}
	// The production executor also clears its bound key on errors before writer
	// construction; query cleanup remains an idempotent backstop.
	q = query.New(context.Background(), manager)
	defer q.Close()
	prepared, err = engine.Prepare(context.Background(), q, sql)
	if err != nil {
		t.Fatal(err)
	}
	bound = prepared.(*preparedCopyStatement).bound
	owned = bound.Options.Encryption.Key()
	if err := (&ExportCopyExecutor{}).Run(q, bound, nil, sql); err == nil {
		t.Fatal("expected execution failure")
	}
	if !bytes.Equal(owned, make([]byte, 16)) || len(bound.Options.Encryption.Key()) != 0 {
		t.Fatal("failed execution retained raw key")
	}
}

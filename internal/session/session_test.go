package session

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/catalog"
	"github.com/steamwo/fssql/internal/settings"
)

type fakeQuery struct {
	canceled bool
	closed   bool
	closeErr error
}

func (q *fakeQuery) Cancel()     { q.canceled = true }
func (q *fakeQuery) Close() error { q.closed = true; return q.closeErr }

type fakeFactory struct {
	last     *fakeQuery
	settings settings.QuerySettings
	group    string
	err      error
}

func (f *fakeFactory) NewQuery(_ context.Context, q settings.QuerySettings, group string) (QueryLifecycle, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.settings = q
	f.group = group
	f.last = &fakeQuery{}
	return f.last, nil
}

func newTestSession(t *testing.T) (*Session, *fakeFactory) {
	t.Helper()
	c, _ := catalog.NewMemoryCatalog(8)
	r := settings.NewRegistry()
	if err := r.Register(settings.Definition{Name: "threads", Kind: settings.KindInt64, Default: settings.Int64(1)}); err != nil {
		t.Fatal(err)
	}
	f := &fakeFactory{}
	s, err := New(Config{
		Catalog:       c,
		Settings:      r,
		Engine:        settings.NewLayer(map[string]settings.Value{"threads": settings.Int64(2)}),
		ResourceGroup: settings.NewLayer(map[string]settings.Value{"threads": settings.Int64(3)}),
		GroupName:     "analytics",
		Queries:       f,
	})
	if err != nil {
		t.Fatal(err)
	}
	return s, f
}

func TestStatementFreezesSettingHierarchy(t *testing.T) {
	s, f := newTestSession(t)
	if err := s.SetSetting("threads", settings.Int64(4)); err != nil {
		t.Fatal(err)
	}
	st, err := s.BeginStatement(context.Background(), settings.NewLayer(map[string]settings.Value{"threads": settings.Int64(5)}))
	if err != nil {
		t.Fatal(err)
	}
	v, _ := st.Settings().Get("threads")
	n, _ := v.Int64Value()
	if n != 5 || f.group != "analytics" {
		t.Fatalf("value=%d group=%s", n, f.group)
	}
	if err := s.SetSetting("threads", settings.Int64(9)); err != nil {
		t.Fatal(err)
	}
	v, _ = st.Settings().Get("threads")
	n, _ = v.Int64Value()
	if n != 5 {
		t.Fatal("statement settings mutated after begin")
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStatementCloseReleasesQueryAndRollsBackCatalog(t *testing.T) {
	s, f := newTestSession(t)
	st, err := s.BeginStatement(context.Background(), settings.NewLayer(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Catalog().CreateExternalTable(context.Background(), catalog.ExternalTable{Name: catalog.ObjectName{Name: "x"}, TableFunction: "read_parquet", Source: "file:///x"}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if !f.last.canceled || !f.last.closed {
		t.Fatal("query lifecycle not closed")
	}
	check, _ := s.config.Catalog.Begin(context.Background())
	if _, ok, _ := check.Get(context.Background(), catalog.ObjectName{Name: "x"}); ok {
		t.Fatal("uncommitted metadata leaked")
	}
}

func TestCommitCatalogPersistsAndCloseDoesNotRollback(t *testing.T) {
	s, _ := newTestSession(t)
	st, _ := s.BeginStatement(context.Background(), settings.NewLayer(nil))
	_ = st.Catalog().CreateExternalTable(context.Background(), catalog.ExternalTable{Name: catalog.ObjectName{Name: "x"}, TableFunction: "read_parquet", Source: "file:///x"})
	if err := st.CommitCatalog(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	check, _ := s.config.Catalog.Begin(context.Background())
	if _, ok, _ := check.Get(context.Background(), catalog.ObjectName{Name: "x"}); !ok {
		t.Fatal("committed metadata missing")
	}
}

func TestBeginQueryFailureRollsBackCatalog(t *testing.T) {
	s, f := newTestSession(t)
	f.err = errors.New("boom")
	if _, err := s.BeginStatement(context.Background(), settings.NewLayer(nil)); err == nil {
		t.Fatal("expected query factory failure")
	}
	tx, err := s.config.Catalog.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCloseContinuesCatalogCleanupWhenQueryCloseFails(t *testing.T) {
	s, f := newTestSession(t)
	st, _ := s.BeginStatement(context.Background(), settings.NewLayer(nil))
	f.last.closeErr = errors.New("query close")
	_ = st.Catalog().CreateExternalTable(context.Background(), catalog.ExternalTable{Name: catalog.ObjectName{Name: "x"}, TableFunction: "read_parquet", Source: "file:///x"})
	if err := st.Close(); err == nil {
		t.Fatal("expected joined query close error")
	}
	check, _ := s.config.Catalog.Begin(context.Background())
	if _, ok, _ := check.Get(context.Background(), catalog.ObjectName{Name: "x"}); ok {
		t.Fatal("catalog cleanup skipped after query error")
	}
}

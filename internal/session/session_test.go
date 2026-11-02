package session

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/catalog"
	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/settings"
)

type fakeCatalog struct{ tx *fakeTx }

func (f *fakeCatalog) Begin(context.Context) (catalog.Transaction, error) {
	f.tx = &fakeTx{}
	return f.tx, nil
}

type fakeTx struct {
	committed bool
	rolled    bool
}

func (*fakeTx) Get(context.Context, catalog.ObjectName) (catalog.Entry, bool, error) {
	return catalog.Entry{}, false, nil
}
func (*fakeTx) List(context.Context, string) ([]catalog.Entry, error) { return nil, nil }
func (*fakeTx) CreateExternalTable(context.Context, catalog.ExternalTable) error { return nil }
func (*fakeTx) CreateView(context.Context, catalog.View) error                 { return nil }
func (f *fakeTx) Commit(context.Context) error {
	f.committed = true
	return nil
}
func (f *fakeTx) Rollback(context.Context) error {
	f.rolled = true
	return nil
}

func testRegistry(t *testing.T) *settings.Registry {
	t.Helper()
	r := settings.NewRegistry()
	if err := r.Register(settings.Definition{Name: "threads", Kind: settings.KindInt64, Default: settings.Int64(1)}); err != nil {
		t.Fatal(err)
	}
	return r
}

func testLayer(v int64) settings.Layer {
	return settings.NewLayer(map[string]settings.Value{"threads": settings.Int64(v)})
}

func TestStatementFreezesSettingsPrecedence(t *testing.T) {
	c := &fakeCatalog{}
	s, err := New(c, testRegistry(t), testLayer(2), testLayer(3), testLayer(4))
	if err != nil {
		t.Fatal(err)
	}
	st, err := s.BeginStatement(context.Background(), testLayer(5))
	if err != nil {
		t.Fatal(err)
	}
	v, ok := st.Settings().Get("threads")
	if !ok {
		t.Fatal("missing setting")
	}
	got, err := v.Int64Value()
	if err != nil {
		t.Fatal(err)
	}
	if got != 5 {
		t.Fatalf("threads=%d", got)
	}
	if err := st.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestQueryContextOutlivesStatement(t *testing.T) {
	c := &fakeCatalog{}
	s, err := New(c, testRegistry(t), settings.Layer{}, settings.Layer{}, testLayer(7))
	if err != nil {
		t.Fatal(err)
	}
	st, err := s.BeginStatement(context.Background(), settings.Layer{})
	if err != nil {
		t.Fatal(err)
	}
	q, err := st.NewQueryContext(context.Background(), memory.NewManager(1<<20), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !c.tx.rolled {
		t.Fatal("catalog transaction not rolled back")
	}
	v, ok := q.Settings().Get("threads")
	if !ok {
		t.Fatal("query setting missing")
	}
	got, err := v.Int64Value()
	if err != nil {
		t.Fatal(err)
	}
	if got != 7 {
		t.Fatalf("query setting=%d", got)
	}
	if q.Closed() {
		t.Fatal("statement close closed transferred query")
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitCatalogDoesNotCloseQueryFactory(t *testing.T) {
	c := &fakeCatalog{}
	s, err := New(c, testRegistry(t), settings.Layer{}, settings.Layer{}, settings.Layer{})
	if err != nil {
		t.Fatal(err)
	}
	st, err := s.BeginStatement(context.Background(), settings.Layer{})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CommitCatalog(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !c.tx.committed {
		t.Fatal("catalog transaction not committed")
	}
	q, err := st.NewQueryContext(context.Background(), memory.NewManager(1<<20), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.tx.rolled {
		t.Fatal("committed catalog transaction rolled back")
	}
}

func TestCanceledContextStillRollsBack(t *testing.T) {
	c := &fakeCatalog{}
	s, err := New(c, testRegistry(t), settings.Layer{}, settings.Layer{}, settings.Layer{})
	if err != nil {
		t.Fatal(err)
	}
	st, err := s.BeginStatement(context.Background(), settings.Layer{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := st.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if !c.tx.rolled {
		t.Fatal("rollback cleanup skipped")
	}
}

package catalog

import (
	"context"
	"fmt"
	"strings"
)

// SessionCatalog overlays connection-local TEMP objects over a shared catalog.
// Explicit main/schema references bypass the overlay; temp.name selects it.
type SessionCatalog struct {
	Base      Catalog
	Temporary *MemoryCatalog
	Limit     int
}

func NewSessionCatalog(base Catalog, limit int) (*SessionCatalog, error) {
	temp, err := NewMemoryCatalog(limit)
	if err != nil {
		return nil, err
	}
	return &SessionCatalog{Base: base, Temporary: temp, Limit: limit}, nil
}
func (c *SessionCatalog) Begin(ctx context.Context) (Transaction, error) {
	base, err := c.Base.Begin(ctx)
	if err != nil {
		return nil, err
	}
	temp, err := c.Temporary.Begin(ctx)
	if err != nil {
		base.Rollback(context.Background())
		return nil, err
	}
	return &sessionTransaction{base: base, temp: temp}, nil
}

type sessionTransaction struct {
	base, temp Transaction
	write      int
}

func (t *sessionTransaction) Get(ctx context.Context, name ObjectName) (Entry, bool, error) {
	if name.Schema == "" || strings.EqualFold(name.Schema, "temp") {
		local := name
		local.Schema = "temp"
		entry, ok, err := t.temp.Get(ctx, local)
		if err != nil || ok || name.Schema != "" {
			return entry, ok, err
		}
	}
	if name.Schema == "" {
		name.Schema = "main"
	}
	return t.base.Get(ctx, name)
}
func (t *sessionTransaction) List(ctx context.Context, schema string) ([]Entry, error) {
	if schema == "temp" {
		return t.temp.List(ctx, "temp")
	}
	base, err := t.base.List(ctx, schema)
	if err != nil || schema != "" {
		return base, err
	}
	local, err := t.temp.List(ctx, "temp")
	if err != nil {
		return nil, err
	}
	return append(base, local...), nil
}
func (t *sessionTransaction) target(temp bool) (Transaction, error) {
	side := 1
	if temp {
		side = 2
	}
	if t.write != 0 && t.write != side {
		return nil, fmt.Errorf("cannot mix persistent and temporary metadata writes in one statement")
	}
	t.write = side
	if temp {
		return t.temp, nil
	}
	return t.base, nil
}
func (t *sessionTransaction) CreateView(ctx context.Context, v View) error {
	temp := v.Name.Schema == "temp"
	tx, err := t.target(temp)
	if err != nil {
		return err
	}
	return tx.CreateView(ctx, v)
}
func (t *sessionTransaction) CreateTemporaryView(ctx context.Context, v View) error {
	v.Name.Schema = "temp"
	tx, err := t.target(true)
	if err != nil {
		return err
	}
	return tx.CreateView(ctx, v)
}
func (t *sessionTransaction) ReplaceView(ctx context.Context, v View) error {
	tx, err := t.target(v.Name.Schema == "temp")
	if err != nil {
		return err
	}
	r, ok := tx.(interface {
		ReplaceView(context.Context, View) error
	})
	if !ok {
		return fmt.Errorf("catalog replacement is unsupported")
	}
	return r.ReplaceView(ctx, v)
}
func (t *sessionTransaction) ReplaceTemporaryView(ctx context.Context, v View) error {
	v.Name.Schema = "temp"
	tx, err := t.target(true)
	if err != nil {
		return err
	}
	return tx.(interface {
		ReplaceView(context.Context, View) error
	}).ReplaceView(ctx, v)
}
func (t *sessionTransaction) CreateExternalTable(ctx context.Context, v ExternalTable) error {
	tx, err := t.target(v.Name.Schema == "temp")
	if err != nil {
		return err
	}
	return tx.CreateExternalTable(ctx, v)
}
func (t *sessionTransaction) CreateTemporaryExternalTable(ctx context.Context, v ExternalTable) error {
	v.Name.Schema = "temp"
	tx, err := t.target(true)
	if err != nil {
		return err
	}
	return tx.CreateExternalTable(ctx, v)
}
func (t *sessionTransaction) Commit(ctx context.Context) error {
	if t.write == 2 {
		defer t.base.Rollback(context.Background())
		return t.temp.Commit(ctx)
	}
	defer t.temp.Rollback(context.Background())
	return t.base.Commit(ctx)
}
func (t *sessionTransaction) Rollback(ctx context.Context) error {
	a := t.base.Rollback(ctx)
	b := t.temp.Rollback(ctx)
	if a != nil {
		return a
	}
	return b
}

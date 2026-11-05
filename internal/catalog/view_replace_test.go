package catalog

import (
	"context"
	"github.com/steamwo/fssql/internal/core/types"
	"testing"
)

func TestCatalogAtomicViewReplacement(t *testing.T) {
	mem, _ := NewMemoryCatalog(1)
	db, _ := openFakeCatalogDB(t)
	sqlite, err := NewSQLiteCatalog(context.Background(), db, 1, 8)
	if err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]Catalog{"memory": mem, "sqlite": sqlite} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			view := View{Name: ObjectName{Name: "v"}, SQL: "SELECT 1", Columns: []ColumnDefinition{{Name: "x", Type: types.LogicalType{ID: types.Integer}}}}
			tx, err := c.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			replacer := tx.(interface {
				ReplaceView(context.Context, View) error
			})
			if err = replacer.ReplaceView(ctx, view); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			tx, _ = c.Begin(ctx)
			view.SQL = "SELECT 2"
			if err = tx.(interface {
				ReplaceView(context.Context, View) error
			}).ReplaceView(ctx, view); err != nil {
				t.Fatal(err)
			}
			if err = tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			tx, _ = c.Begin(ctx)
			got, _, err := tx.Get(ctx, view.Name)
			if err != nil || got.View.SQL != "SELECT 1" {
				t.Fatalf("rollback lost view: %+v %v", got, err)
			}
			if err = tx.(interface {
				ReplaceView(context.Context, View) error
			}).ReplaceView(ctx, view); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			tx, _ = c.Begin(ctx)
			got, _, err = tx.Get(ctx, view.Name)
			if err != nil || got.View.SQL != "SELECT 2" {
				t.Fatalf("replace=%+v %v", got, err)
			}
			tx.Rollback(ctx)
		})
	}
}

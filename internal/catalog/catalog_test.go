package catalog

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
)

func testExternalTable(name string) ExternalTable {
	return ExternalTable{
		Name:          ObjectName{Name: name},
		Columns:       []ColumnDefinition{{Name: "id", Type: types.LogicalType{ID: types.BigInt}, Nullable: true}},
		TableFunction: "read_parquet",
		Source:        "file:///x.parquet",
	}
}

func TestMemoryCatalogCommitAndRepeatableRead(t *testing.T) {
	c, _ := NewMemoryCatalog(4)
	ctx := context.Background()
	r, _ := c.Begin(ctx)
	w, _ := c.Begin(ctx)
	if err := w.CreateExternalTable(ctx, testExternalTable("t")); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := r.Get(ctx, ObjectName{Name: "t"}); err != nil || ok {
		t.Fatalf("old snapshot ok=%v err=%v", ok, err)
	}
	if err := r.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	r2, _ := c.Begin(ctx)
	got, ok, err := r2.Get(ctx, ObjectName{Name: "t"})
	if err != nil || !ok {
		t.Fatalf("get ok=%v err=%v", ok, err)
	}
	if got.ExternalTable.Name.Schema != "main" {
		t.Fatalf("stored name changed: %+v", got.ExternalTable.Name)
	}
}

func TestMemoryCatalogWriteConflict(t *testing.T) {
	c, _ := NewMemoryCatalog(4)
	ctx := context.Background()
	a, _ := c.Begin(ctx)
	b, _ := c.Begin(ctx)
	_ = a.CreateExternalTable(ctx, testExternalTable("a"))
	if err := a.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	_ = b.CreateExternalTable(ctx, testExternalTable("b"))
	if err := b.Commit(ctx); err == nil {
		t.Fatal("expected stale writer conflict")
	}
}

func TestMemoryCatalogCapacityAndRollback(t *testing.T) {
	c, _ := NewMemoryCatalog(1)
	ctx := context.Background()
	tx, _ := c.Begin(ctx)
	_ = tx.CreateExternalTable(ctx, testExternalTable("a"))
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	tx, _ = c.Begin(ctx)
	_ = tx.CreateExternalTable(ctx, testExternalTable("a"))
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	tx, _ = c.Begin(ctx)
	_ = tx.CreateExternalTable(ctx, testExternalTable("b"))
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("expected capacity error")
	}
}

func TestMemoryCatalogDeepCopiesMetadata(t *testing.T) {
	c, _ := NewMemoryCatalog(4)
	ctx := context.Background()
	src := testExternalTable("t")
	tx, _ := c.Begin(ctx)
	if err := tx.CreateExternalTable(ctx, src); err != nil {
		t.Fatal(err)
	}
	src.Columns[0].Name = "mutated"
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	tx, _ = c.Begin(ctx)
	got, _, _ := tx.Get(ctx, ObjectName{Name: "t"})
	got.ExternalTable.Columns[0].Name = "again"
	got2, _, _ := tx.Get(ctx, ObjectName{Name: "t"})
	if got2.ExternalTable.Columns[0].Name != "id" {
		t.Fatalf("metadata alias: %+v", got2)
	}
}

func TestCatalogValidation(t *testing.T) {
	if _, err := NewMemoryCatalog(0); err == nil {
		t.Fatal("expected bounded capacity")
	}
	c, _ := NewMemoryCatalog(2)
	ctx := context.Background()
	tx, _ := c.Begin(ctx)
	bad := testExternalTable("t")
	bad.Source = ""
	if err := tx.CreateExternalTable(ctx, bad); err == nil {
		t.Fatal("expected source validation")
	}
	bad = testExternalTable("t")
	bad.Columns = append(bad.Columns, bad.Columns[0])
	if err := tx.CreateExternalTable(ctx, bad); err == nil {
		t.Fatal("expected duplicate column validation")
	}
}

func TestMemoryCatalogListDeterministicAndComplexTypeCopy(t *testing.T) {
	c, _ := NewMemoryCatalog(4)
	ctx := context.Background()
	complexType := types.NewList(types.LogicalType{ID: types.BigInt})
	a := testExternalTable("z")
	a.Columns[0].Type = complexType
	b := testExternalTable("a")
	tx, _ := c.Begin(ctx)
	_ = tx.CreateExternalTable(ctx, a)
	_ = tx.CreateExternalTable(ctx, b)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	tx, _ = c.Begin(ctx)
	list, err := tx.List(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].ExternalTable.Name.Name != "a" || list[1].ExternalTable.Name.Name != "z" {
		t.Fatalf("list=%+v", list)
	}
	list[1].ExternalTable.Columns[0].Type.Child.ID = types.Varchar
	got, _, _ := tx.Get(ctx, ObjectName{Name: "z"})
	if got.ExternalTable.Columns[0].Type.Child.ID != types.BigInt {
		t.Fatal("complex type aliased")
	}
}

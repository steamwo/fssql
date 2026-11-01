package export

import (
	"errors"
	"reflect"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

func newRoutingTestChunk(t *testing.T, schema []Column, regions []any, ids []int32) (*vector.DataChunk, *memory.Scope) {
	t.Helper()
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	columns := make([]*vector.Vector, len(schema))
	var err error
	for i := range schema {
		columns[i], err = vector.NewFlat(scope, bm, schema[i].Type, len(ids))
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(schema) == 1 {
		for i, id := range ids {
			columns[0].SetInt32(i, id)
		}
	} else {
		for i, value := range regions {
			if value == nil {
				columns[0].SetNull(i)
				continue
			}
			if err := columns[0].SetString(i, value.(string)); err != nil {
				t.Fatal(err)
			}
		}
		for i, id := range ids {
			columns[1].SetInt32(i, id)
		}
	}
	chunk, err := vector.NewDataChunk(columns...)
	if err != nil {
		t.Fatal(err)
	}
	return chunk, scope
}

func TestSingleAndMultiFileRouting(t *testing.T) {
	schema := []Column{{Name: "id", Type: types.LogicalType{ID: types.Integer}}}
	chunk, scope := newRoutingTestChunk(t, schema, nil, []int32{1, 2})
	defer func() {
		chunk.Release()
		if err := scope.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	single, err := NewFileRouter(RoutingConfig{Mode: LayoutSingleFile, SingleFilePath: "result.csv"}, schema)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := single.RouteChunk(chunk, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 1 || routes[0].Path != "result.csv" || routes[0].Rows != nil {
		t.Fatalf("routes=%+v", routes)
	}
	multi, err := NewFileRouter(RoutingConfig{Mode: LayoutMultiFile, Prefix: "data", Extension: "csv"}, schema)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := multi.RouteChunk(chunk, ""); !errors.Is(err, ErrInvalidRouting) {
		t.Fatalf("expected stable id error: %v", err)
	}
	routes, err = multi.RouteChunk(chunk, "000001")
	if err != nil {
		t.Fatal(err)
	}
	if routes[0].Path != "data/part-000001.csv" {
		t.Fatalf("path=%s", routes[0].Path)
	}
}

func TestPartitionRoutingDeterministicAndNull(t *testing.T) {
	schema := []Column{{Name: "region", Type: types.LogicalType{ID: types.Varchar}}, {Name: "id", Type: types.LogicalType{ID: types.Integer}}}
	chunk, scope := newRoutingTestChunk(t, schema, []any{"us/west", nil, "apac", "us/west"}, []int32{1, 2, 3, 4})
	defer func() {
		chunk.Release()
		if err := scope.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	r, err := NewFileRouter(RoutingConfig{Mode: LayoutPartitioned, Prefix: "out", Extension: "parquet", PartitionColumns: []int{0}}, schema)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := r.RouteChunk(chunk, "segA")
	if err != nil {
		t.Fatal(err)
	}
	got := []string{routes[0].Path, routes[1].Path, routes[2].Path}
	want := []string{"out/region=__HIVE_DEFAULT_PARTITION__/part-segA.parquet", "out/region=apac/part-segA.parquet", "out/region=us%2Fwest/part-segA.parquet"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("paths=%v", got)
	}
	if !reflect.DeepEqual(routes[2].Rows, []uint32{0, 3}) {
		t.Fatalf("rows=%v", routes[2].Rows)
	}
}

func TestPartitionRouterRejectsNestedDuplicateAndUnsafeSegment(t *testing.T) {
	list := types.LogicalType{ID: types.List}
	schema := []Column{{Name: "x", Type: list}}
	if _, err := NewFileRouter(RoutingConfig{Mode: LayoutPartitioned, Extension: "json", PartitionColumns: []int{0}}, schema); !errors.Is(err, ErrInvalidRouting) {
		t.Fatal(err)
	}
	scalar := []Column{{Name: "x", Type: types.LogicalType{ID: types.Integer}}}
	if _, err := NewFileRouter(RoutingConfig{Mode: LayoutPartitioned, Extension: "json", PartitionColumns: []int{0, 0}}, scalar); !errors.Is(err, ErrInvalidRouting) {
		t.Fatal(err)
	}
	if _, err := NewFileRouter(RoutingConfig{Mode: LayoutMultiFile, Prefix: "../escape", Extension: "json"}, scalar); !errors.Is(err, ErrInvalidRouting) {
		t.Fatal(err)
	}
	r, _ := NewFileRouter(RoutingConfig{Mode: LayoutMultiFile, Extension: "json"}, scalar)
	chunk, scope := newRoutingTestChunk(t, scalar, nil, []int32{1})
	defer func() {
		chunk.Release()
		if err := scope.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	if _, err := r.RouteChunk(chunk, "../x"); !errors.Is(err, ErrInvalidRouting) {
		t.Fatal(err)
	}
}

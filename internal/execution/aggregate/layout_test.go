package aggregate

import (
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
)

func TestAggregateLayoutAlignment(t *testing.T) {
	layout, err := NewAggregateLayout(
		[]types.LogicalType{{ID: types.BigInt}, {ID: types.Varchar}},
		[]StateSpec{
			{Name: "count", Width: 8, Alignment: 8},
			{Name: "flag", Width: 1, Alignment: 1},
			{Name: "sum128", Width: 16, Alignment: 16},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !layout.GroupLayout.Relocatable() {
		t.Fatal("group layout must be relocatable")
	}
	if got := layout.States[0].Offset; got != 0 {
		t.Fatalf("count offset=%d", got)
	}
	if got := layout.States[1].Offset; got != 8 {
		t.Fatalf("flag offset=%d", got)
	}
	if got := layout.States[2].Offset; got != 16 {
		t.Fatalf("sum128 offset=%d", got)
	}
	if layout.StateWidth != 32 {
		t.Fatalf("state width=%d", layout.StateWidth)
	}
}

func TestAggregateLayoutRejectsInvalidAlignment(t *testing.T) {
	if _, err := NewAggregateLayout(nil, []StateSpec{{Name: "bad", Width: 8, Alignment: 3}}); err == nil {
		t.Fatal("expected alignment error")
	}
}

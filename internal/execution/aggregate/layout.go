package aggregate

import (
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/core/types"
	exechash "github.com/steamwo/fssql/internal/execution/hash"
)

type StateSpec struct {
	Name      string
	Width     int
	Alignment int
}

type StateSlot struct {
	Name      string
	Offset    int
	Width     int
	Alignment int
}

type AggregateLayout struct {
	GroupLayout exechash.RowLayout
	States      []StateSlot
	StateWidth  int
}

func alignUp(value, alignment int) (int, error) {
	if alignment <= 0 || alignment&(alignment-1) != 0 {
		return 0, errors.New("aggregate state alignment must be a positive power of two")
	}
	if value > int(^uint(0)>>1)-(alignment-1) {
		return 0, errors.New("aggregate state layout overflow")
	}
	return (value + alignment - 1) &^ (alignment - 1), nil
}

func NewAggregateLayout(groupTypes []types.LogicalType, states []StateSpec) (AggregateLayout, error) {
	groups, err := exechash.NewRelocatableRowLayout(groupTypes)
	if err != nil {
		return AggregateLayout{}, err
	}
	layout := AggregateLayout{GroupLayout: groups}
	offset := 0
	for _, spec := range states {
		if spec.Width <= 0 {
			return AggregateLayout{}, fmt.Errorf("aggregate state %q has invalid width %d", spec.Name, spec.Width)
		}
		aligned, err := alignUp(offset, spec.Alignment)
		if err != nil {
			return AggregateLayout{}, fmt.Errorf("aggregate state %q: %w", spec.Name, err)
		}
		if aligned > int(^uint(0)>>1)-spec.Width {
			return AggregateLayout{}, errors.New("aggregate state layout overflow")
		}
		layout.States = append(layout.States, StateSlot{
			Name: spec.Name, Offset: aligned, Width: spec.Width, Alignment: spec.Alignment,
		})
		offset = aligned + spec.Width
	}
	layout.StateWidth = offset
	return layout, nil
}

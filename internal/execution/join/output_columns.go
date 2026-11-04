package join

import (
	"errors"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

func validateRequiredOutput(config ExternalConfig) error {
	width := len(config.LeftTypes) + len(config.RightTypes)
	if config.Type == Semi || config.Type == Anti {
		width = len(config.LeftTypes)
	}
	if config.RequiredOutput != nil && len(config.RequiredOutput) != width {
		return errors.New("join required output mask width mismatch")
	}
	return nil
}

// SetRequiredOutputColumns configures immutable output requirements before
// build. The complete schema stays available to downstream index references.
func (j *StableExternalHashJoin) SetRequiredOutputColumns(needed []bool) error {
	j.core.mu.Lock()
	defer j.core.mu.Unlock()
	if j.core.built || j.core.closed || j.core.stats.BuildRows != 0 {
		return errors.New("join output requirements must be configured before build")
	}
	config := j.core.config
	config.RequiredOutput = needed
	if err := validateRequiredOutput(config); err != nil {
		return err
	}
	j.core.config.RequiredOutput = append([]bool(nil), needed...)
	return nil
}

func neededColumns(mask []bool, start, width int) []bool {
	if mask == nil {
		return nil
	}
	return mask[start : start+width]
}

func newOutputColumns(scope *memory.Scope, bm *memory.BufferManager, ts []types.LogicalType, needed []bool) ([]*vector.Vector, error) {
	cols := make([]*vector.Vector, len(ts))
	for i, typ := range ts {
		var v *vector.Vector
		var err error
		if needed != nil && !needed[i] {
			v, err = vector.NewConstant(scope, bm, typ, vector.DefaultVectorSize)
			if err == nil {
				v.SetNull(0)
			}
		} else {
			v, err = vector.NewFlat(scope, bm, typ, vector.DefaultVectorSize)
		}
		if err != nil {
			for _, column := range cols {
				if column != nil {
					column.Release()
				}
			}
			return nil, err
		}
		cols[i] = v
	}
	return cols, nil
}

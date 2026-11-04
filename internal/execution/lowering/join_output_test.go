package lowering

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

type outputMaskRuntime struct {
	binaryTestRuntime
	mask []bool
	err  error
}

func (r *outputMaskRuntime) SetRequiredOutputColumns(mask []bool) error {
	r.mask = append([]bool(nil), mask...)
	return r.err
}

type outputMaskFactory struct{ runtime *outputMaskRuntime }

func (f outputMaskFactory) OpenJoin(context.Context, planner.JoinType, []planner.Column, []planner.Column, *binder.BoundExpression, planner.CardinalityEstimate, planner.CardinalityEstimate) (pipeline.BinaryOperator, error) {
	return f.runtime, nil
}

func TestJoinOptionalOutputRequirementsConfiguredBeforeSources(t *testing.T) {
	for _, fail := range []bool{false, true} {
		runtime := &outputMaskRuntime{}
		runtime.buildSide = pipeline.BinaryBuildRight
		if fail {
			runtime.err = errors.New("invalid output mask")
		}
		sources := &fakeScanSourceFactory{src: emptySource{}}
		builder, _ := NewBuilder(sources, loweringExecutor())
		builder.Joins = outputMaskFactory{runtime: runtime}
		plan := physicalEquiJoin()
		plan.RequiredOutput = []bool{false, false}
		_, err := builder.Build(context.Background(), plan, emptySink{})
		if !reflect.DeepEqual(runtime.mask, plan.RequiredOutput) {
			t.Fatalf("mask=%v", runtime.mask)
		}
		if fail {
			if !errors.Is(err, runtime.err) || sources.calls != 0 || runtime.closed.Load() != 1 {
				t.Fatalf("err=%v sources=%d closed=%d", err, sources.calls, runtime.closed.Load())
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			runtime.Close()
		}
	}
}

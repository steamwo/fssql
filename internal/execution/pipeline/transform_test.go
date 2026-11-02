package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/query"
)

// multiplyTransform 创建完全独立的 replacement chunk，验证 runner 可以立即释放 input。
type multiplyTransform struct {
	scope *memory.Scope
	bm    *memory.BufferManager
}

func (t multiplyTransform) Transform(_ context.Context, in *vector.DataChunk) (*vector.DataChunk, error) {
	out, err := vector.NewFlat(t.scope, t.bm, types.LogicalType{ID: types.BigInt}, in.Cardinality)
	if err != nil {
		return nil, err
	}
	for row := 0; row < in.Cardinality; row++ {
		value, ok := in.Columns[0].Int64(row)
		if !ok {
			out.SetNull(row)
			continue
		}
		out.SetInt64(row, value*2)
	}
	chunk, err := vector.NewDataChunk(out)
	if err != nil {
		out.Release()
		return nil, err
	}
	return chunk, nil
}

// replacementErrorTransform 模拟已经创建 replacement 后失败，runner 必须同时回收新旧 ownership。
type replacementErrorTransform struct {
	scope *memory.Scope
	bm    *memory.BufferManager
	err   error
}

func (t replacementErrorTransform) Transform(_ context.Context, in *vector.DataChunk) (*vector.DataChunk, error) {
	out, err := vector.NewFlat(t.scope, t.bm, types.LogicalType{ID: types.BigInt}, in.Cardinality)
	if err != nil {
		return nil, err
	}
	chunk, err := vector.NewDataChunk(out)
	if err != nil {
		out.Release()
		return nil, err
	}
	return chunk, t.err
}

// nilTransform 验证成功路径禁止无 ownership 的 nil chunk。
type nilTransform struct{}

func (nilTransform) Transform(context.Context, *vector.DataChunk) (*vector.DataChunk, error) {
	return nil, nil
}

func TestTransformReplacementOwnership(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	q := query.New(context.Background(), manager)
	buffers := memory.NewBufferManager(nil)
	sink := &sumSink{}
	p := &Pipeline{
		ID:         "replacement",
		Source:     &intSource{end: 3, scope: q.Memory, bm: buffers},
		Transforms: []TransformOperator{multiplyTransform{scope: q.Memory, bm: buffers}},
		Sink:       sink,
	}
	if err := p.Run(q.Context(), 1, q); err != nil {
		t.Fatal(err)
	}
	if sink.rows != 3 || sink.sum != 6 {
		t.Fatalf("rows=%d sum=%d", sink.rows, sink.sum)
	}
	if manager.Used() != 0 {
		t.Fatalf("replacement ownership leak: %d", manager.Used())
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTransformErrorReleasesInputAndReplacement(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	q := query.New(context.Background(), manager)
	buffers := memory.NewBufferManager(nil)
	want := errors.New("transform failed")
	p := &Pipeline{
		ID:         "replacement-error",
		Source:     &intSource{end: 1, scope: q.Memory, bm: buffers},
		Transforms: []TransformOperator{replacementErrorTransform{scope: q.Memory, bm: buffers, err: want}},
		Sink:       &sumSink{},
	}
	if err := p.Run(q.Context(), 1, q); !errors.Is(err, want) {
		t.Fatalf("err=%v", err)
	}
	if manager.Used() != 0 {
		t.Fatalf("error ownership leak: %d", manager.Used())
	}
	if q.ActiveTasks() != 0 {
		t.Fatalf("task leak: %d", q.ActiveTasks())
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTransformNilReplacementFailsClosed(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	q := query.New(context.Background(), manager)
	buffers := memory.NewBufferManager(nil)
	p := &Pipeline{
		ID:         "nil-replacement",
		Source:     &intSource{end: 1, scope: q.Memory, bm: buffers},
		Transforms: []TransformOperator{nilTransform{}},
		Sink:       &sumSink{},
	}
	if err := p.Run(q.Context(), 1, q); err == nil {
		t.Fatal("expected nil replacement failure")
	}
	if manager.Used() != 0 {
		t.Fatalf("nil replacement ownership leak: %d", manager.Used())
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

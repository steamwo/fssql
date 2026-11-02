package lowering

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

type binaryTestRuntime struct {
	buildSide pipeline.BinaryBuildSide
	built     atomic.Bool
	closed    atomic.Int64
	probeDown pipeline.Sink
}

func (r *binaryTestRuntime) BuildSide() pipeline.BinaryBuildSide { return r.buildSide }
func (r *binaryTestRuntime) BuildSink() pipeline.Sink            { return &binaryBuildTestSink{runtime: r} }
func (r *binaryTestRuntime) ProbeSink(downstream pipeline.Sink) pipeline.Sink {
	r.probeDown = downstream
	return &binaryProbeTestSink{runtime: r, downstream: downstream}
}
func (r *binaryTestRuntime) Close() { r.closed.Add(1) }

type binaryBuildTestSink struct{ runtime *binaryTestRuntime }
func (*binaryBuildTestSink) Consume(context.Context, *vector.DataChunk) error { return nil }
func (s *binaryBuildTestSink) Finalize(context.Context) error { s.runtime.built.Store(true); return nil }

type binaryProbeTestSink struct {
	runtime    *binaryTestRuntime
	downstream pipeline.Sink
}
func (s *binaryProbeTestSink) Consume(ctx context.Context, chunk *vector.DataChunk) error {
	if !s.runtime.built.Load() { return errors.New("probe ran before build finalize") }
	return s.downstream.Consume(ctx, chunk)
}
func (s *binaryProbeTestSink) Finalize(ctx context.Context) error {
	if !s.runtime.built.Load() { return errors.New("probe finalized before build") }
	err := s.downstream.Finalize(ctx)
	s.runtime.Close()
	return err
}

type joinTestFactory struct {
	runtime *binaryTestRuntime
	err     error
	calls   atomic.Int64
	join    planner.JoinType
	left    []planner.Column
	right   []planner.Column
	pred    *binder.BoundExpression
}
func (f *joinTestFactory) OpenJoin(_ context.Context, join planner.JoinType, left, right []planner.Column, pred *binder.BoundExpression, _, _ planner.CardinalityEstimate) (pipeline.BinaryOperator, error) {
	f.calls.Add(1); f.join=join; f.left=append([]planner.Column(nil),left...); f.right=append([]planner.Column(nil),right...); f.pred=pred
	if f.err!=nil{return nil,f.err};return f.runtime,nil
}

func physicalJoinScan(id string) *planner.PhysicalPlan {
	scan:=physicalScan(); scan.Scan.RelationID=id; scan.Output=[]planner.Column{{Name:id+"_id",Type:types.LogicalType{ID:types.BigInt}}}; return scan
}
func physicalEquiJoin() *planner.PhysicalPlan {
	left,right:=physicalJoinScan("left"),physicalJoinScan("right")
	pred:=&binder.BoundExpression{Kind:binder.BoundBinary,Type:types.LogicalType{ID:types.Boolean},Op:"=",Children:[]*binder.BoundExpression{
		{Kind:binder.BoundColumn,Type:types.LogicalType{ID:types.BigInt},ColumnIndex:0},
		{Kind:binder.BoundColumn,Type:types.LogicalType{ID:types.BigInt},ColumnIndex:1},
	}}
	return &planner.PhysicalPlan{Kind:planner.PhysicalHashJoin,Children:[]*planner.PhysicalPlan{left,right},Join:planner.JoinInner,Predicate:pred,Output:[]planner.Column{left.Output[0],right.Output[0]}}
}

// TestBuildJoinFactoryFailsBeforeChildIO 验证 JoinFactory 语义/资源校验先于左右 reader Open。
func TestBuildJoinFactoryFailsBeforeChildIO(t *testing.T){
	want:=errors.New("join factory failed"); sources:=&fakeScanSourceFactory{src:emptySource{}}; builder,_:=NewBuilder(sources,loweringExecutor()); builder.Joins=&joinTestFactory{err:want}
	if _,err:=builder.Build(context.Background(),physicalEquiJoin(),emptySink{});!errors.Is(err,want){t.Fatalf("err=%v",err)}
	if sources.calls!=0{t.Fatalf("source opens=%d want=0",sources.calls)}
}

// TestBuildJoinRunsBuildDependencyBeforeProbe 验证 build side dependency 先 Finalize，再执行 probe root。
func TestBuildJoinRunsBuildDependencyBeforeProbe(t *testing.T){
	runtime:=&binaryTestRuntime{buildSide:pipeline.BinaryBuildRight}; sources:=&fakeScanSourceFactory{src:emptySource{}}; builder,_:=NewBuilder(sources,loweringExecutor()); factory:=&joinTestFactory{runtime:runtime}; builder.Joins=factory
	root,err:=builder.Build(context.Background(),physicalEquiJoin(),emptySink{});if err!=nil{t.Fatal(err)}
	if len(root.Dependencies)!=1{t.Fatalf("dependencies=%d",len(root.Dependencies))}
	if factory.calls.Load()!=1||factory.join!=planner.JoinInner||len(factory.left)!=1||len(factory.right)!=1{t.Fatalf("factory=%+v",factory)}
	if err:=pipeline.RunDAG(context.Background(),root,1,nil);err!=nil{t.Fatal(err)}
	if !runtime.built.Load(){t.Fatal("build not finalized")};if runtime.closed.Load()!=1{t.Fatalf("close=%d want=1",runtime.closed.Load())}
}

// TestBuildJoinParentProjectWrapsProbeOutput 验证 JOIN 上方 Project 不会被错误下放到 probe 输入。
func TestBuildJoinParentProjectWrapsProbeOutput(t *testing.T){
	runtime:=&binaryTestRuntime{buildSide:pipeline.BinaryBuildRight}; sources:=&fakeScanSourceFactory{src:emptySource{}}; builder,_:=NewBuilder(sources,loweringExecutor()); builder.Joins=&joinTestFactory{runtime:runtime}
	join:=physicalEquiJoin(); project:=&planner.PhysicalPlan{Kind:planner.PhysicalProject,Children:[]*planner.PhysicalPlan{join},Output:[]planner.Column{join.Output[0]},Projection:[]*binder.BoundExpression{{Kind:binder.BoundColumn,Type:types.LogicalType{ID:types.BigInt},ColumnIndex:0}}}
	if _,err:=builder.Build(context.Background(),project,emptySink{});err!=nil{t.Fatal(err)}
	if _,ok:=runtime.probeDown.(*pipeline.TransformSink);!ok{t.Fatalf("probe downstream=%T want *pipeline.TransformSink",runtime.probeDown)}
}

// TestBuildJoinDependencyFailureClosesRuntime 验证 build dependency 失败时 join runtime 经 CloseableSink/Abort 回收。
func TestBuildJoinDependencyFailureClosesRuntime(t *testing.T){
	want:=errors.New("scan failed"); runtime:=&binaryTestRuntime{buildSide:pipeline.BinaryBuildRight}; sources:=&fakeScanSourceFactory{src:errorSource{err:want}}; builder,_:=NewBuilder(sources,loweringExecutor()); builder.Joins=&joinTestFactory{runtime:runtime}
	root,err:=builder.Build(context.Background(),physicalEquiJoin(),emptySink{});if err!=nil{t.Fatal(err)}
	if err:=pipeline.RunDAG(context.Background(),root,1,nil);!errors.Is(err,want){t.Fatalf("err=%v",err)}
	if runtime.closed.Load()==0{t.Fatal("join runtime was not closed")}
}

package session

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"

	"github.com/steamwo/fssql/internal/catalog"
	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/expression"
	"github.com/steamwo/fssql/internal/execution/lowering"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/optimizer"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/binder"
	"github.com/steamwo/fssql/internal/sql/parser"
)

type QuerySourceFactory interface { NewQuerySources(*query.Context) ([]TableBindAdapter, *RuntimeScanSourceFactory, error) }
type QuerySourceFactoryFunc func(*query.Context) ([]TableBindAdapter, *RuntimeScanSourceFactory, error)
func (f QuerySourceFactoryFunc) NewQuerySources(q *query.Context) ([]TableBindAdapter, *RuntimeScanSourceFactory, error) { if f==nil{return nil,nil,fmt.Errorf("session: nil QuerySourceFactoryFunc")};return f(q) }

type StableSQLEngineConfig struct {
	Catalog catalog.Catalog
	Functions *function.Catalog
	Sources QuerySourceFactory
	Buffers *memory.BufferManager
	Statistics stats.Provider
	Scalars expression.ScalarRegistry
	Distinct ExternalDistinctRuntimeConfig
	Sort ExternalSortRuntimeConfig
	Aggregate ExternalAggregateRuntimeConfig
	Join ExternalJoinRuntimeConfig
	Workers int
	BlockingQueue int
}
type StableSQLEngine struct { config StableSQLEngineConfig; functions *function.Catalog }
type PreparedSQLQuery struct { engine *StableSQLEngine; physical *planner.PhysicalPlan; scans *RuntimeScanSourceFactory; subqueries []preparedScalarSubquery }
func NewStableSQLEngine(config StableSQLEngineConfig)(*StableSQLEngine,error){if config.Catalog==nil||config.Sources==nil||config.Buffers==nil{return nil,fmt.Errorf("session: Stable SQL engine requires catalog, query source factory and BufferManager")};if config.Functions==nil{config.Functions=function.NewPhase5Catalog()};if config.Workers<=0{config.Workers=1};if config.BlockingQueue<=0{config.BlockingQueue=1};return &StableSQLEngine{config:config,functions:config.Functions},nil}

func(e *StableSQLEngine)Prepare(ctx context.Context,q *query.Context,sql string)(*PreparedSQLQuery,error){
 statement,err:=parser.Parse(sql);if err!=nil{return nil,err};selectStatement,ok:=statement.(*ast.SelectStmt);if !ok{return nil,fmt.Errorf("session: query execution currently requires SELECT; got %T",statement)}
 return e.prepareSelect(ctx,q,sql,selectStatement)
}
func(e *StableSQLEngine)prepareSelect(ctx context.Context,q *query.Context,sql string,selectStatement *ast.SelectStmt)(*PreparedSQLQuery,error){
	if e==nil||e.config.Catalog==nil||e.config.Sources==nil||e.functions==nil||e.config.Buffers==nil{return nil,fmt.Errorf("session: Stable SQL engine is uninitialized")};if q==nil||q.Memory==nil||q.Closed(){return nil,fmt.Errorf("session: Prepare requires live QueryContext")};if err:=ctx.Err();err!=nil{return nil,err}

	bindings,scans,err:=e.config.Sources.NewQuerySources(q);if err!=nil{return nil,fmt.Errorf("session: create query source adapters: %w",err)};if scans==nil{return nil,fmt.Errorf("session: query source factory returned nil runtime scan factory")}
	tx,err:=e.config.Catalog.Begin(ctx);if err!=nil{return nil,err};closedTx:=false;rollback:=func()error{if closedTx{return nil};closedTx=true;return tx.Rollback(context.Background())};defer func(){_=rollback()}()
	baseResolver,err:=NewSourceResolver(tx,e.functions,bindings);if err!=nil{return nil,err};planningResolver,err:=NewPlanningSourceResolver(baseResolver);if err!=nil{return nil,err};viewResolver,err:=NewViewPlanningResolver(planningResolver);if err!=nil{return nil,err};bound,err:=binder.NewQueryBinder(e.functions,viewResolver,viewResolver,sql).BindSelect(ctx,selectStatement);if err!=nil{return nil,err};if err:=planner.PreparePhase5BoundQuery(bound);err!=nil{return nil,err};queryPlanner:=planner.NewQueryPlanner(e.functions,viewResolver);opt:=&optimizer.Optimizer{Statistics:e.config.Statistics};rewritten,subqueries,err:=rewriteScalarSubqueries(ctx,queryPlanner,opt,bound);if err!=nil{return nil,err};logical,err:=queryPlanner.PlanStable(ctx,rewritten);if err!=nil{return nil,err};logical,err=planner.RewritePhase5Logical(logical);if err!=nil{return nil,err};physical,err:=opt.Optimize(ctx,logical);if err!=nil{return nil,err};plans:=make([]*planner.PhysicalPlan,0,1+len(subqueries));plans=append(plans,physical);for _,subquery:=range subqueries{plans=append(plans,subquery.physical)};if err:=bindPhysicalSourceSnapshot(q,plans...);err!=nil{return nil,err};if err:=rollback();err!=nil{return nil,fmt.Errorf("session: close read-only planning transaction: %w",err)};return &PreparedSQLQuery{engine:e,physical:physical,scans:scans,subqueries:subqueries},nil
}
func(p *PreparedSQLQuery)Run(q *query.Context,sink pipeline.Sink)error{if p==nil||p.engine==nil||p.physical==nil||p.scans==nil{return fmt.Errorf("session: prepared SQL query is uninitialized")};if q==nil||q.Memory==nil||q.Closed(){return fmt.Errorf("session: prepared SQL query requires live QueryContext")};if sink==nil{return fmt.Errorf("session: prepared SQL query requires result sink")};constants,err:=p.materializeScalarConstants(q);if err!=nil{return err};defer releaseRuntimeConstants(constants);return p.runPhysical(q,p.physical,sink,constants)}
func(p *PreparedSQLQuery)runPhysical(q *query.Context,plan *planner.PhysicalPlan,sink pipeline.Sink,constants map[int]*vector.Vector)error{if p==nil||p.engine==nil||p.scans==nil||plan==nil||sink==nil{return fmt.Errorf("session: physical query execution is uninitialized")};e:=p.engine;executor:=expression.NewExecutor(q.Memory,e.config.Buffers);defer expression.ReleasePhase5State(executor);executor.RuntimeConstants=constants;executor.Scalars=e.config.Scalars;builder,err:=lowering.NewBuilder(p.scans,executor);if err!=nil{return err};builder.BlockingQueue=e.config.BlockingQueue
	if physicalContains(plan,planner.PhysicalDistinct){factory,err:=NewExternalDistinctFactory(q,e.config.Buffers,e.config.Distinct);if err!=nil{return err};builder.Distincts=factory};if physicalContains(plan,planner.PhysicalSort){factory,err:=NewExternalSortFactory(q,e.config.Buffers,e.config.Sort);if err!=nil{return err};configureTopNSort(factory,plan);builder.Sorts=factory};if physicalContains(plan,planner.PhysicalHashAggregate){factory,err:=NewExternalAggregateFactory(q,e.config.Buffers,e.config.Aggregate);if err!=nil{return err};builder.Aggregates=factory};hasJoin:=physicalContains(plan,planner.PhysicalHashJoin)||physicalContains(plan,planner.PhysicalCrossJoin);if hasJoin{factory,err:=NewExternalJoinFactory(q,e.config.Buffers,e.config.Join);if err!=nil{return err};builder.Joins=factory};executionSink:=sink;if (hasJoin||physicalContains(plan,planner.PhysicalUnionAll))&&pipeline.SinkTakesChunkOwnership(sink){boundary,err:=pipeline.NewOwnershipCopySink(q.Memory,e.config.Buffers,sink);if err!=nil{return err};executionSink=boundary};root,err:=builder.Build(q.Context(),plan,executionSink);if err!=nil{return err};root.PreserveOrder=physicalOutputOrdered(plan);workers:=e.config.Workers;if phase5RequiresSerialExecution(plan){workers=1};return pipeline.RunDAG(q.Context(),root,workers,q)}
func(p *PreparedSQLQuery)materializeScalarConstants(q *query.Context)(map[int]*vector.Vector,error){if len(p.subqueries)==0{return nil,nil};constants:=make(map[int]*vector.Vector,len(p.subqueries));required,err:=p.requiredScalarConstants();if err!=nil{return nil,err};for _,subquery:=range p.subqueries{if subquery.physical==nil||subquery.id<0||subquery.id>=len(required){releaseRuntimeConstants(constants);return nil,fmt.Errorf("session: invalid prepared scalar subquery")};if !required[subquery.id]{continue};sink,err:=newScalarResultSink(q.Memory,p.engine.config.Buffers,subquery.typeID);if err!=nil{releaseRuntimeConstants(constants);return nil,err};if err:=p.runPhysical(q,subquery.physical,sink,constants);err!=nil{_=sink.Close();releaseRuntimeConstants(constants);return nil,err};value,err:=sink.take();if err!=nil{_=sink.Close();releaseRuntimeConstants(constants);return nil,err};constants[subquery.id]=value};return constants,nil}
func releaseRuntimeConstants(constants map[int]*vector.Vector){for id,value:=range constants{if value!=nil{value.Release()};delete(constants,id)}}
func(p *PreparedSQLQuery)Output()[]planner.Column{if p==nil||p.physical==nil{return nil};out:=make([]planner.Column,len(p.physical.Output));for i,column:=range p.physical.Output{out[i]=column;out[i].Type=cloneSourceType(column.Type)};return out}
func physicalContains(plan *planner.PhysicalPlan,kind planner.PhysicalKind)bool{if plan==nil{return false};if plan.Kind==kind{return true};for _,child:=range plan.Children{if physicalContains(child,kind){return true}};return false}
func bindPhysicalSourceSnapshot(q *query.Context,plans ...*planner.PhysicalPlan)error{if q==nil{return fmt.Errorf("session: source snapshot requires query")};pairs:=make([]string,0,4);for _,plan:=range plans{if plan==nil{return fmt.Errorf("session: source snapshot contains nil physical plan")};collectPhysicalScanFingerprints(plan,&pairs)};if len(pairs)==0{return nil};sort.Strings(pairs);h:=sha256.New();for _,pair:=range pairs{_,_=h.Write([]byte(pair));_,_=h.Write([]byte{0})};fingerprint:=hex.EncodeToString(h.Sum(nil));if err:=q.BindSourceSnapshot(query.SourceSnapshot{Fingerprint:fingerprint});err!=nil{return fmt.Errorf("session: bind statement source snapshot: %w",err)};return nil}
func collectPhysicalScanFingerprints(plan *planner.PhysicalPlan,out *[]string){if plan==nil{return};if plan.Kind==planner.PhysicalScan&&plan.Scan!=nil{*out=append(*out,plan.Scan.RelationID+"\x1f"+string(plan.Scan.Fingerprint))};for _,child:=range plan.Children{collectPhysicalScanFingerprints(child,out)}}
func AbortPrepared(runErr,cleanupErr error)error{return errors.Join(runErr,cleanupErr)}

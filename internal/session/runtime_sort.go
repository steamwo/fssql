package session

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/execution/blocking"
	execsort "github.com/steamwo/fssql/internal/execution/sort"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/sql/binder"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

type ExternalSortRuntimeConfig struct {
	MemoryBudget int64
	SpillBlockBytes int
	Compression storagetemp.Compression
}

type ExternalSortFactory struct {
	query *query.Context
	buffers *memory.BufferManager
	config ExternalSortRuntimeConfig
	topNRows uint64
}

func NewExternalSortFactory(q *query.Context,buffers *memory.BufferManager,config ExternalSortRuntimeConfig)(*ExternalSortFactory,error){
	if q==nil||q.Memory==nil||q.Temp==nil{return nil,fmt.Errorf("session: external SORT requires query memory and temporary storage scopes")}
	if buffers==nil{return nil,fmt.Errorf("session: external SORT requires BufferManager")}
	if config.MemoryBudget<=0{return nil,fmt.Errorf("session: external SORT memory budget must be positive")}
	if config.SpillBlockBytes<=0{return nil,fmt.Errorf("session: external SORT spill block bytes must be positive")}
	if config.Compression!=storagetemp.CompressionNone&&config.Compression!=storagetemp.CompressionRLE{return nil,fmt.Errorf("session: unsupported external SORT spill compression %d",config.Compression)}
	return &ExternalSortFactory{query:q,buffers:buffers,config:config},nil
}
func(f *ExternalSortFactory)setTopNRows(rows uint64){if f!=nil{f.topNRows=rows}}

func(f *ExternalSortFactory)OpenSort(ctx context.Context,columns []planner.Column,keys []planner.SortKey,estimate planner.CardinalityEstimate)(blocking.Operator,error){
	if f==nil||f.query==nil||f.query.Memory==nil||f.query.Temp==nil||f.buffers==nil{return nil,fmt.Errorf("session: external SORT factory is uninitialized")}
	if err:=ctx.Err();err!=nil{return nil,err};if f.query.Closed(){return nil,fmt.Errorf("session: external SORT query context is closed")}
	if spec,ok:=planner.IsPhase5SampleSort(keys);ok{return newPhase5SampleOperator(f.query,f.buffers,spec)}
	if key,ok:=planner.IsPhase5CTESort(keys);ok{return newPhase5CTEOperator(f,key)}
	if len(columns)==0||len(keys)==0{return nil,fmt.Errorf("session: SORT requires input columns and sort keys")};_ = estimate
	inputTypes,executionKeys,err:=prepareSortRuntime(columns,keys);if err!=nil{return nil,err}
	if f.topNRows>0&&f.topNRows<=uint64(math.MaxInt)&&fixedWidthSortInput(inputTypes){topN,err:=execsort.NewTopN(execsort.TopNConfig{InputTypes:inputTypes,Keys:executionKeys,N:int(f.topNRows),MemoryBudget:f.config.MemoryBudget,Scope:f.query.Memory,Buffers:f.buffers,PreallocateFixed:true});if err==nil{return topN,nil};if !errors.Is(err,memory.ErrLimitExceeded){return nil,err}}
	op,err:=execsort.New(execsort.Config{InputTypes:inputTypes,Keys:executionKeys,MemoryBudget:f.config.MemoryBudget,SpillBlockBytes:f.config.SpillBlockBytes,Scope:f.query.Memory,Buffers:f.buffers,Temp:f.query.Temp,Compression:f.config.Compression});if err!=nil{return nil,err};return op,nil
}

func prepareSortRuntime(columns []planner.Column,keys []planner.SortKey)([]types.LogicalType,[]execsort.Key,error){inputTypes:=make([]types.LogicalType,len(columns));for i,column:=range columns{if column.Type.ID==types.Invalid{return nil,nil,fmt.Errorf("session: SORT input column %d has invalid logical type",i)};inputTypes[i]=cloneSortLogicalType(column.Type)};executionKeys:=make([]execsort.Key,len(keys));for i,key:=range keys{if key.Expr==nil||key.Expr.Kind!=binder.BoundColumn{return nil,nil,fmt.Errorf("session: SORT key %d is not a materialized BoundColumn",i)};index:=key.Expr.ColumnIndex;if index<0||index>=len(columns){return nil,nil,fmt.Errorf("session: SORT key %d column index %d out of range",i,index)};nullsFirst:=false;if key.NullsFirst!=nil{nullsFirst=*key.NullsFirst};executionKeys[i]=execsort.Key{Column:index,Desc:key.Descending,NullsFirst:nullsFirst}};return inputTypes,executionKeys,nil}
func fixedWidthSortInput(inputTypes []types.LogicalType)bool{if len(inputTypes)==0{return false};for _,inputType:=range inputTypes{if _,ok:=inputType.FixedSize();!ok{return false}};return true}
func cloneSortLogicalType(value types.LogicalType)types.LogicalType{out:=value;if value.Child!=nil{child:=cloneSortLogicalType(*value.Child);out.Child=&child};if value.Fields!=nil{out.Fields=make([]types.StructField,len(value.Fields));for i,field:=range value.Fields{out.Fields[i]=types.StructField{Name:field.Name,Type:cloneSortLogicalType(field.Type)}}};return out}

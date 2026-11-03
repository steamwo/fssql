package expression

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

type phase5WindowState struct {
	mu sync.Mutex
	partition []ScalarValue
	rowNumber int64
	count int64
	haveSum bool
	intSum big.Int
	floatSum float64
}
var phase5WindowStateMu sync.Mutex
var phase5WindowStates = map[*Executor]map[*binder.BoundExpression]*phase5WindowState{}

func ReleasePhase5State(e *Executor){phase5WindowStateMu.Lock();delete(phase5WindowStates,e);phase5WindowStateMu.Unlock()}
func phase5State(e *Executor,expr *binder.BoundExpression)*phase5WindowState{phase5WindowStateMu.Lock();defer phase5WindowStateMu.Unlock();m:=phase5WindowStates[e];if m==nil{m=map[*binder.BoundExpression]*phase5WindowState{};phase5WindowStates[e]=m};s:=m[expr];if s==nil{s=&phase5WindowState{};m[expr]=s};return s}

type runtimeWindowSpec struct{partition,order int;frame string}
func runtimeWindowSpecOf(expr *binder.BoundExpression)(runtimeWindowSpec,error){if expr==nil||len(expr.Children)<1||expr.Children[0].Kind!=binder.BoundLiteral{return runtimeWindowSpec{},fmt.Errorf("phase5: malformed window spec")};var s runtimeWindowSpec;for _,part:=range strings.Split(expr.Children[0].Literal,";"){kv:=strings.SplitN(part,"=",2);if len(kv)!=2{continue};switch kv[0]{case"p":s.partition,_=strconv.Atoi(kv[1]);case"o":s.order,_=strconv.Atoi(kv[1]);case"f":s.frame=kv[1]}};if s.partition<0||s.order<0{return s,fmt.Errorf("phase5: invalid window spec")};return s,nil}

func(e *Executor)executePhase5Window(expr *binder.BoundExpression,chunk *vector.DataChunk)(*vector.Vector,error){
	spec,err:=runtimeWindowSpecOf(expr);if err!=nil{return nil,err};state:=phase5State(e,expr);state.mu.Lock();defer state.mu.Unlock()
	valueOffset:=1;if expr.FunctionID==function.WindowSumID{valueOffset=2};if len(expr.Children)<valueOffset+spec.partition+spec.order{return nil,fmt.Errorf("phase5: incomplete window arguments")}
	parts:=make([]*vector.Vector,spec.partition);for i:=0;i<spec.partition;i++{v,er:=e.ExecuteSQL(expr.Children[valueOffset+i],chunk);if er!=nil{releaseScalarVectors(parts);return nil,er};parts[i]=v};defer releaseScalarVectors(parts)
	var values *vector.Vector;if expr.FunctionID==function.WindowSumID{values,err=e.ExecuteSQL(expr.Children[1],chunk);if err!=nil{return nil,err};defer values.Release()}
	out,err:=vector.NewFlat(e.Scope,e.Buffers,expr.Type,chunk.Cardinality);if err!=nil{return nil,err}
	for row:=0;row<chunk.Cardinality;row++{
		key,er:=phase5PartitionKey(parts,row);if er!=nil{out.Release();return nil,er};if !phase5SamePartition(state.partition,key){state.partition=cloneScalarValues(key);state.rowNumber=0;state.count=0;state.haveSum=false;state.intSum.SetInt64(0);state.floatSum=0}
		switch expr.FunctionID{
		case function.WindowRowNumberID:
			state.rowNumber++;out.SetInt64(row,state.rowNumber)
		case function.WindowCountID:
			if spec.frame!="prefix1"{out.Release();return nil,fmt.Errorf("phase5: COUNT window must be rewritten or use supported prefix frame")};out.SetInt64(row,state.count);state.count++
		case function.WindowSumID:
			if spec.frame!="prefix1"{out.Release();return nil,fmt.Errorf("phase5: SUM window must be rewritten or use supported prefix frame")};if !state.haveSum{out.SetNull(row)}else if err:=writePhase5Accumulator(out,row,expr.Type,state);err!=nil{out.Release();return nil,err};if values.IsValid(row){if err:=addPhase5Accumulator(state,values,row);err!=nil{out.Release();return nil,err}}
		default:out.Release();return nil,fmt.Errorf("phase5: unknown window function %d",expr.FunctionID)
		}
	}
	return out,nil
}

func phase5PartitionKey(vs []*vector.Vector,row int)([]ScalarValue,error){out:=make([]ScalarValue,len(vs));for i,v:=range vs{x,err:=scalarFromVector(v,row);if err!=nil{return nil,err};if x.Bytes!=nil{x.Bytes=append([]byte(nil),x.Bytes...)};out[i]=x};return out,nil}
func cloneScalarValues(in []ScalarValue)[]ScalarValue{out:=append([]ScalarValue(nil),in...);for i:=range out{if out[i].Bytes!=nil{out[i].Bytes=append([]byte(nil),out[i].Bytes...)}};return out}
func phase5SamePartition(a,b []ScalarValue)bool{if len(a)!=len(b){return false};for i:=range a{if a[i].Valid!=b[i].Valid{return false};if !a[i].Valid{continue};c,err:=compareScalar(a[i],b[i]);if err!=nil||c!=0{return false}};return true}
func phase5Int128Big(v types.Int128)*big.Int{hi:=new(big.Int).SetInt64(v.Hi);hi.Lsh(hi,64);return hi.Add(hi,new(big.Int).SetUint64(v.Lo))}

func addPhase5Accumulator(s *phase5WindowState,v *vector.Vector,row int)error{x,err:=scalarFromVector(v,row);if err!=nil{return err};if !x.Valid{return nil};s.haveSum=true;if v.Type.IsUnsignedInteger(){s.intSum.Add(&s.intSum,new(big.Int).SetUint64(x.Uint64));return nil};switch v.Type.ID{case types.TinyInt,types.SmallInt,types.Integer,types.BigInt:s.intSum.Add(&s.intSum,big.NewInt(x.Int64));case types.HugeInt,types.Decimal:s.intSum.Add(&s.intSum,phase5Int128Big(x.Int128));case types.Float,types.Double:s.floatSum+=x.Float64;default:return fmt.Errorf("phase5: SUM window unsupported type %d",v.Type.ID)};return nil}
func writePhase5Accumulator(dst *vector.Vector,row int,t types.LogicalType,s *phase5WindowState)error{switch t.ID{case types.Double:dst.SetFloat64(row,s.floatSum);return nil;case types.HugeInt:v,ok:=types.Int128FromBig(&s.intSum);if !ok{return fmt.Errorf("phase5: SUM window overflow")};dst.SetInt128(row,v);return nil;case types.Decimal:v,ok:=types.Int128FromBig(&s.intSum);if !ok{return fmt.Errorf("phase5: SUM window decimal overflow")};if t.Decimal.Width<=18{b:=phase5Int128Big(v);if !b.IsInt64(){return fmt.Errorf("phase5: SUM window decimal overflow")};dst.SetInt64(row,b.Int64())}else{dst.SetInt128(row,v)};return nil;default:return fmt.Errorf("phase5: SUM window result type %d unsupported",t.ID)}}

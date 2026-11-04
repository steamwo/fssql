package aggregate

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	exechash "github.com/steamwo/fssql/internal/execution/hash"
	"github.com/steamwo/fssql/internal/semantics"
)

type Kind uint8

const (
	CountStar Kind = iota
	Count
	Sum
	Avg
	Min
	Max
)

type Spec struct {
	Kind  Kind
	Input int
}

type BoundSpec struct {
	Spec       Spec
	InputType  types.LogicalType
	OutputType types.LogicalType
}

func Bind(inputTypes []types.LogicalType, specs []Spec) ([]BoundSpec, error) {
	out := make([]BoundSpec, len(specs))
	for i, s := range specs {
		b := BoundSpec{Spec: s}
		if s.Kind != CountStar {
			if s.Input < 0 || s.Input >= len(inputTypes) { return nil, fmt.Errorf("aggregate input %d out of range", s.Input) }
			b.InputType = inputTypes[s.Input]
		}
		switch s.Kind {
		case CountStar, Count: b.OutputType = types.LogicalType{ID: types.BigInt}
		case Sum:
			switch b.InputType.ID {
			case types.TinyInt, types.SmallInt, types.Integer, types.BigInt, types.HugeInt: b.OutputType = types.LogicalType{ID: types.HugeInt}
			case types.Float, types.Double: b.OutputType = types.LogicalType{ID: types.Double}
			case types.Decimal: d, err := types.NewDecimal(38, b.InputType.Decimal.Scale); if err != nil { return nil, err }; b.OutputType = d
			default: return nil, fmt.Errorf("SUM unsupported type %v", b.InputType.ID)
			}
		case Avg:
			if !b.InputType.IsNumeric() { return nil, fmt.Errorf("AVG unsupported type %v", b.InputType.ID) }
			b.OutputType = types.LogicalType{ID: types.Double}
		case Min, Max:
			switch b.InputType.ID {
			case types.Boolean, types.TinyInt, types.SmallInt, types.Integer, types.BigInt, types.HugeInt, types.Float, types.Double, types.Decimal, types.Varchar, types.DateType, types.TimestampType: b.OutputType = b.InputType
			default: return nil, fmt.Errorf("MIN/MAX unsupported type %v", b.InputType.ID)
			}
		default: return nil, fmt.Errorf("unknown aggregate kind %d", s.Kind)
		}
		out[i] = b
	}
	return out, nil
}

const aggregateStateWidth = 40
const groupSlotWidth = 16

type groupTable struct { scope *memory.Scope; bm *memory.BufferManager; buf *memory.Buffer; capacity, count int }
func newGroupTable(scope *memory.Scope, bm *memory.BufferManager, capacity int) (*groupTable,error) { if capacity<8{capacity=8};c:=1;for c<capacity{c<<=1};b,err:=bm.Allocate(scope,memory.TagAggregate,c*groupSlotWidth);if err!=nil{return nil,err};return &groupTable{scope:scope,bm:bm,buf:b,capacity:c},nil }
func(t *groupTable)slot(i int)[]byte{return t.buf.Bytes()[i*groupSlotWidth:(i+1)*groupSlotWidth]}
func(t *groupTable)lookup(hash uint64,eq func(int)(bool,error))(int,bool,error){mask:=t.capacity-1;buf:=t.buf.Bytes();for probe:=0;probe<t.capacity;probe++{idx:=(int(hash)+probe)&mask;s:=buf[idx*groupSlotWidth:(idx+1)*groupSlotWidth];if s[0]==0{return 0,false,nil};if binary.LittleEndian.Uint64(s[4:12])==hash{row:=int(binary.LittleEndian.Uint32(s[12:16]));ok,err:=eq(row);if err!=nil{return 0,false,err};if ok{return row,true,nil}}};return 0,false,nil}
func(t *groupTable)putNoGrow(hash uint64,row int)error{mask:=t.capacity-1;for p:=0;p<t.capacity;p++{i:=(int(hash)+p)&mask;s:=t.slot(i);if s[0]==0{s[0]=1;binary.LittleEndian.PutUint64(s[4:12],hash);binary.LittleEndian.PutUint32(s[12:16],uint32(row));t.count++;return nil}};return errors.New("aggregate hash table full")}
func(t *groupTable)grow()error{n,err:=newGroupTable(t.scope,t.bm,t.capacity*2);if err!=nil{return err};for i:=0;i<t.capacity;i++{s:=t.slot(i);if s[0]!=0{if err:=n.putNoGrow(binary.LittleEndian.Uint64(s[4:12]),int(binary.LittleEndian.Uint32(s[12:16])));err!=nil{n.Release();return err}}};old:=t.buf;t.buf=n.buf;t.capacity=n.capacity;t.count=n.count;n.buf=nil;old.Release();return nil}
func(t *groupTable)ensureInsert()error{if(t.count+1)*10>=t.capacity*7{return t.grow()};return nil}
func(t *groupTable)Release(){if t!=nil&&t.buf!=nil{t.buf.Release();t.buf=nil}}

type HashAggregate struct { scope *memory.Scope; bm *memory.BufferManager; inputTypes []types.LogicalType; groupColumns []int; groupKeys []int; groupTypes []types.LogicalType; groupLayout exechash.RowLayout; specs []BoundSpec; layout AggregateLayout; groups *exechash.TupleDataCollection; table *groupTable; states *memory.Buffer; stateCapacity int; scratch *memory.Buffer; arena *memory.Buffer; arenaUsed int; finalized bool }
func NewHashAggregate(scope *memory.Scope,bm *memory.BufferManager,inputTypes []types.LogicalType,groupColumns []int,specs []Spec,estimatedGroups int)(*HashAggregate,error){
	if scope==nil||bm==nil{return nil,errors.New("aggregate requires memory scope and buffer manager")};gtypes:=make([]types.LogicalType,len(groupColumns));for i,c:=range groupColumns{if c<0||c>=len(inputTypes){return nil,fmt.Errorf("group column %d out of range",c)};gtypes[i]=inputTypes[c]};bound,err:=Bind(inputTypes,specs);if err!=nil{return nil,err};stateSpecs:=make([]StateSpec,len(bound));for i:=range bound{stateSpecs[i]=StateSpec{Name:fmt.Sprintf("agg_%d",i),Width:aggregateStateWidth,Alignment:8}};layout,err:=NewAggregateLayout(gtypes,stateSpecs);if err!=nil{return nil,err};groups,err:=exechash.NewTupleDataCollectionForLayout(scope,bm,layout.GroupLayout,max(estimatedGroups,1));if err!=nil{return nil,err};table,err:=newGroupTable(scope,bm,max(estimatedGroups*2,8));if err!=nil{groups.Release();return nil,err};a:=&HashAggregate{scope:scope,bm:bm,inputTypes:append([]types.LogicalType(nil),inputTypes...),groupColumns:append([]int(nil),groupColumns...),groupKeys:seq(len(groupColumns)),groupTypes:gtypes,groupLayout:layout.GroupLayout,specs:bound,layout:layout,groups:groups,table:table};if layout.StateWidth>0{cap:=max(estimatedGroups,8);a.states,err=bm.Allocate(scope,memory.TagAggregate,cap*layout.StateWidth);if err!=nil{a.Release();return nil,err};a.stateCapacity=cap};a.scratch,err=bm.Allocate(scope,memory.TagAggregate,max(layout.GroupLayout.FixedWidth,64));if err!=nil{a.Release();return nil,err};return a,nil
}
func max(a,b int)int{if a>b{return a};return b}
func(a *HashAggregate)GroupCount()int{if a==nil||a.groups==nil{return 0};return a.groups.Count()}
func(a *HashAggregate)ManagedBytes()int64{if a==nil{return 0};var n int64;if a.groups!=nil{n+=int64(a.groups.AllocatedBytes())};if a.table!=nil&&a.table.buf!=nil{n+=int64(a.table.buf.Len())};if a.states!=nil{n+=int64(a.states.Len())};if a.scratch!=nil{n+=int64(a.scratch.Len())};if a.arena!=nil{n+=int64(a.arena.Len())};return n}
func(a *HashAggregate)OutputTypes()[]types.LogicalType{out:=append([]types.LogicalType(nil),a.groupTypes...);for _,s:=range a.specs{out=append(out,s.OutputType)};return out}
func(a *HashAggregate)ensureStates(n int)error{if a.layout.StateWidth==0||n<=a.stateCapacity{return nil};cap:=a.stateCapacity*2;if cap<n{cap=n};if err:=a.states.Resize(cap*a.layout.StateWidth,nil);err!=nil{return err};a.stateCapacity=cap;return nil}
func(a *HashAggregate)state(group,agg int)[]byte{off:=group*a.layout.StateWidth+a.layout.States[agg].Offset;return a.states.Bytes()[off:off+aggregateStateWidth]}
func(a *HashAggregate)ensureScratch(n int)error{if a.scratch.Len()>=n{return nil};return a.scratch.Resize(n,nil)}
func(a *HashAggregate)ensureArena(n int)error{if n<=0{return nil};need:=a.arenaUsed+n;if a.arena==nil{cap:=max(256,need);b,err:=a.bm.Allocate(a.scope,memory.TagAggregate,cap);if err!=nil{return err};a.arena=b;return nil};if need<=a.arena.Len(){return nil};cap:=a.arena.Len()*2;if cap<need{cap=need};return a.arena.Resize(cap,nil)}
func(a *HashAggregate)projected(chunk *vector.DataChunk)*vector.DataChunk{cols:=make([]*vector.Vector,len(a.groupColumns));for i,c:=range a.groupColumns{cols[i]=chunk.Columns[c]};return &vector.DataChunk{Columns:cols,Cardinality:chunk.Cardinality}}
func(a *HashAggregate)findOrInsert(projected *vector.DataChunk,row int,hash uint64)(int,error){size,err:=a.groupLayout.RowSize(projected,row);if err!=nil{return 0,err};if err:=a.ensureScratch(max(size,1));err!=nil{return 0,err};enc:=a.scratch.Bytes()[:size];if _,err:=a.groupLayout.WriteRow(projected,row,enc);err!=nil{return 0,err};found,ok,err:=a.table.lookup(hash,func(existing int)(bool,error){return a.groupLayout.EqualRows(a.groups.Row(existing),enc,exechash.NullsEqual)});if err!=nil{return 0,err};if ok{return found,nil};if err:=a.table.ensureInsert();err!=nil{return 0,err};idx:=a.groups.Count();if err:=a.ensureStates(idx+1);err!=nil{return 0,err};if err:=a.groups.AppendChunkRow(projected,row);err!=nil{return 0,err};if err:=a.table.putNoGrow(hash,idx);err!=nil{return 0,err};return idx,nil}
func (a *HashAggregate) Consume(ctx context.Context, chunk *vector.DataChunk) error {
	if a.finalized {
		return errors.New("aggregate already finalized")
	}
	if chunk == nil {
		return errors.New("nil chunk")
	}
	if len(chunk.Columns) != len(a.inputTypes) {
		return errors.New("aggregate input column count mismatch")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(a.groupColumns) == 0 {
		return a.consumeGlobal(ctx, chunk)
	}
	projected := a.projected(chunk)
	var groupIDs [vector.DefaultVectorSize]int
	if handled, err := a.findDictionaryGroups(ctx, projected, groupIDs[:chunk.Cardinality]); handled || err != nil {
		if err != nil { return err }
		return a.updateGroupsBatch(ctx, chunk, groupIDs[:chunk.Cardinality])
	}
	var hashes [vector.DefaultVectorSize]uint64
	if err := exechash.HashChunk(projected, a.groupKeys, hashes[:chunk.Cardinality]); err != nil {
		return err
	}
	if err := a.findOrCreateGroups(ctx, projected, hashes[:chunk.Cardinality], groupIDs[:chunk.Cardinality]); err != nil {
		return err
	}
	return a.updateGroupsBatch(ctx, chunk, groupIDs[:chunk.Cardinality])
}

func seq(n int)[]int{r:=make([]int,n);for i:=range r{r[i]=i};return r}
func(a *HashAggregate)add128(st []byte,v types.Int128)error{cur:=types.Int128{Lo:binary.LittleEndian.Uint64(st[16:24]),Hi:int64(binary.LittleEndian.Uint64(st[24:32]))};next,err:=types.AddInt128(cur,v);if err!=nil{return semantics.ErrOverflow};binary.LittleEndian.PutUint64(st[16:24],next.Lo);binary.LittleEndian.PutUint64(st[24:32],uint64(next.Hi));st[0]=1;return nil}
func int128Input(v *vector.Vector,row int)(types.Int128,bool){
	if !v.IsValid(row){return types.Int128{},false}
	if v.Type.IsUnsignedInteger(){
		switch v.Type.ID{
		case types.TinyInt:x,_:=v.Uint8(row);return types.Int128{Lo:uint64(x)},true
		case types.SmallInt:x,_:=v.Uint16(row);return types.Int128{Lo:uint64(x)},true
		case types.Integer:x,_:=v.Uint32(row);return types.Int128{Lo:uint64(x)},true
		case types.BigInt:x,_:=v.Uint64(row);return types.Int128{Lo:x},true
		}
	}
	switch v.Type.ID{case types.TinyInt:x,_:=v.Int8(row);return types.Int128FromInt64(int64(x)),true;case types.SmallInt:x,_:=v.Int16(row);return types.Int128FromInt64(int64(x)),true;case types.Integer:x,_:=v.Int32(row);return types.Int128FromInt64(int64(x)),true;case types.BigInt:x,_:=v.Int64(row);return types.Int128FromInt64(x),true;case types.HugeInt:x,_:=v.Int128(row);return x,true;case types.Decimal:if v.Type.Decimal.Width<=18{x,_:=v.Int64(row);return types.Int128FromInt64(x),true};x,_:=v.Int128(row);return x,true};return types.Int128{},false
}
func unsignedVectorValue(v *vector.Vector,row int)(uint64,bool){
	if v==nil||!v.IsValid(row)||!v.Type.IsUnsignedInteger(){return 0,false}
	switch v.Type.ID{case types.TinyInt:x,_:=v.Uint8(row);return uint64(x),true;case types.SmallInt:x,_:=v.Uint16(row);return uint64(x),true;case types.Integer:x,_:=v.Uint32(row);return uint64(x),true;case types.BigInt:x,_:=v.Uint64(row);return x,true}
	return 0,false
}
func unsignedStateValue(t types.LogicalType,data []byte)uint64{
	switch t.ID{case types.TinyInt:return uint64(data[0]);case types.SmallInt:return uint64(binary.LittleEndian.Uint16(data));case types.Integer:return uint64(binary.LittleEndian.Uint32(data));case types.BigInt:return binary.LittleEndian.Uint64(data)}
	return 0
}
func floatInput(v *vector.Vector,row int)(float64,bool){if !v.IsValid(row){return 0,false};switch v.Type.ID{case types.Float:x,_:=v.Float32(row);return float64(x),true;case types.Double:x,_:=v.Float64(row);return x,true};return 0,false}
func (a *HashAggregate) update(g, ai int, chunk *vector.DataChunk, row int) error {
	return a.updateState(a.state(g, ai), a.specs[ai], chunk, row)
}
func (a *HashAggregate) updateState(st []byte, b BoundSpec, chunk *vector.DataChunk, row int) error {
	switch b.Spec.Kind {
	case CountStar:
		binary.LittleEndian.PutUint64(st[8:16], binary.LittleEndian.Uint64(st[8:16])+1)
		st[0] = 1
		return nil
	case Count:
		if chunk.Columns[b.Spec.Input].IsValid(row) {
			binary.LittleEndian.PutUint64(st[8:16], binary.LittleEndian.Uint64(st[8:16])+1)
			st[0] = 1
		}
		return nil
	case Sum, Avg:
		v := chunk.Columns[b.Spec.Input]
		if !v.IsValid(row) {
			return nil
		}
		if b.Spec.Kind == Avg {
			c := binary.LittleEndian.Uint64(st[8:16])
			if c == math.MaxUint64 {
				return semantics.ErrOverflow
			}
			binary.LittleEndian.PutUint64(st[8:16], c+1)
		}
		if b.InputType.ID == types.Float || b.InputType.ID == types.Double {
			x, _ := floatInput(v, row)
			cur := math.Float64frombits(binary.LittleEndian.Uint64(st[16:24]))
			binary.LittleEndian.PutUint64(st[16:24], math.Float64bits(cur+x))
			st[0] = 1
			return nil
		}
		x, ok := int128Input(v, row)
		if !ok {
			return errors.New("aggregate numeric input unsupported")
		}
		return a.add128(st, x)
	case Min, Max:
		return a.updateMinMax(st, b, chunk.Columns[b.Spec.Input], row)
	}
	return errors.New("unknown aggregate kind")
}

func(a *HashAggregate)updateMinMax(st []byte,b BoundSpec,v *vector.Vector,row int)error{if !v.IsValid(row){return nil};take:=st[0]==0;if !take{cmp,err:=a.compareStateValue(st,b.InputType,v,row);if err!=nil{return err};if b.Spec.Kind==Min{take=cmp>0}else{take=cmp<0}};if !take{return nil};st[0]=1;return a.storeStateValue(st,b.InputType,v,row)}
func(a *HashAggregate)storeStateValue(st []byte,t types.LogicalType,v *vector.Vector,row int)error{
	for i:=8;i<len(st);i++{st[i]=0}
	if t.IsUnsignedInteger(){x,ok:=unsignedVectorValue(v,row);if !ok{return errors.New("unsigned aggregate value unavailable")};switch t.ID{case types.TinyInt:st[16]=byte(x);case types.SmallInt:binary.LittleEndian.PutUint16(st[16:18],uint16(x));case types.Integer:binary.LittleEndian.PutUint32(st[16:20],uint32(x));case types.BigInt:binary.LittleEndian.PutUint64(st[16:24],x)};return nil}
	switch t.ID{case types.Boolean:x,_:=v.Bool(row);if x{st[16]=1};case types.TinyInt:x,_:=v.Int8(row);st[16]=byte(x);case types.SmallInt:x,_:=v.Int16(row);binary.LittleEndian.PutUint16(st[16:18],uint16(x));case types.Integer,types.DateType:x,_:=v.Int32(row);binary.LittleEndian.PutUint32(st[16:20],uint32(x));case types.BigInt,types.TimestampType:x,_:=v.Int64(row);binary.LittleEndian.PutUint64(st[16:24],uint64(x));case types.HugeInt:x,_:=v.Int128(row);binary.LittleEndian.PutUint64(st[16:24],x.Lo);binary.LittleEndian.PutUint64(st[24:32],uint64(x.Hi));case types.Float:x,_:=v.Float32(row);binary.LittleEndian.PutUint32(st[16:20],math.Float32bits(x));case types.Double:x,_:=v.Float64(row);binary.LittleEndian.PutUint64(st[16:24],math.Float64bits(x));case types.Decimal:if t.Decimal.Width<=18{x,_:=v.Int64(row);binary.LittleEndian.PutUint64(st[16:24],uint64(x))}else{x,_:=v.Int128(row);binary.LittleEndian.PutUint64(st[16:24],x.Lo);binary.LittleEndian.PutUint64(st[24:32],uint64(x.Hi))};case types.Varchar:r,h,_:=v.StringRefAt(row);var src []byte;if r.External{src=h.ExternalBytes(r)}else{src=r.Inline[:r.Length]};if err:=a.ensureArena(len(src));err!=nil{return err};off:=a.arenaUsed;copy(a.arena.Bytes()[off:off+len(src)],src);a.arenaUsed+=len(src);binary.LittleEndian.PutUint32(st[16:20],uint32(off));binary.LittleEndian.PutUint32(st[20:24],uint32(len(src)));default:return errors.New("min/max store unsupported type")};return nil
}
func(a *HashAggregate)compareStateValue(st []byte,t types.LogicalType,v *vector.Vector,row int)(int,error){
	if t.IsUnsignedInteger(){x,ok:=unsignedVectorValue(v,row);if !ok{return 0,errors.New("unsigned aggregate value unavailable")};return semantics.CompareUint64(unsignedStateValue(t,st[16:]),x),nil}
	switch t.ID{case types.Boolean:x,_:=v.Bool(row);sv:=st[16]!=0;if sv==x{return 0,nil};if !sv&&x{return -1,nil};return 1,nil;case types.TinyInt:x,_:=v.Int8(row);return semantics.CompareInt64(int64(int8(st[16])),int64(x)),nil;case types.SmallInt:x,_:=v.Int16(row);return semantics.CompareInt64(int64(int16(binary.LittleEndian.Uint16(st[16:18]))),int64(x)),nil;case types.Integer,types.DateType:x,_:=v.Int32(row);return semantics.CompareInt64(int64(int32(binary.LittleEndian.Uint32(st[16:20]))),int64(x)),nil;case types.BigInt,types.TimestampType:x,_:=v.Int64(row);return semantics.CompareInt64(int64(binary.LittleEndian.Uint64(st[16:24])),x),nil;case types.HugeInt:x,_:=v.Int128(row);return types.CompareInt128(types.Int128{Lo:binary.LittleEndian.Uint64(st[16:24]),Hi:int64(binary.LittleEndian.Uint64(st[24:32]))},x),nil;case types.Float:x,_:=v.Float32(row);return semantics.CompareFloat32(math.Float32frombits(binary.LittleEndian.Uint32(st[16:20])),x),nil;case types.Double:x,_:=v.Float64(row);return semantics.CompareFloat64(math.Float64frombits(binary.LittleEndian.Uint64(st[16:24])),x),nil;case types.Decimal:sv:=types.Int128FromInt64(int64(binary.LittleEndian.Uint64(st[16:24])));if t.Decimal.Width>18{sv=types.Int128{Lo:binary.LittleEndian.Uint64(st[16:24]),Hi:int64(binary.LittleEndian.Uint64(st[24:32]))}};x,_:=int128Input(v,row);return types.CompareInt128(sv,x),nil;case types.Varchar:off:=int(binary.LittleEndian.Uint32(st[16:20]));n:=int(binary.LittleEndian.Uint32(st[20:24]));r,h,_:=v.StringRefAt(row);var x []byte;if r.External{x=h.ExternalBytes(r)}else{x=r.Inline[:r.Length]};return bytes.Compare(a.arena.Bytes()[off:off+n],x),nil};return 0,errors.New("min/max compare unsupported type")
}
func(a *HashAggregate)Finalize(ctx context.Context)(*vector.DataChunk,error){if a.finalized{return nil,errors.New("aggregate already finalized")};if err:=ctx.Err();err!=nil{return nil,err};if a.groups.Count()==0&&len(a.groupColumns)==0{empty:=&vector.DataChunk{Cardinality:1};if err:=a.groups.AppendChunkRow(empty,0);err!=nil{return nil,err};if err:=a.ensureStates(1);err!=nil{return nil,err}};if a.groups.Count()>vector.DefaultVectorSize{return nil,errors.New("aggregate result exceeds one DataChunk; use Emit")};chunk,err:=a.makeOutputChunk(0,a.groups.Count());if err!=nil{return nil,err};a.finalized=true;return chunk,nil}
func(a *HashAggregate)finalizeState(dst *vector.Vector,row int,st []byte,b BoundSpec)error{switch b.Spec.Kind{case CountStar,Count:dst.SetInt64(row,int64(binary.LittleEndian.Uint64(st[8:16])));return nil;case Sum:if st[0]==0{dst.SetNull(row);return nil};if b.OutputType.ID==types.Double{dst.SetFloat64(row,math.Float64frombits(binary.LittleEndian.Uint64(st[16:24])))}else{x:=types.Int128{Lo:binary.LittleEndian.Uint64(st[16:24]),Hi:int64(binary.LittleEndian.Uint64(st[24:32]))};dst.SetInt128(row,x)};return nil;case Avg:if st[0]==0||binary.LittleEndian.Uint64(st[8:16])==0{dst.SetNull(row);return nil};count:=binary.LittleEndian.Uint64(st[8:16]);var total float64;if b.InputType.ID==types.Float||b.InputType.ID==types.Double{total=math.Float64frombits(binary.LittleEndian.Uint64(st[16:24]))}else{total=types.Int128ToFloat64(types.Int128{Lo:binary.LittleEndian.Uint64(st[16:24]),Hi:int64(binary.LittleEndian.Uint64(st[24:32]))});if b.InputType.ID==types.Decimal{total/=math.Pow10(int(b.InputType.Decimal.Scale))}};dst.SetFloat64(row,total/float64(count));return nil;case Min,Max:if st[0]==0{dst.SetNull(row);return nil};return a.loadStateValue(dst,row,st,b.OutputType)};return errors.New("finalize unsupported aggregate")}
func(a *HashAggregate)loadStateValue(dst *vector.Vector,row int,st []byte,t types.LogicalType)error{
	if t.IsUnsignedInteger(){x:=unsignedStateValue(t,st[16:]);switch t.ID{case types.TinyInt:dst.SetUint8(row,uint8(x));case types.SmallInt:dst.SetUint16(row,uint16(x));case types.Integer:dst.SetUint32(row,uint32(x));case types.BigInt:dst.SetUint64(row,x)};return nil}
	switch t.ID{case types.Boolean:dst.SetBool(row,st[16]!=0);case types.TinyInt:dst.SetInt8(row,int8(st[16]));case types.SmallInt:dst.SetInt16(row,int16(binary.LittleEndian.Uint16(st[16:18])));case types.Integer,types.DateType:dst.SetInt32(row,int32(binary.LittleEndian.Uint32(st[16:20])));case types.BigInt,types.TimestampType:dst.SetInt64(row,int64(binary.LittleEndian.Uint64(st[16:24])));case types.HugeInt:dst.SetInt128(row,types.Int128{Lo:binary.LittleEndian.Uint64(st[16:24]),Hi:int64(binary.LittleEndian.Uint64(st[24:32]))});case types.Float:dst.SetFloat32(row,math.Float32frombits(binary.LittleEndian.Uint32(st[16:20])));case types.Double:dst.SetFloat64(row,math.Float64frombits(binary.LittleEndian.Uint64(st[16:24])));case types.Decimal:if t.Decimal.Width<=18{dst.SetInt64(row,int64(binary.LittleEndian.Uint64(st[16:24])))}else{dst.SetInt128(row,types.Int128{Lo:binary.LittleEndian.Uint64(st[16:24]),Hi:int64(binary.LittleEndian.Uint64(st[24:32]))})};case types.Varchar:off:=int(binary.LittleEndian.Uint32(st[16:20]));n:=int(binary.LittleEndian.Uint32(st[20:24]));return dst.SetStringBytes(row,a.arena.Bytes()[off:off+n]);default:return errors.New("load state unsupported type")};return nil
}
func sameLogicalType(a,b types.LogicalType)bool{return a.Same(b)}
func(a *HashAggregate)makeOutputChunk(start,count int)(*vector.DataChunk,error){if start<0||count<0||start>a.groups.Count()||count>a.groups.Count()-start{return nil,errors.New("aggregate output range out of bounds")};if count>vector.DefaultVectorSize{return nil,errors.New("aggregate output chunk exceeds vector size")};typesOut:=a.OutputTypes();cols:=make([]*vector.Vector,len(typesOut));cleanup:=func(){for _,v:=range cols{if v!=nil{v.Release()}}};for i,typ:=range typesOut{v,err:=vector.NewFlat(a.scope,a.bm,typ,count);if err!=nil{cleanup();return nil,err};cols[i]=v};for outRow:=0;outRow<count;outRow++{g:=start+outRow;if len(a.groupTypes)!=0{if err:=a.groupLayout.MaterializeRow(a.groups.Row(g),cols[:len(a.groupTypes)],outRow);err!=nil{cleanup();return nil,err}};for ai,spec:=range a.specs{if err:=a.finalizeState(cols[len(a.groupTypes)+ai],outRow,a.state(g,ai),spec);err!=nil{cleanup();return nil,err}}};chunk,err:=vector.NewDataChunk(cols...);if err!=nil{cleanup();return nil,err};return chunk,nil}
func(a *HashAggregate)Emit(ctx context.Context,emit func(*vector.DataChunk)error)error{if a.finalized{return errors.New("aggregate already finalized")};if emit==nil{return errors.New("aggregate emit callback is required")};if err:=ctx.Err();err!=nil{return err};if a.groups.Count()==0&&len(a.groupColumns)==0{empty:=&vector.DataChunk{Cardinality:1};if err:=a.groups.AppendChunkRow(empty,0);err!=nil{return err};if err:=a.ensureStates(1);err!=nil{return err}};for start:=0;start<a.groups.Count();start+=vector.DefaultVectorSize{if err:=ctx.Err();err!=nil{return err};n:=a.groups.Count()-start;if n>vector.DefaultVectorSize{n=vector.DefaultVectorSize};chunk,err:=a.makeOutputChunk(start,n);if err!=nil{return err};if err:=emit(chunk);err!=nil{chunk.Release();return err}};a.finalized=true;return nil}
func(a *HashAggregate)findOrInsertEncoded(row []byte,hashv uint64)(int,error){found,ok,err:=a.table.lookup(hashv,func(existing int)(bool,error){return a.groupLayout.EqualRows(a.groups.Row(existing),row,exechash.NullsEqual)});if err!=nil{return 0,err};if ok{return found,nil};if err:=a.table.ensureInsert();err!=nil{return 0,err};idx:=a.groups.Count();if err:=a.ensureStates(idx+1);err!=nil{return 0,err};if err:=a.groups.AppendEncodedRow(row);err!=nil{return 0,err};if err:=a.table.putNoGrow(hashv,idx);err!=nil{return 0,err};return idx,nil}
func checkedAddCount(a,b uint64)(uint64,error){if b>uint64(math.MaxInt64)||a>uint64(math.MaxInt64)-b{return 0,semantics.ErrOverflow};return a+b,nil}
func(a *HashAggregate)copyStateValue(dst []byte,srcAgg *HashAggregate,src []byte,typ types.LogicalType)error{for i:=range dst{dst[i]=0};copy(dst[:16],src[:16]);if typ.ID!=types.Varchar{copy(dst[16:],src[16:]);return nil};off:=int(binary.LittleEndian.Uint32(src[16:20]));n:=int(binary.LittleEndian.Uint32(src[20:24]));if n<0||srcAgg.arena==nil||off<0||off>srcAgg.arenaUsed||n>srcAgg.arenaUsed-off{return errors.New("invalid aggregate varchar state")};if err:=a.ensureArena(n);err!=nil{return err};newOff:=a.arenaUsed;copy(a.arena.Bytes()[newOff:newOff+n],srcAgg.arena.Bytes()[off:off+n]);a.arenaUsed+=n;binary.LittleEndian.PutUint32(dst[16:20],uint32(newOff));binary.LittleEndian.PutUint32(dst[20:24],uint32(n));return nil}
func(a *HashAggregate)compareStates(dst []byte,dstAgg *HashAggregate,src []byte,srcAgg *HashAggregate,typ types.LogicalType)(int,error){
	if typ.IsUnsignedInteger(){return semantics.CompareUint64(unsignedStateValue(typ,dst[16:]),unsignedStateValue(typ,src[16:])),nil}
	switch typ.ID{case types.Boolean:d,s:=dst[16]!=0,src[16]!=0;if d==s{return 0,nil};if !d{return -1,nil};return 1,nil;case types.TinyInt:return semantics.CompareInt64(int64(int8(dst[16])),int64(int8(src[16]))),nil;case types.SmallInt:return semantics.CompareInt64(int64(int16(binary.LittleEndian.Uint16(dst[16:18]))),int64(int16(binary.LittleEndian.Uint16(src[16:18])))),nil;case types.Integer,types.DateType:return semantics.CompareInt64(int64(int32(binary.LittleEndian.Uint32(dst[16:20]))),int64(int32(binary.LittleEndian.Uint32(src[16:20])))),nil;case types.BigInt,types.TimestampType:return semantics.CompareInt64(int64(binary.LittleEndian.Uint64(dst[16:24])),int64(binary.LittleEndian.Uint64(src[16:24]))),nil;case types.HugeInt:return types.CompareInt128(types.Int128{Lo:binary.LittleEndian.Uint64(dst[16:24]),Hi:int64(binary.LittleEndian.Uint64(dst[24:32]))},types.Int128{Lo:binary.LittleEndian.Uint64(src[16:24]),Hi:int64(binary.LittleEndian.Uint64(src[24:32]))}),nil;case types.Float:return semantics.CompareFloat32(math.Float32frombits(binary.LittleEndian.Uint32(dst[16:20])),math.Float32frombits(binary.LittleEndian.Uint32(src[16:20]))),nil;case types.Double:return semantics.CompareFloat64(math.Float64frombits(binary.LittleEndian.Uint64(dst[16:24])),math.Float64frombits(binary.LittleEndian.Uint64(src[16:24]))),nil;case types.Decimal:d:=types.Int128FromInt64(int64(binary.LittleEndian.Uint64(dst[16:24])));s:=types.Int128FromInt64(int64(binary.LittleEndian.Uint64(src[16:24])));if typ.Decimal.Width>18{d=types.Int128{Lo:binary.LittleEndian.Uint64(dst[16:24]),Hi:int64(binary.LittleEndian.Uint64(dst[24:32]))};s=types.Int128{Lo:binary.LittleEndian.Uint64(src[16:24]),Hi:int64(binary.LittleEndian.Uint64(src[24:32]))}};return types.CompareInt128(d,s),nil;case types.Varchar:do,dn:=int(binary.LittleEndian.Uint32(dst[16:20])),int(binary.LittleEndian.Uint32(dst[20:24]));so,sn:=int(binary.LittleEndian.Uint32(src[16:20])),int(binary.LittleEndian.Uint32(src[20:24]));if dstAgg.arena==nil||srcAgg.arena==nil||do<0||so<0||dn<0||sn<0||do>dstAgg.arenaUsed||dn>dstAgg.arenaUsed-do||so>srcAgg.arenaUsed||sn>srcAgg.arenaUsed-so{return 0,errors.New("invalid aggregate varchar state")};return bytes.Compare(dstAgg.arena.Bytes()[do:do+dn],srcAgg.arena.Bytes()[so:so+sn]),nil;default:return 0,errors.New("aggregate state compare unsupported type")}
}
func(a *HashAggregate)combineState(dst []byte,srcAgg *HashAggregate,src []byte,spec BoundSpec)error{switch spec.Spec.Kind{case CountStar,Count:n,err:=checkedAddCount(binary.LittleEndian.Uint64(dst[8:16]),binary.LittleEndian.Uint64(src[8:16]));if err!=nil{return err};binary.LittleEndian.PutUint64(dst[8:16],n);if n!=0{dst[0]=1};return nil;case Sum,Avg:if src[0]==0{return nil};if spec.Spec.Kind==Avg{n,err:=checkedAddCount(binary.LittleEndian.Uint64(dst[8:16]),binary.LittleEndian.Uint64(src[8:16]));if err!=nil{return err};binary.LittleEndian.PutUint64(dst[8:16],n)};if spec.InputType.ID==types.Float||spec.InputType.ID==types.Double{d:=math.Float64frombits(binary.LittleEndian.Uint64(dst[16:24]));s:=math.Float64frombits(binary.LittleEndian.Uint64(src[16:24]));binary.LittleEndian.PutUint64(dst[16:24],math.Float64bits(d+s));dst[0]=1;return nil};d:=types.Int128{Lo:binary.LittleEndian.Uint64(dst[16:24]),Hi:int64(binary.LittleEndian.Uint64(dst[24:32]))};sv:=types.Int128{Lo:binary.LittleEndian.Uint64(src[16:24]),Hi:int64(binary.LittleEndian.Uint64(src[24:32]))};n,err:=types.AddInt128(d,sv);if err!=nil{return semantics.ErrOverflow};binary.LittleEndian.PutUint64(dst[16:24],n.Lo);binary.LittleEndian.PutUint64(dst[24:32],uint64(n.Hi));dst[0]=1;return nil;case Min,Max:if src[0]==0{return nil};if dst[0]==0{return a.copyStateValue(dst,srcAgg,src,spec.InputType)};cmp,err:=a.compareStates(dst,a,src,srcAgg,spec.InputType);if err!=nil{return err};take:=(spec.Spec.Kind==Min&&cmp>0)||(spec.Spec.Kind==Max&&cmp<0);if take{return a.copyStateValue(dst,srcAgg,src,spec.InputType)};return nil;default:return errors.New("combine unsupported aggregate")}}
func(a *HashAggregate)CombineStates(ctx context.Context,other *HashAggregate)error{if a.finalized||other==nil||other.finalized{return errors.New("aggregate combine requires live states")};if len(a.groupTypes)!=len(other.groupTypes)||len(a.specs)!=len(other.specs){return errors.New("aggregate combine schema mismatch")};for i:=range a.groupTypes{if !sameLogicalType(a.groupTypes[i],other.groupTypes[i]){return errors.New("aggregate combine group type mismatch")}};for i:=range a.specs{if a.specs[i].Spec!=other.specs[i].Spec||!sameLogicalType(a.specs[i].InputType,other.specs[i].InputType)||!sameLogicalType(a.specs[i].OutputType,other.specs[i].OutputType){return errors.New("aggregate combine spec mismatch")}};for sg:=0;sg<other.groups.Count();sg++{if err:=ctx.Err();err!=nil{return err};row:=other.groups.Row(sg);h,err:=a.groupLayout.HashRow(row);if err!=nil{return err};dg,err:=a.findOrInsertEncoded(row,h);if err!=nil{return err};for ai,spec:=range a.specs{if err:=a.combineState(a.state(dg,ai),other,other.state(sg,ai),spec);err!=nil{return err}}};return nil}
func(a *HashAggregate)Merge(ctx context.Context,partial *vector.DataChunk)error{return errors.New("Merge requires aggregate-state input; use CombineStates")}
func(a *HashAggregate)Release(){if a.groups!=nil{a.groups.Release();a.groups=nil};if a.table!=nil{a.table.Release();a.table=nil};if a.states!=nil{a.states.Release();a.states=nil};if a.scratch!=nil{a.scratch.Release();a.scratch=nil};if a.arena!=nil{a.arena.Release();a.arena=nil}}

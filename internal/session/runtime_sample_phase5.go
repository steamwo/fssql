package session

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	mathrand "math/rand"
	"strconv"
	"strings"

	"context"
	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/blocking"
	"github.com/steamwo/fssql/internal/query"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

type phase5SampleSpec struct{ method byte; amount uint64; seeded bool; seed uint64 }
func parsePhase5SampleSpec(raw string)(phase5SampleSpec,error){parts:=strings.Split(raw,":");if len(parts)!=5||parts[0]!="sample"||(parts[1]!="r"&&parts[1]!="b"&&parts[1]!="s"&&parts[1]!="p"){return phase5SampleSpec{},fmt.Errorf("session: invalid phase5 sample spec %q",raw)};amount,err:=strconv.ParseUint(parts[2],16,64);if err!=nil{return phase5SampleSpec{},err};seed,err:=strconv.ParseUint(parts[4],16,64);if err!=nil{return phase5SampleSpec{},err};if parts[3]!="s"&&parts[3]!="n"{return phase5SampleSpec{},fmt.Errorf("session: invalid phase5 sample seed mode")};return phase5SampleSpec{method:parts[1][0],amount:amount,seeded:parts[3]=="s",seed:seed},nil}

type phase5SampleOperator struct{
	q *query.Context; buffers *memory.BufferManager; store *storagetemp.SpillablePageStore; rng *mathrand.Rand; spec phase5SampleSpec
	seen uint64; pages []storagetemp.PageID
}
var _ blocking.Operator=(*phase5SampleOperator)(nil)

func newPhase5SampleOperator(q *query.Context,buffers *memory.BufferManager,raw string)(blocking.Operator,error){spec,err:=parsePhase5SampleSpec(raw);if err!=nil{return nil,err};seed:=spec.seed;if !spec.seeded{var b [8]byte;if _,err:=cryptorand.Read(b[:]);err!=nil{return nil,err};seed=binary.LittleEndian.Uint64(b[:])};store,err:=storagetemp.NewSpillablePageStore(q.Temp,q.Memory,buffers,storagetemp.PageStoreConfig{SegmentMaxBytes:64<<20,Compression:storagetemp.CompressionNone});if err!=nil{return nil,err};return &phase5SampleOperator{q:q,buffers:buffers,store:store,rng:mathrand.New(mathrand.NewSource(int64(seed))),spec:spec},nil}

func(s *phase5SampleOperator)Consume(ctx context.Context,chunk *vector.DataChunk)error{if err:=ctx.Err();err!=nil{return err};if chunk==nil{return fmt.Errorf("session: sample cannot consume nil chunk")};switch s.spec.method{case'b':return s.consumeBernoulli(ctx,chunk);case'r':return s.consumeReservoir(ctx,chunk);case's':p:=math.Float64frombits(s.spec.amount)/100;if p>=1||(p>0&&s.rng.Float64()<p){return s.appendChunk(ctx,chunk)};return nil;case'p':if math.Float64frombits(s.spec.amount)<=0{return nil};s.seen+=uint64(chunk.Cardinality);return s.appendChunk(ctx,chunk);default:return fmt.Errorf("session: unknown sample method")}}
func(s *phase5SampleOperator)consumeBernoulli(ctx context.Context,chunk *vector.DataChunk)error{p:=math.Float64frombits(s.spec.amount)/100;if p<=0{return nil};if p>=1{return s.appendChunk(ctx,chunk)};idx:=make([]int,0,chunk.Cardinality);for row:=0;row<chunk.Cardinality;row++{if p>=1||s.rng.Float64()<p{idx=append(idx,row)}};if len(idx)==0{return nil};copyChunk,err:=phase5CopyRows(s.q.Memory,s.buffers,chunk,idx);if err!=nil{return err};defer copyChunk.Release();return s.appendChunk(ctx,copyChunk)}
func(s *phase5SampleOperator)consumeReservoir(ctx context.Context,chunk *vector.DataChunk)error{k:=s.spec.amount;if k==0{return nil};for row:=0;row<chunk.Cardinality;row++{s.seen++;slot:=uint64(len(s.pages));if slot>=k{slot=uint64(s.rng.Int63n(int64(s.seen)));if slot>=k{continue}};one,err:=phase5CopyRows(s.q.Memory,s.buffers,chunk,[]int{row});if err!=nil{return err};encoded,err:=vector.SerializeChunk(one,s.q.Memory,s.buffers);one.Release();if err!=nil{return err};page,err:=s.store.WritePage(ctx,encoded.Bytes());encoded.Release();if err!=nil{return err};if uint64(len(s.pages))<k{s.pages=append(s.pages,page)}else{old:=s.pages[slot];s.pages[slot]=page;if err:=s.store.DeletePage(old);err!=nil{return err}}};return nil}
func(s *phase5SampleOperator)appendChunk(ctx context.Context,chunk *vector.DataChunk)error{encoded,err:=vector.SerializeChunk(chunk,s.q.Memory,s.buffers);if err!=nil{return err};defer encoded.Release();page,err:=s.store.WritePage(ctx,encoded.Bytes());if err!=nil{return err};s.pages=append(s.pages,page);return nil}
func(s *phase5SampleOperator)Emit(ctx context.Context,emit func(*vector.DataChunk)error)error{if s.spec.method=='p'&&math.Float64frombits(s.spec.amount)<100{return s.emitReservoirPercent(ctx,emit)};for _,id:=range s.pages{if err:=ctx.Err();err!=nil{return err};page,err:=s.store.ReadPage(ctx,id);if err!=nil{return err};chunk,err:=vector.DeserializeChunk(page.Bytes(),s.q.Memory,s.buffers);page.Release();if err!=nil{return err};if err:=emit(chunk);err!=nil{chunk.Release();return err}};return nil}
func(s *phase5SampleOperator)Release(){if s==nil{return};if s.store!=nil{_ = s.store.Close();s.store=nil};s.pages=nil}

func phase5CopyRows(scope *memory.Scope,buffers *memory.BufferManager,src *vector.DataChunk,rows []int)(*vector.DataChunk,error){cols:=make([]*vector.Vector,len(src.Columns));for c,in:=range src.Columns{out,err:=vector.NewFlat(scope,buffers,in.Type,len(rows));if err!=nil{for _,v:=range cols{if v!=nil{v.Release()}};return nil,err};for i,row:=range rows{if err:=vector.CopyValue(in,row,out,i);err!=nil{out.Release();for _,v:=range cols{if v!=nil{v.Release()}};return nil,err}};cols[c]=out};chunk,err:=vector.NewDataChunk(cols...);if err!=nil{for _,v:=range cols{if v!=nil{v.Release()}};return nil,err};if len(cols)==0{chunk.Cardinality=len(rows)};return chunk,nil}

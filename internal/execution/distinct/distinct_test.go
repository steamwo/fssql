package distinct

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

func denv(t testing.TB) (*memory.Manager, *memory.Scope, *memory.BufferManager, *storagetemp.TemporaryMemoryManager, *storagetemp.QueryScope, func()) {
	t.Helper()
	mm := memory.NewManager(64 << 20)
	s := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	q := storagetemp.NewTemporaryMemoryManager(storagetemp.Limits{Global: 64 << 20, DefaultResourceGroup: 64 << 20, DefaultQuery: 64 << 20})
	sm, err := storagetemp.NewTemporaryStorageManager(t.TempDir(), q)
	if err != nil { t.Fatal(err) }
	ts, err := sm.NewQueryScope("default")
	if err != nil { t.Fatal(err) }
	return mm, s, bm, q, ts, func() {
		if err := ts.Close(); err != nil { t.Fatal(err) }
		if err := s.Close(); err != nil { t.Fatal(err) }
		if mm.Used() != 0 { t.Fatalf("memory leak=%d", mm.Used()) }
		if q.GlobalUsed() != 0 { t.Fatalf("temp leak=%d", q.GlobalUsed()) }
	}
}
func chunk(t testing.TB, s *memory.Scope, bm *memory.BufferManager, start, n int) *vector.DataChunk {
	t.Helper(); iv, _ := vector.NewFlat(s, bm, types.LogicalType{ID: types.BigInt}, n); sv, _ := vector.NewFlat(s, bm, types.LogicalType{ID: types.Varchar}, n)
	for i := 0; i < n; i++ { x := (start+i)%73; if x%11==0 { iv.SetNull(i) } else { iv.SetInt64(i,int64(x%17)) }; if x%13==0 { sv.SetNull(i) } else { _=sv.SetString(i,fmt.Sprintf("v%02d",x%19)) } }
	c,_:=vector.NewDataChunk(iv,sv); return c
}
func uniqueChunk(t testing.TB,s *memory.Scope,bm *memory.BufferManager,start,n int)*vector.DataChunk{t.Helper();iv,_:=vector.NewFlat(s,bm,types.LogicalType{ID:types.BigInt},n);sv,_:=vector.NewFlat(s,bm,types.LogicalType{ID:types.Varchar},n);for i:=0;i<n;i++{x:=start+i;iv.SetInt64(i,int64(x));_=sv.SetString(i,fmt.Sprintf("unique-%08d",x))};c,_:=vector.NewDataChunk(iv,sv);return c}
func collect(t testing.TB,emit func(func(*vector.DataChunk)error)error)[]string{t.Helper();var r []string;if err:=emit(func(c *vector.DataChunk)error{for i:=0;i<c.Cardinality;i++{a:="NULL";if x,ok:=c.Columns[0].Int64(i);ok{a=fmt.Sprint(x)};b:="NULL";if x,ok:=c.Columns[1].String(i);ok{b=x};r=append(r,a+"|"+b)};c.Release();return nil});err!=nil{t.Fatal(err)};sort.Strings(r);return r}
func TestDistinctNullCompoundSemantics(t *testing.T){mm,s,bm,_,_,clean:=denv(t);defer clean();_=mm;d,err:=New(s,bm,[]types.LogicalType{{ID:types.BigInt},{ID:types.Varchar}},[]int{0,1},32);if err!=nil{t.Fatal(err)};for base:=0;base<300;base+=100{c:=chunk(t,s,bm,base,100);if err:=d.Consume(context.Background(),c);err!=nil{t.Fatal(err)};c.Release()};out:=collect(t,func(cb func(*vector.DataChunk)error)error{return d.Emit(context.Background(),cb)});seen:=map[string]bool{};for _,x:=range out{if seen[x]{t.Fatalf("duplicate output %q",x)};seen[x]=true};if !seen["NULL|NULL"]{t.Fatal("expected NULL|NULL distinct row")};d.Release()}
func TestExternalDistinctForcedSpillEqualsInMemory(t *testing.T){_,s,bm,_,ts,clean:=denv(t);defer clean();typesIn:=[]types.LogicalType{{ID:types.BigInt},{ID:types.Varchar}};d,_:=New(s,bm,typesIn,[]int{0,1},128);for base:=0;base<4096;base+=256{c:=uniqueChunk(t,s,bm,base,256);if err:=d.Consume(context.Background(),c);err!=nil{t.Fatal(err)};c.Release()};want:=collect(t,func(cb func(*vector.DataChunk)error)error{return d.Emit(context.Background(),cb)});d.Release();e,err:=NewExternal(ExternalConfig{InputTypes:typesIn,Columns:[]int{0,1},EstimatedRows:8,MemoryBudget:12<<10,RadixBits:8,SpillBlockBytes:2048,Scope:s,Buffers:bm,Temp:ts,Compression:storagetemp.CompressionRLE});if err!=nil{t.Fatal(err)};for base:=0;base<4096;base+=256{c:=uniqueChunk(t,s,bm,base,256);if err:=e.Consume(context.Background(),c);err!=nil{t.Fatal(err)};c.Release()};got:=collect(t,func(cb func(*vector.DataChunk)error)error{return e.Emit(context.Background(),cb)});if len(got)!=len(want){t.Fatalf("len=%d want=%d",len(got),len(want))};for i:=range want{if got[i]!=want[i]{t.Fatalf("row %d=%q want %q",i,got[i],want[i])}};if e.Stats().SpillPasses==0{t.Fatal("expected forced spill")};e.Release();if ts.Used()!=0||ts.OwnedFiles()!=0{t.Fatalf("temp used=%d files=%d",ts.Used(),ts.OwnedFiles())}}
func BenchmarkExternalDistinct(b *testing.B){for i:=0;i<b.N;i++{_,s,bm,_,ts,clean:=denv(b);e,_:=NewExternal(ExternalConfig{InputTypes:[]types.LogicalType{{ID:types.BigInt},{ID:types.Varchar}},Columns:[]int{0,1},EstimatedRows:8,MemoryBudget:12<<10,RadixBits:8,SpillBlockBytes:4096,Scope:s,Buffers:bm,Temp:ts});for base:=0;base<2048;base+=256{c:=uniqueChunk(b,s,bm,base,256);if err:=e.Consume(context.Background(),c);err!=nil{b.Fatal(err)};c.Release()};if err:=e.Emit(context.Background(),func(c *vector.DataChunk)error{c.Release();return nil});err!=nil{b.Fatal(err)};e.Release();clean()}}

package gosql

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"testing"
)

func phase5RowsFunction() TableFunction {
	return TableFunction{Name:"phase5_rows",Columns:[]Column{{Name:"grp",Type:Type{ID:"BIGINT"},Nullable:false},{Name:"id",Type:Type{ID:"BIGINT"},Nullable:false}},Open:func(context.Context)(BatchReader,error){emitted:=false;return &batchReaderFunc{next:func(_ context.Context,w BatchWriter)(int,error){if emitted{return 0,io.EOF};emitted=true;rows:=[][2]int64{{1,3},{1,1},{1,2},{2,5},{2,4}};for i,r:=range rows{if err:=w.SetInt64(i,0,r[0]);err!=nil{return 0,err};if err:=w.SetInt64(i,1,r[1]);err!=nil{return 0,err}};return len(rows),io.EOF}},nil}}
}
func phase5DB(t *testing.T,spill int64)*DB{t.Helper();return testTypedDB(t,spill,phase5RowsFunction())}
func collectPhase5Int(t *testing.T,db *DB,sql string,col int)[]int64{t.Helper();var out []int64;_,err:=db.QueryChunks(context.Background(),sql,func(c *Chunk)error{for r:=0;r<c.RowCount();r++{v,ok,e:=c.Int64(r,col);if e!=nil{return e};if !ok{return fmt.Errorf("column %d row %d unexpectedly NULL",col,r)};out=append(out,v)};return nil});if err!=nil{t.Fatal(err)};return out}

func TestPhase5DerivedTableAndWindows(t *testing.T){db:=phase5DB(t,1<<20)
	if got:=collectPhase5Int(t,db,"SELECT d.id FROM (SELECT id FROM phase5_rows()) AS d ORDER BY d.id",0);!reflect.DeepEqual(got,[]int64{1,2,3,4,5}){t.Fatalf("derived=%v",got)}
	var rows [][4]int64;_,err:=db.QueryChunks(context.Background(),"SELECT grp,id,ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id), SUM(id) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) FROM phase5_rows() ORDER BY grp,id",func(c *Chunk)error{for r:=0;r<c.RowCount();r++{g,_,e:=c.Int64(r,0);if e!=nil{return e};id,_,e:=c.Int64(r,1);if e!=nil{return e};rn,_,e:=c.Int64(r,2);if e!=nil{return e};prefixNull,e:=c.IsNull(r,3);if e!=nil{return e};prefix:=int64(-1);if !prefixNull{wide,ok,e:=c.Int128(r,3);if e!=nil{return e};if !ok{return fmt.Errorf("prefix NULL")};prefix,e=strconv.ParseInt(wide.String(),10,64);if e!=nil{return e}};rows=append(rows,[4]int64{g,id,rn,prefix})};return nil});if err!=nil{t.Fatal(err)};want:=[][4]int64{{1,1,1,-1},{1,2,2,1},{1,3,3,3},{2,4,1,-1},{2,5,2,4}};if !reflect.DeepEqual(rows,want){t.Fatalf("windows=%v want=%v",rows,want)}
	var global [][3]int64;_,err=db.QueryChunks(context.Background(),"SELECT id, COUNT(*) OVER (), SUM(id) OVER () FROM phase5_rows() ORDER BY id",func(c *Chunk)error{for r:=0;r<c.RowCount();r++{a,_,_:=c.Int64(r,0);b,_,_:=c.Int64(r,1);sum,ok,e:=c.Int128(r,2);if e!=nil{return e};if !ok{return fmt.Errorf("sum NULL")};sv,e:=strconv.ParseInt(sum.String(),10,64);if e!=nil{return e};global=append(global,[3]int64{a,b,sv})};return nil});if err!=nil{t.Fatal(err)};for i,r:=range global{if r[0]!=int64(i+1)||r[1]!=5||r[2]!=15{t.Fatalf("global=%v",global)}}
}

func TestPhase5SamplingRepeatableAndBoundaries(t *testing.T){
	for _,spill:=range []int64{1<<20,1}{t.Run(fmt.Sprintf("spill_%d",spill),func(t *testing.T){db:=phase5DB(t,spill);a:=collectPhase5Int(t,db,"SELECT id FROM phase5_rows() USING SAMPLE reservoir(3 ROWS) REPEATABLE(0) ORDER BY id",0);b:=collectPhase5Int(t,db,"SELECT id FROM phase5_rows() USING SAMPLE reservoir(3 ROWS) REPEATABLE(0) ORDER BY id",0);if !reflect.DeepEqual(a,b)||len(a)!=3{t.Fatalf("reservoir a=%v b=%v",a,b)};zero:=collectPhase5Int(t,db,"SELECT id FROM phase5_rows() USING SAMPLE bernoulli(0 PERCENT) REPEATABLE(1)",0);if len(zero)!=0{t.Fatalf("bernoulli0=%v",zero)};all:=collectPhase5Int(t,db,"SELECT id FROM phase5_rows() USING SAMPLE bernoulli(100 PERCENT) REPEATABLE(1)",0);sort.Slice(all,func(i,j int)bool{return all[i]<all[j]});if !reflect.DeepEqual(all,[]int64{1,2,3,4,5}){t.Fatalf("bernoulli100=%v",all)}})}
}

func TestPhase5IsNotDistinctFrom(t *testing.T){db:=phase5DB(t,1<<20);var got [][2]int64;_,err:=db.QueryChunks(context.Background(),"SELECT a.id,b.id FROM phase5_rows() a JOIN phase5_rows() b ON a.id IS NOT DISTINCT FROM b.id WHERE a.id=2",func(c *Chunk)error{for r:=0;r<c.RowCount();r++{a,_,_:=c.Int64(r,0);b,_,_:=c.Int64(r,1);got=append(got,[2]int64{a,b})};return nil});if err!=nil{t.Fatal(err)};if !reflect.DeepEqual(got,[][2]int64{{2,2}}){t.Fatalf("null-safe join=%v",got)}}


func TestPhase5WindowRemediation(t *testing.T) {
	db := phase5DB(t, 1<<20)

	nested := collectPhase5Int(t, db, "SELECT COUNT(*) OVER () + 1 FROM phase5_rows() ORDER BY id", 0)
	if !reflect.DeepEqual(nested, []int64{6, 6, 6, 6, 6}) {
		t.Fatalf("nested window=%v", nested)
	}

	var ordered [][3]int64
	_, err := db.QueryChunks(context.Background(), "SELECT d.id, ROW_NUMBER() OVER (ORDER BY d.id), ROW_NUMBER() OVER (ORDER BY d.rev) FROM (SELECT id, -id AS rev FROM phase5_rows()) d ORDER BY d.id", func(c *Chunk) error {
		for row := 0; row < c.RowCount(); row++ {
			id, _, e := c.Int64(row, 0)
			if e != nil { return e }
			asc, _, e := c.Int64(row, 1)
			if e != nil { return e }
			desc, _, e := c.Int64(row, 2)
			if e != nil { return e }
			ordered = append(ordered, [3]int64{id, asc, desc})
		}
		return nil
	})
	if err != nil { t.Fatal(err) }
	want := [][3]int64{{1,1,5},{2,2,4},{3,3,3},{4,4,2},{5,5,1}}
	if !reflect.DeepEqual(ordered, want) {
		t.Fatalf("different window sorts=%v want=%v", ordered, want)
	}

	var sumRows float64
	var windowSum float64
	_, err = db.QueryChunks(context.Background(), "SELECT r, SUM(r) OVER () FROM (SELECT random() AS r FROM phase5_rows()) d", func(c *Chunk) error {
		for row := 0; row < c.RowCount(); row++ {
			r, ok, e := c.Float64(row, 0)
			if e != nil || !ok { return fmt.Errorf("random valid=%v err=%v", ok, e) }
			s, ok, e := c.Float64(row, 1)
			if e != nil || !ok { return fmt.Errorf("window sum valid=%v err=%v", ok, e) }
			sumRows += r
			windowSum = s
		}
		return nil
	})
	if err != nil { t.Fatal(err) }
	diff := sumRows - windowSum
	if diff < 0 { diff = -diff }
	if diff > 1e-12 {
		t.Fatalf("global window re-executed upstream: rows sum=%g window sum=%g", sumRows, windowSum)
	}
}

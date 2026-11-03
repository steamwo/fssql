package gosql

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestPhase4ExpressionsAndMaskedEvaluation(t *testing.T) {
	var forbidden atomic.Int32
	fail := ScalarFunction{Name:"must_not_run",Arguments:[]Type{{ID:"VARCHAR"}},Result:Type{ID:"VARCHAR"},NullHandling:ScalarNullPropagate,Deterministic:true,Eval:func([]ScalarValue)(ScalarValue,error){forbidden.Add(1);return ScalarValue{},errors.New("unexpected evaluation")}}
	db,err:=Open(Config{ScalarFunctions:[]ScalarFunction{fail},TempDir:t.TempDir(),ManifestDir:t.TempDir()});if err!=nil{t.Fatal(err)};defer db.Close()
	sqlText := "SELECT CAST('12.34' AS DECIMAL(10,2)) AS d, CASE WHEN true THEN 'ok' ELSE must_not_run('x') END AS c, coalesce('kept', must_not_run('y')) AS co, 'A' || 'β' AS joined, length('A😊') AS n, substr('A😊B', 2, 1) AS sub, strpos('A😊B','😊') AS pos, replace('aba','a','x') AS repl, repeat('ab',2) AS rep, lower('ÄBC') AS low, hex('Az') AS hx, floor(CAST('12.34' AS DECIMAL(10,2))) AS fl, mod(7,3) AS m, least(3,2,4) AS le, greatest(3,2,4) AS gr, nullif(1,1) AS ni, md5('abc') AS md"
	seen:=false
	_,err=db.QueryChunks(context.Background(),sqlText,func(c *Chunk)error{seen=true
		if v,ok,e:=c.Int64(0,0);e!=nil||!ok||v!=1234{return fmt.Errorf("decimal=%d valid=%v err=%v",v,ok,e)}
		stringsWant:=map[int]string{1:"ok",2:"kept",3:"Aβ",5:"😊",7:"xbx",8:"abab",9:"äbc",10:"417A",16:"900150983cd24fb0d6963f7d28e17f72"}
		for col,want:=range stringsWant{got,ok,e:=c.String(0,col);if e!=nil||!ok||got!=want{return fmt.Errorf("col %d=%q valid=%v err=%v want=%q",col,got,ok,e,want)}}
		for col,want:=range map[int]int64{4:2,6:2,11:1200,12:1,13:2,14:4}{got,ok,e:=c.Int64(0,col);if e!=nil||!ok||got!=want{return fmt.Errorf("col %d=%d valid=%v err=%v want=%d",col,got,ok,e,want)}}
		null,e:=c.IsNull(0,15);if e!=nil||!null{return fmt.Errorf("nullif null=%v err=%v",null,e)}
		return nil})
	if err!=nil{t.Fatal(err)};if !seen{t.Fatal("no result chunk")};if forbidden.Load()!=0{t.Fatalf("masked branch evaluated %d times",forbidden.Load())};if used:=db.MemoryUsed();used!=0{t.Fatalf("managed memory leaked: %d",used)}
}

func TestPhase4TimeFunctions(t *testing.T){db,err:=Open(Config{TempDir:t.TempDir(),ManifestDir:t.TempDir()});if err!=nil{t.Fatal(err)};defer db.Close();sqlText:="SELECT strftime(CAST('2026-01-02 03:04:05.123456' AS TIMESTAMP), '%Y-%m-%d %H:%M:%S.%f'), CAST(strptime('2026-01-02 03:04:05.123456','%Y-%m-%d %H:%M:%S.%f') AS VARCHAR), CAST(date_trunc('day', CAST('2026-01-02 03:04:05.123456' AS TIMESTAMP)) AS VARCHAR), CAST(timezone('UTC', CAST('2026-01-02 03:04:05.123456' AS TIMESTAMP)) AS VARCHAR)";_,err=db.QueryChunks(context.Background(),sqlText,func(c *Chunk)error{want:=[]string{"2026-01-02 03:04:05.123456","2026-01-02 03:04:05.123456","2026-01-02 00:00:00","2026-01-02 03:04:05.123456"};for i,w:=range want{got,ok,e:=c.String(0,i);if e!=nil||!ok||got!=w{return fmt.Errorf("time col %d=%q valid=%v err=%v want=%q",i,got,ok,e,w)}};return nil});if err!=nil{t.Fatal(err)};if used:=db.MemoryUsed();used!=0{t.Fatalf("managed memory leaked: %d",used)}}

func TestPhase4HMACVectorsNullUnicodeAndTenantIsolation(t *testing.T){if _,err:=NewHMACSHA256ScalarFunction(nil);err==nil{t.Fatal("missing HMAC key should fail")};known,err:=NewHMACSHA256ScalarFunction([]byte("Jefe"));if err!=nil{t.Fatal(err)};db,err:=Open(Config{ScalarFunctions:[]ScalarFunction{known},TempDir:t.TempDir(),ManifestDir:t.TempDir()});if err!=nil{t.Fatal(err)};defer db.Close();var got string;_,err=db.QueryChunks(context.Background(),"SELECT data_plane_hmac_sha256('what do ya want for nothing?')",func(c *Chunk)error{v,ok,e:=c.String(0,0);if e!=nil{return e};if !ok{return errors.New("HMAC unexpectedly NULL")};got=v;return nil});if err!=nil{t.Fatal(err)};if want:="5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843";got!=want{t.Fatalf("HMAC=%s want=%s",got,want)}
	_,err=db.QueryChunks(context.Background(),"SELECT data_plane_hmac_sha256(CAST(NULL AS VARCHAR))",func(c *Chunk)error{isNull,e:=c.IsNull(0,0);if e!=nil{return e};if !isNull{return errors.New("NULL HMAC input did not remain NULL")};return nil});if err!=nil{t.Fatal(err)}
	for _,tc:=range []struct{key,msg string}{{"tenant-a","你好😊"},{"tenant-b","你好😊"}}{fn,_:=NewHMACSHA256ScalarFunction([]byte(tc.key));tenant,err:=Open(Config{ScalarFunctions:[]ScalarFunction{fn},TempDir:t.TempDir(),ManifestDir:t.TempDir()});if err!=nil{t.Fatal(err)};var value string;sqlText:="SELECT data_plane_hmac_sha256('"+tc.msg+"')";_,err=tenant.QueryChunks(context.Background(),sqlText,func(c *Chunk)error{value,_,err=c.String(0,0);return err});_ = tenant.Close();if err!=nil{t.Fatal(err)};mac:=hmac.New(sha256.New,[]byte(tc.key));_,_=mac.Write([]byte(tc.msg));want:=hex.EncodeToString(mac.Sum(nil));if value!=want{t.Fatalf("tenant HMAC=%s want=%s",value,want)}}
}

func TestPhase4RandomEvaluatesPerRow(t *testing.T){fn:=TableFunction{Name:"random_rows",Columns:[]Column{{Name:"id",Type:Type{ID:"BIGINT"},Nullable:false}},Open:func(context.Context)(BatchReader,error){emitted:=false;return &batchReaderFunc{next:func(_ context.Context,w BatchWriter)(int,error){if emitted{return 0,io.EOF};emitted=true;for i:=0;i<3;i++{if err:=w.SetInt64(i,0,int64(i));err!=nil{return 0,err}};return 3,io.EOF}},nil}};db:=testTypedDB(t,1<<20,fn);var values []float64;_,err:=db.QueryChunks(context.Background(),"SELECT random() FROM random_rows()",func(c *Chunk)error{for row:=0;row<c.RowCount();row++{v,ok,e:=c.Float64(row,0);if e!=nil{return e};if !ok||v<0||v>=1{return fmt.Errorf("random=%v valid=%v",v,ok)};values=append(values,v)};return nil});if err!=nil{t.Fatal(err)};if len(values)!=3{t.Fatalf("rows=%d",len(values))};if values[0]==values[1]&&values[1]==values[2]{t.Fatal("random() was not evaluated per row")}}

func TestPhase4CSVExplicitSchemaAndQuotedNulls(t *testing.T){root:=t.TempDir();path:=filepath.Join(root,"typed.csv");data:="amount,day,ts,note,all_null\n12.34,2026-01-02,2026-01-02 03:04:05,,\n0.01,2026-01-03,2026-01-03 04:05:06,\"\",\n";if err:=os.WriteFile(path,[]byte(data),0600);err!=nil{t.Fatal(err)};uri,err:=FileURI(path);if err!=nil{t.Fatal(err)};db,err:=Open(Config{LocalFiles:&LocalFileConfig{RootDir:root},TempDir:t.TempDir(),ManifestDir:t.TempDir()});if err!=nil{t.Fatal(err)};defer db.Close();sqlText:=fmt.Sprintf("SELECT amount, day, ts, note, all_null FROM read_csv('%s', columns='amount DECIMAL(10,2), day DATE, ts TIMESTAMP, note VARCHAR, all_null BIGINT', auto_detect=false, header=true, allow_quoted_nulls=false)",strings.ReplaceAll(uri,"'","''"));rows:=0;_,err=db.QueryChunks(context.Background(),sqlText,func(c *Chunk)error{for row:=0;row<c.RowCount();row++{amount,ok,e:=c.Int64(row,0);if e!=nil||!ok{return fmt.Errorf("amount valid=%v err=%v",ok,e)};want:=int64(1234);if rows==1{want=1};if amount!=want{return fmt.Errorf("amount=%d want=%d",amount,want)};if null,e:=c.IsNull(row,4);e!=nil||!null{return fmt.Errorf("all_null=%v err=%v",null,e)};noteNull,e:=c.IsNull(row,3);if e!=nil{return e};if rows==0&&!noteNull{return errors.New("unquoted empty field should be NULL")};if rows==1{if noteNull{return errors.New("quoted empty field should remain empty string")};note,ok,e:=c.String(row,3);if e!=nil||!ok||note!=""{return fmt.Errorf("quoted empty=%q valid=%v err=%v",note,ok,e)}};rows++};return nil});if err!=nil{t.Fatal(err)};if rows!=2{t.Fatalf("rows=%d",rows)};if used:=db.MemoryUsed();used!=0{t.Fatalf("managed memory leaked: %d",used)}
	quoted:=filepath.Join(root,"quoted.csv");if err:=os.WriteFile(quoted,[]byte("\"\"\n"),0600);err!=nil{t.Fatal(err)};quotedURI,_:=FileURI(quoted);sqlText=fmt.Sprintf("SELECT note FROM read_csv('%s', columns='note VARCHAR', auto_detect=false, allow_quoted_nulls=true)",quotedURI);_,err=db.QueryChunks(context.Background(),sqlText,func(c *Chunk)error{null,e:=c.IsNull(0,0);if e!=nil{return e};if !null{return errors.New("allow_quoted_nulls=true should map quoted null literal to NULL")};return nil});if err!=nil{t.Fatal(err)}
}

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

func TestExpressionsAndMaskedEvaluation(t *testing.T) {
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

func TestTimeFunctions(t *testing.T){db,err:=Open(Config{TempDir:t.TempDir(),ManifestDir:t.TempDir()});if err!=nil{t.Fatal(err)};defer db.Close();sqlText:="SELECT strftime(CAST('2026-01-02 03:04:05.123456' AS TIMESTAMP), '%Y-%m-%d %H:%M:%S.%f'), CAST(strptime('2026-01-02 03:04:05.123456','%Y-%m-%d %H:%M:%S.%f') AS VARCHAR), CAST(date_trunc('day', CAST('2026-01-02 03:04:05.123456' AS TIMESTAMP)) AS VARCHAR), CAST(timezone('UTC', CAST('2026-01-02 03:04:05.123456' AS TIMESTAMP)) AS VARCHAR)";_,err=db.QueryChunks(context.Background(),sqlText,func(c *Chunk)error{want:=[]string{"2026-01-02 03:04:05.123456","2026-01-02 03:04:05.123456","2026-01-02 00:00:00","2026-01-02 03:04:05.123456+00"};for i,w:=range want{got,ok,e:=c.String(0,i);if e!=nil||!ok||got!=w{return fmt.Errorf("time col %d=%q valid=%v err=%v want=%q",i,got,ok,e,w)}};return nil});if err!=nil{t.Fatal(err)};if used:=db.MemoryUsed();used!=0{t.Fatalf("managed memory leaked: %d",used)}}

func TestHMACVectorsNullUnicodeAndTenantIsolation(t *testing.T){if _,err:=NewHMACSHA256ScalarFunction(nil);err==nil{t.Fatal("missing HMAC key should fail")};known,err:=NewHMACSHA256ScalarFunction([]byte("Jefe"));if err!=nil{t.Fatal(err)};db,err:=Open(Config{ScalarFunctions:[]ScalarFunction{known},TempDir:t.TempDir(),ManifestDir:t.TempDir()});if err!=nil{t.Fatal(err)};defer db.Close();var got string;_,err=db.QueryChunks(context.Background(),"SELECT data_plane_hmac_sha256('what do ya want for nothing?')",func(c *Chunk)error{v,ok,e:=c.String(0,0);if e!=nil{return e};if !ok{return errors.New("HMAC unexpectedly NULL")};got=v;return nil});if err!=nil{t.Fatal(err)};if want:="5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843";got!=want{t.Fatalf("HMAC=%s want=%s",got,want)}
	_,err=db.QueryChunks(context.Background(),"SELECT data_plane_hmac_sha256(CAST(NULL AS VARCHAR))",func(c *Chunk)error{isNull,e:=c.IsNull(0,0);if e!=nil{return e};if !isNull{return errors.New("NULL HMAC input did not remain NULL")};return nil});if err!=nil{t.Fatal(err)}
	for _,tc:=range []struct{key,msg string}{{"tenant-a","你好😊"},{"tenant-b","你好😊"}}{fn,_:=NewHMACSHA256ScalarFunction([]byte(tc.key));tenant,err:=Open(Config{ScalarFunctions:[]ScalarFunction{fn},TempDir:t.TempDir(),ManifestDir:t.TempDir()});if err!=nil{t.Fatal(err)};var value string;sqlText:="SELECT data_plane_hmac_sha256('"+tc.msg+"')";_,err=tenant.QueryChunks(context.Background(),sqlText,func(c *Chunk)error{value,_,err=c.String(0,0);return err});_ = tenant.Close();if err!=nil{t.Fatal(err)};mac:=hmac.New(sha256.New,[]byte(tc.key));_,_=mac.Write([]byte(tc.msg));want:=hex.EncodeToString(mac.Sum(nil));if value!=want{t.Fatalf("tenant HMAC=%s want=%s",value,want)}}
}

func TestRandomEvaluatesPerRow(t *testing.T){fn:=TableFunction{Name:"random_rows",Columns:[]Column{{Name:"id",Type:Type{ID:"BIGINT"},Nullable:false}},Open:func(context.Context)(BatchReader,error){emitted:=false;return &batchReaderFunc{next:func(_ context.Context,w BatchWriter)(int,error){if emitted{return 0,io.EOF};emitted=true;for i:=0;i<3;i++{if err:=w.SetInt64(i,0,int64(i));err!=nil{return 0,err}};return 3,io.EOF}},nil}};db:=testTypedDB(t,1<<20,fn);var values []float64;_,err:=db.QueryChunks(context.Background(),"SELECT random() FROM random_rows()",func(c *Chunk)error{for row:=0;row<c.RowCount();row++{v,ok,e:=c.Float64(row,0);if e!=nil{return e};if !ok||v<0||v>=1{return fmt.Errorf("random=%v valid=%v",v,ok)};values=append(values,v)};return nil});if err!=nil{t.Fatal(err)};if len(values)!=3{t.Fatalf("rows=%d",len(values))};if values[0]==values[1]&&values[1]==values[2]{t.Fatal("random() was not evaluated per row")}}

func TestCSVExplicitSchemaAndQuotedNulls(t *testing.T){root:=t.TempDir();path:=filepath.Join(root,"typed.csv");data:="amount,day,ts,note,all_null\n12.34,2026-01-02,2026-01-02 03:04:05,,\n0.01,2026-01-03,2026-01-03 04:05:06,\"\",\n";if err:=os.WriteFile(path,[]byte(data),0600);err!=nil{t.Fatal(err)};uri,err:=FileURI(path);if err!=nil{t.Fatal(err)};db,err:=Open(Config{LocalFiles:&LocalFileConfig{RootDir:root},TempDir:t.TempDir(),ManifestDir:t.TempDir()});if err!=nil{t.Fatal(err)};defer db.Close();sqlText:=fmt.Sprintf("SELECT amount, day, ts, note, all_null FROM read_csv('%s', columns='amount DECIMAL(10,2), day DATE, ts TIMESTAMP, note VARCHAR, all_null BIGINT', auto_detect=false, header=true, allow_quoted_nulls=false)",strings.ReplaceAll(uri,"'","''"));rows:=0;_,err=db.QueryChunks(context.Background(),sqlText,func(c *Chunk)error{for row:=0;row<c.RowCount();row++{amount,ok,e:=c.Int64(row,0);if e!=nil||!ok{return fmt.Errorf("amount valid=%v err=%v",ok,e)};want:=int64(1234);if rows==1{want=1};if amount!=want{return fmt.Errorf("amount=%d want=%d",amount,want)};if null,e:=c.IsNull(row,4);e!=nil||!null{return fmt.Errorf("all_null=%v err=%v",null,e)};noteNull,e:=c.IsNull(row,3);if e!=nil{return e};if rows==0&&!noteNull{return errors.New("unquoted empty field should be NULL")};if rows==1{if noteNull{return errors.New("quoted empty field should remain empty string")};note,ok,e:=c.String(row,3);if e!=nil||!ok||note!=""{return fmt.Errorf("quoted empty=%q valid=%v err=%v",note,ok,e)}};rows++};return nil});if err!=nil{t.Fatal(err)};if rows!=2{t.Fatalf("rows=%d",rows)};if used:=db.MemoryUsed();used!=0{t.Fatalf("managed memory leaked: %d",used)}
	quoted:=filepath.Join(root,"quoted.csv");if err:=os.WriteFile(quoted,[]byte("\"\"\n"),0600);err!=nil{t.Fatal(err)};quotedURI,_:=FileURI(quoted);sqlText=fmt.Sprintf("SELECT note FROM read_csv('%s', columns='note VARCHAR', auto_detect=false, allow_quoted_nulls=true)",quotedURI);_,err=db.QueryChunks(context.Background(),sqlText,func(c *Chunk)error{null,e:=c.IsNull(0,0);if e!=nil{return e};if !null{return errors.New("allow_quoted_nulls=true should map quoted null literal to NULL")};return nil});if err!=nil{t.Fatal(err)}
}


// TestSubstrDuckDBBoundaries 验证起点 0、负起点、越界长度与 Unicode 的
// DuckDB 语义：起点 0 表示第一个字符之前，负起点从末尾倒数，越过开头的
// 长度会被自然截短。
func TestSubstrDuckDBBoundaries(t *testing.T) {
	db, err := Open(Config{TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	query := `SELECT substr('abc',0,1), substr('abc',0,2), substr('abc',0,3), substr('abc',0,4),
	                substr('abc',-1,2), substr('abc',-2,2), substr('abc',-4,2), substr('abc',-4,3),
	                substr('abc',0), substr('abc',-1), substr('abc',-4),
	                substr('abc',1,0), substr('abc',1,-1),
	                substr('你好世界',0,2), substr('你好世界',-2,2)`
	want := []string{"", "a", "ab", "abc", "c", "bc", "a", "ab", "abc", "c", "abc", "", "", "你", "世界"}
	_, err = db.QueryChunks(context.Background(), query, func(chunk *Chunk) error {
		for column, expected := range want {
			got, valid, err := chunk.String(0, column)
			if err != nil || !valid || got != expected {
				return fmt.Errorf("column %d=%q valid=%v err=%v want %q", column, got, valid, err, expected)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("managed memory leak: %d", used)
	}
}

// TestBooleanNumericCastDuckDBSemantics 验证 BOOLEAN 与数值互转：
// true=1、false=0；数值 0 为 false，非 0（含负数和浮点）为 true。
func TestBooleanNumericCastDuckDBSemantics(t *testing.T) {
	db, err := Open(Config{TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.QueryChunks(context.Background(),
		"SELECT CAST(TRUE AS INTEGER), CAST(FALSE AS BIGINT), CAST(TRUE AS UTINYINT)",
		func(chunk *Chunk) error {
			for column, expected := range []int64{1, 0, 1} {
				var got int64
				var valid bool
				var err error
				if column == 2 {
					var unsigned uint64
					unsigned, valid, err = chunk.Uint64(0, column)
					got = int64(unsigned)
				} else {
					got, valid, err = chunk.Int64(0, column)
				}
				if err != nil || !valid || got != expected {
					return fmt.Errorf("column %d=%d valid=%v err=%v want %d", column, got, valid, err, expected)
				}
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.QueryChunks(context.Background(),
		"SELECT CAST(0 AS BOOLEAN), CAST(2 AS BOOLEAN), CAST(-3 AS BOOLEAN), CAST(0.5 AS BOOLEAN), CAST(0.0 AS BOOLEAN)",
		func(chunk *Chunk) error {
			for column, expected := range []bool{false, true, true, true, false} {
				got, valid, err := chunk.Bool(0, column)
				if err != nil || !valid || got != expected {
					return fmt.Errorf("column %d=%v valid=%v err=%v want %v", column, got, valid, err, expected)
				}
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("managed memory leak: %d", used)
	}
}

// TestBlobLiteralDuckDBSemantics 验证 VARCHAR -> BLOB 只解码 \xHH，
// 并拒绝裸反斜杠、\n 或不足两位的十六进制转义。
func TestBlobLiteralDuckDBSemantics(t *testing.T) {
	db, err := Open(Config{TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.QueryChunks(context.Background(),
		`SELECT hex(CAST('A\x00\xFF' AS BLOB)), hex(CAST('' AS BLOB)), length(CAST('A\x00\xFF' AS BLOB))`,
		func(chunk *Chunk) error {
			for column, expected := range []string{"4100FF", ""} {
				got, valid, err := chunk.String(0, column)
				if err != nil || !valid || got != expected {
					return fmt.Errorf("column %d=%q valid=%v err=%v want %q", column, got, valid, err, expected)
				}
			}
			length, valid, err := chunk.Int64(0, 2)
			if err != nil || !valid || length != 3 {
				return fmt.Errorf("length=%d valid=%v err=%v want 3", length, valid, err)
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	for _, sqlText := range []string{
		`SELECT CAST('A\x4' AS BLOB)`,
		`SELECT CAST('A\B' AS BLOB)`,
		`SELECT CAST('\n' AS BLOB)`,
	} {
		if _, err := db.QueryChunks(context.Background(), sqlText, func(*Chunk) error { return nil }); err == nil {
			t.Fatalf("expected invalid BLOB escape error: %s", sqlText)
		}
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("managed memory leak: %d", used)
	}
}

// TestBlobVarcharRoundTrip 验证 BLOB 文本转义符合 DuckDB 语义，且往返转换保留原始字节。
func TestBlobVarcharRoundTrip(t *testing.T) {
	db, err := Open(Config{TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, tc := range []struct {
		name    string // name 标识转换边界。
		literal string // literal 是 BLOB 输入的 SQL 字符串内容。
		text    string // text 是预期的 VARCHAR 表示。
		hex     string // hex 是往返转换后预期的字节十六进制。
	}{
		{name: "empty", literal: "", text: "", hex: ""},
		{name: "printable", literal: " Hello, world!~", text: " Hello, world!~", hex: "2048656C6C6F2C20776F726C64217E"},
		{name: "backslash", literal: `\x5C`, text: `\x5C`, hex: "5C"},
		{name: "escape_like_bytes", literal: `\x5Cx41`, text: `\x5Cx41`, hex: "5C783431"},
		{name: "quotes", literal: `\x22\x27`, text: `\x22\x27`, hex: "2227"},
		{name: "binary", literal: `\x00\x09\x0A\x0D\x1F\x7F\x80\xFF`, text: `\x00\x09\x0A\x0D\x1F\x7F\x80\xFF`, hex: "00090A0D1F7F80FF"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := fmt.Sprintf(`SELECT CAST(payload AS VARCHAR),
				hex(CAST(CAST(payload AS VARCHAR) AS BLOB)),
				CAST(CAST(NULL AS BLOB) AS VARCHAR), CAST('\x41' AS VARCHAR)
				FROM (SELECT CAST('%s' AS BLOB) AS payload) t`, tc.literal)
			rows := 0
			_, err := db.QueryChunks(context.Background(), query, func(chunk *Chunk) error {
				for row := 0; row < chunk.RowCount(); row++ {
					rows++
					for column, want := range []string{tc.text, tc.hex} {
						got, valid, err := chunk.String(row, column)
						if err != nil || !valid || got != want {
							return fmt.Errorf("column %d=%q valid=%v err=%v want %q", column, got, valid, err, want)
						}
					}
					if null, err := chunk.IsNull(row, 2); err != nil || !null {
						return fmt.Errorf("NULL conversion: null=%v err=%v", null, err)
					}
					if got, valid, err := chunk.String(row, 3); err != nil || !valid || got != `\x41` {
						return fmt.Errorf("VARCHAR conversion=%q valid=%v err=%v", got, valid, err)
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if rows != 1 {
				t.Fatalf("rows=%d want 1", rows)
			}
		})
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("managed memory leak: %d", used)
	}
}

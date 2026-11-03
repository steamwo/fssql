package expression

import (
	"crypto/md5"
	cryptorand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

type ScalarNullHandling uint8
const (
	ScalarNullPropagate ScalarNullHandling = iota
	ScalarNullCall
)

// ScalarValue is the internal, lossless scalar boundary used by registered
// Go UDFs. Variable-width payloads are copied at the public adapter boundary.
type ScalarValue struct {
	Type    types.LogicalType
	Valid   bool
	Bool    bool
	Int64   int64
	Uint64  uint64
	Float64 float64
	Int128  types.Int128
	Bytes   []byte
}

type ScalarRuntime struct {
	Deterministic bool
	NullHandling  ScalarNullHandling
	Eval          func([]ScalarValue) (ScalarValue, error)
}

type ScalarRegistry map[function.ID]ScalarRuntime

func (e *Executor) executeScalarFunction(expr *binder.BoundExpression, chunk *vector.DataChunk) (*vector.Vector, error) {
	if expr == nil || expr.FunctionKind != function.Scalar { return nil, fmt.Errorf("scalar execution requires scalar BoundFunction") }
	if expr.FunctionID == function.CaseWhenID { return e.executeCaseWhen(expr, chunk) }
	if expr.FunctionID == function.CoalesceID { return e.executeCoalesce(expr, chunk) }
	if expr.FunctionID >= function.FirstCastFunctionID && expr.FunctionID < function.FirstDynamicScalarID { return e.executeCastFunction(expr, chunk) }
	args := make([]*vector.Vector, 0, len(expr.Children))
	for _, child := range expr.Children {
		v, err := e.ExecuteSQL(child, chunk); if err != nil { releaseScalarVectors(args); return nil, err }
		args = append(args, v)
	}
	defer releaseScalarVectors(args)
	out, err := vector.NewFlat(e.Scope, e.Buffers, expr.Type, chunk.Cardinality); if err != nil { return nil, err }
	for row:=0; row<chunk.Cardinality; row++ {
		values:=make([]ScalarValue,len(args)); anyNull:=false
		for i,arg:=range args { values[i],err=scalarFromVector(arg,row); if err!=nil { out.Release(); return nil,err }; if !values[i].Valid { anyNull=true } }
		var result ScalarValue
		if runtime,ok:=e.Scalars[expr.FunctionID]; ok {
			if runtime.Eval==nil { out.Release(); return nil,fmt.Errorf("scalar UDF %d has nil evaluator",expr.FunctionID) }
			if anyNull && runtime.NullHandling==ScalarNullPropagate { result=ScalarValue{Type:expr.Type,Valid:false} } else { result,err=runtime.Eval(values) }
		} else {
			result,err=evalBuiltinScalar(expr.FunctionID,expr.Type,values)
		}
		if err!=nil { out.Release(); return nil,err }
		if err=writeScalarValue(out,row,expr.Type,result); err!=nil { out.Release(); return nil,err }
	}
	return out,nil
}

func releaseScalarVectors(vs []*vector.Vector){ for _,v:=range vs { if v!=nil { v.Release() } } }

func (e *Executor) executeCaseWhen(expr *binder.BoundExpression, chunk *vector.DataChunk)(*vector.Vector,error){
	if len(expr.Children)!=3{return nil,fmt.Errorf("CASE requires condition, then, else")}
	cond,err:=e.ExecuteSQL(expr.Children[0],chunk);if err!=nil{return nil,err};defer cond.Release()
	trueRows:=make([]int,0,chunk.Cardinality);falseRows:=make([]int,0,chunk.Cardinality)
	for row:=0;row<chunk.Cardinality;row++{v,ok:=cond.Bool(row);if ok&&v{trueRows=append(trueRows,row)}else{falseRows=append(falseRows,row)}}
	out,err:=vector.NewFlat(e.Scope,e.Buffers,expr.Type,chunk.Cardinality);if err!=nil{return nil,err};for i:=0;i<chunk.Cardinality;i++{out.SetNull(i)}
	if err:=e.evaluateAndScatter(expr.Children[1],chunk,trueRows,out);err!=nil{out.Release();return nil,err}
	if err:=e.evaluateAndScatter(expr.Children[2],chunk,falseRows,out);err!=nil{out.Release();return nil,err}
	return out,nil
}

func (e *Executor) executeCoalesce(expr *binder.BoundExpression,chunk *vector.DataChunk)(*vector.Vector,error){
	if len(expr.Children)==0{return nil,fmt.Errorf("coalesce requires arguments")}
	out,err:=vector.NewFlat(e.Scope,e.Buffers,expr.Type,chunk.Cardinality);if err!=nil{return nil,err};unresolved:=make([]int,chunk.Cardinality);for i:=range unresolved{unresolved[i]=i;out.SetNull(i)}
	for _,child:=range expr.Children{if len(unresolved)==0{break};sub,cleanup,err:=dictionaryChunk(chunk,unresolved);if err!=nil{out.Release();return nil,err};value,err:=e.ExecuteSQL(child,sub);cleanup();if err!=nil{out.Release();return nil,err};next:=unresolved[:0];for i,row:=range unresolved{if value.IsValid(i){if err:=vector.CopyValue(value,i,out,row);err!=nil{value.Release();out.Release();return nil,err}}else{next=append(next,row)}};value.Release();unresolved=next}
	return out,nil
}

func (e *Executor) evaluateAndScatter(expr *binder.BoundExpression,chunk *vector.DataChunk,rows []int,out *vector.Vector)error{if len(rows)==0{return nil};sub,cleanup,err:=dictionaryChunk(chunk,rows);if err!=nil{return err};value,err:=e.ExecuteSQL(expr,sub);cleanup();if err!=nil{return err};defer value.Release();for i,row:=range rows{if err:=vector.CopyValue(value,i,out,row);err!=nil{return err}};return nil}

func dictionaryChunk(chunk *vector.DataChunk,rows []int)(*vector.DataChunk,func(),error){
	cols:=make([]*vector.Vector,0,len(chunk.Columns));indices:=make([]uint32,len(rows));for i,row:=range rows{if row<0||row>=chunk.Cardinality{return nil,nil,fmt.Errorf("expression row %d out of range",row)};indices[i]=uint32(row)}
	for _,src:=range chunk.Columns{v,err:=vector.NewDictionary(src,vector.SelectionFrom(indices));if err!=nil{releaseScalarVectors(cols);return nil,nil,err};cols=append(cols,v)}
	sub,err:=vector.NewDataChunk(cols...);if err!=nil{releaseScalarVectors(cols);return nil,nil,err};if len(cols)==0{sub.Cardinality=len(rows)};return sub,func(){sub.Release()},nil
}

func scalarFromVector(v *vector.Vector,row int)(ScalarValue,error){
	out:=ScalarValue{Type:v.Type,Valid:v.IsValid(row)};if !out.Valid{return out,nil}
	if v.Type.IsUnsignedInteger(){switch v.Type.ID{case types.TinyInt:x,_:=v.Uint8(row);out.Uint64=uint64(x);case types.SmallInt:x,_:=v.Uint16(row);out.Uint64=uint64(x);case types.Integer:x,_:=v.Uint32(row);out.Uint64=uint64(x);case types.BigInt:x,_:=v.Uint64(row);out.Uint64=x};return out,nil}
	switch v.Type.ID{case types.Boolean:out.Bool,_=v.Bool(row);case types.TinyInt:x,_:=v.Int8(row);out.Int64=int64(x);case types.SmallInt:x,_:=v.Int16(row);out.Int64=int64(x);case types.Integer:x,_:=v.Int32(row);out.Int64=int64(x);case types.BigInt:out.Int64,_=v.Int64(row);case types.HugeInt:out.Int128,_=v.Int128(row);case types.Float:x,_:=v.Float32(row);out.Float64=float64(x);case types.Double:out.Float64,_=v.Float64(row);case types.Decimal:if v.Type.Decimal.Width<=18{x,_:=v.Int64(row);out.Int128=types.Int128FromInt64(x)}else{out.Int128,_=v.Int128(row)};case types.Varchar:b,_:=v.Bytes(row);out.Bytes=append([]byte(nil),b...);case types.DateType:x,_:=v.Int32(row);out.Int64=int64(x);case types.TimestampType:out.Int64,_=v.Int64(row);default:return ScalarValue{},fmt.Errorf("scalar type %d is unsupported",v.Type.ID)};return out,nil
}

func writeScalarValue(dst *vector.Vector,row int,target types.LogicalType,value ScalarValue)error{
	if !value.Valid{dst.SetNull(row);return nil}
	if value.Type.ID!=types.Invalid&&!value.Type.Same(target){return fmt.Errorf("scalar returned type %d, want %d",value.Type.ID,target.ID)}
	if target.IsUnsignedInteger(){switch target.ID{case types.TinyInt:if value.Uint64>math.MaxUint8{return fmt.Errorf("scalar unsigned overflow")};dst.SetUint8(row,uint8(value.Uint64));case types.SmallInt:if value.Uint64>math.MaxUint16{return fmt.Errorf("scalar unsigned overflow")};dst.SetUint16(row,uint16(value.Uint64));case types.Integer:if value.Uint64>math.MaxUint32{return fmt.Errorf("scalar unsigned overflow")};dst.SetUint32(row,uint32(value.Uint64));case types.BigInt:dst.SetUint64(row,value.Uint64)};return nil}
	switch target.ID{case types.Boolean:dst.SetBool(row,value.Bool);case types.TinyInt:dst.SetInt8(row,int8(value.Int64));case types.SmallInt:dst.SetInt16(row,int16(value.Int64));case types.Integer:dst.SetInt32(row,int32(value.Int64));case types.BigInt:dst.SetInt64(row,value.Int64);case types.HugeInt:dst.SetInt128(row,value.Int128);case types.Float:dst.SetFloat32(row,float32(value.Float64));case types.Double:dst.SetFloat64(row,value.Float64);case types.Decimal:if target.Decimal.Width<=18{b:=types.Int128Big(value.Int128);if !b.IsInt64(){return fmt.Errorf("scalar DECIMAL overflow")};dst.SetInt64(row,b.Int64())}else{dst.SetInt128(row,value.Int128)};case types.Varchar:return dst.SetStringBytes(row,value.Bytes);case types.DateType:dst.SetInt32(row,int32(value.Int64));case types.TimestampType:dst.SetInt64(row,value.Int64);default:return fmt.Errorf("scalar result type %d unsupported",target.ID)};return nil
}

func evalBuiltinScalar(id function.ID,resultType types.LogicalType,args []ScalarValue)(ScalarValue,error){
	propagate:=func()bool{for _,a:=range args{if !a.Valid{return true}};return false}
	if id!=function.LeastID&&id!=function.GreatestID&&id!=function.NullIfID&&propagate(){return ScalarValue{Type:resultType,Valid:false},nil}
	out:=ScalarValue{Type:resultType,Valid:true}
	switch id{
	case function.ConcatID:out.Bytes=[]byte(string(args[0].Bytes)+string(args[1].Bytes))
	case function.LengthID:out.Int64=int64(utf8.RuneCount(args[0].Bytes))
	case function.SubstrID:out.Bytes=[]byte(substrRunes(string(args[0].Bytes),args[1].Int64,func() *int64{if len(args)<3{return nil};v:=args[2].Int64;return &v}()))
	case function.StrposID:out.Int64=int64(strposRunes(string(args[0].Bytes),string(args[1].Bytes)))
	case function.ReplaceID:out.Bytes=[]byte(strings.ReplaceAll(string(args[0].Bytes),string(args[1].Bytes),string(args[2].Bytes)))
	case function.RepeatID:if args[1].Int64<0{return ScalarValue{},fmt.Errorf("repeat count must be non-negative")};out.Bytes=[]byte(strings.Repeat(string(args[0].Bytes),int(args[1].Int64)))
	case function.LowerID:out.Bytes=[]byte(strings.ToLower(string(args[0].Bytes)))
	case function.HexID:out.Bytes=[]byte(strings.ToUpper(hex.EncodeToString(args[0].Bytes)))
	case function.FloorID:return floorScalar(resultType,args[0])
	case function.ModID:return modScalar(resultType,args[0],args[1])
	case function.LeastID,function.GreatestID:return leastGreatestScalar(id==function.GreatestID,resultType,args)
	case function.NullIfID:if !args[0].Valid{return ScalarValue{Type:resultType,Valid:false},nil};if args[1].Valid{cmp,err:=compareScalar(args[0],args[1]);if err!=nil{return ScalarValue{},err};if cmp==0{return ScalarValue{Type:resultType,Valid:false},nil}};return args[0],nil
	case function.MD5ID:sum:=md5.Sum(args[0].Bytes);out.Bytes=[]byte(hex.EncodeToString(sum[:]))
	case function.StrftimeID:s,err:=formatTemporal(args[0],string(args[1].Bytes));if err!=nil{return ScalarValue{},err};out.Bytes=[]byte(s)
	case function.StrptimeID:v,err:=parseTemporal(string(args[0].Bytes),string(args[1].Bytes),resultType);if err!=nil{return ScalarValue{},err};return v,nil
	case function.DateTruncID:return truncateTemporal(string(args[0].Bytes),args[1],resultType)
	case function.TimezoneID:if !strings.EqualFold(string(args[0].Bytes),"UTC"){return ScalarValue{},fmt.Errorf("timezone currently supports UTC only")};out.Int64=args[1].Int64
	case function.RandomID:var raw [8]byte;if _,err:=cryptorand.Read(raw[:]);err!=nil{return ScalarValue{},err};out.Float64=float64(binary.LittleEndian.Uint64(raw[:])>>11)/float64(uint64(1)<<53)
	default:return ScalarValue{},fmt.Errorf("no builtin scalar runtime for function id %d",id)}
	return out,nil
}

func substrRunes(s string,start int64,length *int64)string{r:=[]rune(s);n:=int64(len(r));var i int64;if start>0{i=start-1}else if start<0{i=n+start}else{i=0};if i<0{i=0};if i>n{i=n};j:=n;if length!=nil{if *length<=0{return ""};j=i+*length;if j>n{j=n}};return string(r[i:j])}
func strposRunes(s,needle string)int{if needle==""{return 1};sr,nr:=[]rune(s),[]rune(needle);for i:=0;i+len(nr)<=len(sr);i++{ok:=true;for j:=range nr{if sr[i+j]!=nr[j]{ok=false;break}};if ok{return i+1}};return 0}

func floorScalar(t types.LogicalType,a ScalarValue)(ScalarValue,error){out:=a;out.Type=t;switch t.ID{case types.Float,types.Double:out.Float64=math.Floor(a.Float64);case types.Decimal:scale:=t.Decimal.Scale;if scale==0{return out,nil};pow:=new(big.Int).Exp(big.NewInt(10),big.NewInt(int64(scale)),nil);n:=types.Int128Big(a.Int128);q,r:=new(big.Int),new(big.Int);q.QuoRem(n,pow,r);if n.Sign()<0&&r.Sign()!=0{q.Sub(q,big.NewInt(1))};q.Mul(q,pow);v,ok:=types.Int128FromBig(q);if !ok{return ScalarValue{},fmt.Errorf("floor DECIMAL overflow")};out.Int128=v};return out,nil}
func modScalar(t types.LogicalType,a,b ScalarValue)(ScalarValue,error){out:=ScalarValue{Type:t,Valid:true};if t.Unsigned{if b.Uint64==0{return ScalarValue{},fmt.Errorf("division by zero")};out.Uint64=a.Uint64%b.Uint64;return out,nil};switch t.ID{case types.TinyInt,types.SmallInt,types.Integer,types.BigInt:if b.Int64==0{return ScalarValue{},fmt.Errorf("division by zero")};out.Int64=a.Int64%b.Int64;case types.Float,types.Double:if b.Float64==0{return ScalarValue{},fmt.Errorf("division by zero")};out.Float64=math.Mod(a.Float64,b.Float64);case types.Decimal:n,d:=types.Int128Big(a.Int128),types.Int128Big(b.Int128);if d.Sign()==0{return ScalarValue{},fmt.Errorf("division by zero")};n.Rem(n,d);v,ok:=types.Int128FromBig(n);if !ok{return ScalarValue{},fmt.Errorf("DECIMAL modulo overflow")};out.Int128=v;default:return ScalarValue{},fmt.Errorf("mod unsupported type %d",t.ID)};return out,nil}
func leastGreatestScalar(greatest bool,t types.LogicalType,args []ScalarValue)(ScalarValue,error){var best ScalarValue;have:=false;for _,a:=range args{if !a.Valid{continue};if !have{best=a;have=true;continue};cmp,err:=compareScalar(a,best);if err!=nil{return ScalarValue{},err};if (greatest&&cmp>0)||(!greatest&&cmp<0){best=a}};if !have{return ScalarValue{Type:t,Valid:false},nil};best.Type=t;return best,nil}
func compareScalar(a,b ScalarValue)(int,error){t:=a.Type;if !t.Same(b.Type){return 0,fmt.Errorf("scalar compare type mismatch")};if t.IsUnsignedInteger(){if a.Uint64<b.Uint64{return -1,nil};if a.Uint64>b.Uint64{return 1,nil};return 0,nil};switch t.ID{case types.Boolean:if a.Bool==b.Bool{return 0,nil};if !a.Bool{return -1,nil};return 1,nil;case types.TinyInt,types.SmallInt,types.Integer,types.BigInt,types.DateType,types.TimestampType:if a.Int64<b.Int64{return -1,nil};if a.Int64>b.Int64{return 1,nil};return 0,nil;case types.HugeInt,types.Decimal:return types.CompareInt128(a.Int128,b.Int128),nil;case types.Float,types.Double:if a.Float64<b.Float64{return -1,nil};if a.Float64>b.Float64{return 1,nil};return 0,nil;case types.Varchar:return strings.Compare(string(a.Bytes),string(b.Bytes)),nil};return 0,fmt.Errorf("compare unsupported type %d",t.ID)}

func temporalTime(v ScalarValue)time.Time{if v.Type.ID==types.DateType{return time.Unix(v.Int64*86400,0).UTC()};switch v.Type.Timestamp.Unit{case types.TimestampNanos:return time.Unix(0,v.Int64).UTC();case types.TimestampMillis:return time.UnixMilli(v.Int64).UTC();default:return time.UnixMicro(v.Int64).UTC()}}
func formatTemporal(v ScalarValue,format string)(string,error){return temporalTime(v).Format(goTimeLayout(format)),nil}
func parseTemporal(value,format string,t types.LogicalType)(ScalarValue,error){tm,err:=time.ParseInLocation(goTimeLayout(format),value,time.UTC);if err!=nil{return ScalarValue{},err};out:=ScalarValue{Type:t,Valid:true};switch t.Timestamp.Unit{case types.TimestampNanos:out.Int64=tm.UnixNano();case types.TimestampMillis:out.Int64=tm.UnixMilli();default:out.Int64=tm.UnixMicro()};return out,nil}
func goTimeLayout(f string)string{r:=strings.NewReplacer("%Y","2006","%m","01","%d","02","%H","15","%M","04","%S","05","%f","000000","%g","000","%z","-0700","%%","%");return r.Replace(f)}
func truncateTemporal(unit string,v ScalarValue,t types.LogicalType)(ScalarValue,error){tm:=temporalTime(v);switch strings.ToLower(unit){case "second":tm=tm.Truncate(time.Second);case "minute":tm=tm.Truncate(time.Minute);case "hour":tm=tm.Truncate(time.Hour);case "day":tm=time.Date(tm.Year(),tm.Month(),tm.Day(),0,0,0,0,time.UTC);case "month":tm=time.Date(tm.Year(),tm.Month(),1,0,0,0,0,time.UTC);case "year":tm=time.Date(tm.Year(),1,1,0,0,0,0,time.UTC);default:return ScalarValue{},fmt.Errorf("unsupported date_trunc unit %q",unit)};out:=ScalarValue{Type:t,Valid:true};if t.ID==types.DateType{out.Int64=tm.Unix()/86400;return out,nil};switch t.Timestamp.Unit{case types.TimestampNanos:out.Int64=tm.UnixNano();case types.TimestampMillis:out.Int64=tm.UnixMilli();default:out.Int64=tm.UnixMicro()};return out,nil}

func (e *Executor) executeCastFunction(expr *binder.BoundExpression,chunk *vector.DataChunk)(*vector.Vector,error){if len(expr.Children)!=1{return nil,fmt.Errorf("CAST requires one argument")};src,err:=e.ExecuteSQL(expr.Children[0],chunk);if err!=nil{return nil,err};defer src.Release();out,err:=vector.NewFlat(e.Scope,e.Buffers,expr.Type,chunk.Cardinality);if err!=nil{return nil,err};for row:=0;row<chunk.Cardinality;row++{v,err:=scalarFromVector(src,row);if err!=nil{out.Release();return nil,err};casted,err:=castScalar(v,expr.Type);if err!=nil{out.Release();return nil,err};if err:=writeScalarValue(out,row,expr.Type,casted);err!=nil{out.Release();return nil,err}};return out,nil}
func castScalar(v ScalarValue,target types.LogicalType)(ScalarValue,error){if !v.Valid{return ScalarValue{Type:target,Valid:false},nil};if v.Type.Same(target){v.Type=target;return v,nil};out:=ScalarValue{Type:target,Valid:true};if target.ID==types.Varchar&&!target.Binary{s,err:=scalarText(v);if err!=nil{return ScalarValue{},err};out.Bytes=[]byte(s);return out,nil};if v.Type.ID==types.Varchar&&!v.Type.Binary{return parseScalarText(string(v.Bytes),target)};if target.ID==types.Decimal{var n *big.Int;switch{case v.Type.IsUnsignedInteger():n=new(big.Int).SetUint64(v.Uint64);case v.Type.ID>=types.TinyInt&&v.Type.ID<=types.BigInt:n=big.NewInt(v.Int64);case v.Type.ID==types.Decimal:n=types.Int128Big(v.Int128);default:return ScalarValue{},fmt.Errorf("unsupported CAST to DECIMAL")};if v.Type.ID!=types.Decimal{n.Mul(n,new(big.Int).Exp(big.NewInt(10),big.NewInt(int64(target.Decimal.Scale)),nil))};wide,ok:=types.Int128FromBig(n);if !ok{return ScalarValue{},fmt.Errorf("CAST DECIMAL overflow")};out.Int128=wide;return out,nil};if v.Type.ID==types.Decimal{if target.ID==types.Double{f,_:=new(big.Rat).SetFrac(types.Int128Big(v.Int128),new(big.Int).Exp(big.NewInt(10),big.NewInt(int64(v.Type.Decimal.Scale)),nil)).Float64();out.Float64=f;return out,nil};return ScalarValue{},fmt.Errorf("unsupported DECIMAL CAST")};if target.IsUnsignedInteger(){if v.Type.IsUnsignedInteger(){out.Uint64=v.Uint64}else if v.Int64>=0{out.Uint64=uint64(v.Int64)}else{return ScalarValue{},fmt.Errorf("negative value cannot CAST to unsigned")};return out,nil};switch target.ID{case types.TinyInt,types.SmallInt,types.Integer,types.BigInt:if v.Type.IsUnsignedInteger(){if v.Uint64>math.MaxInt64{return ScalarValue{},fmt.Errorf("CAST integer overflow")};out.Int64=int64(v.Uint64)}else if v.Type.ID==types.Float||v.Type.ID==types.Double{out.Int64=int64(v.Float64)}else{out.Int64=v.Int64};return out,nil;case types.Float,types.Double:if v.Type.IsUnsignedInteger(){out.Float64=float64(v.Uint64)}else if v.Type.ID==types.Float||v.Type.ID==types.Double{out.Float64=v.Float64}else{out.Float64=float64(v.Int64)};return out,nil;case types.DateType,types.TimestampType:out.Int64=v.Int64;return out,nil};return ScalarValue{},fmt.Errorf("unsupported CAST %d -> %d",v.Type.ID,target.ID)}
func scalarText(v ScalarValue)(string,error){if v.Type.IsUnsignedInteger(){return strconv.FormatUint(v.Uint64,10),nil};switch v.Type.ID{case types.Boolean:return strconv.FormatBool(v.Bool),nil;case types.TinyInt,types.SmallInt,types.Integer,types.BigInt:return strconv.FormatInt(v.Int64,10),nil;case types.Float,types.Double:return strconv.FormatFloat(v.Float64,'g',-1,64),nil;case types.Decimal:return types.FormatDecimalText(v.Int128,v.Type.Decimal.Scale),nil;case types.Varchar:return string(v.Bytes),nil;case types.DateType:return temporalTime(v).Format("2006-01-02"),nil;case types.TimestampType:return temporalTime(v).Format("2006-01-02 15:04:05.999999999"),nil};return "",fmt.Errorf("cannot format scalar type %d",v.Type.ID)}
func parseScalarText(s string,t types.LogicalType)(ScalarValue,error){out:=ScalarValue{Type:t,Valid:true};var err error;if t.IsUnsignedInteger(){out.Uint64,err=strconv.ParseUint(strings.TrimSpace(s),10,64);return out,err};switch t.ID{case types.Boolean:out.Bool,err=strconv.ParseBool(strings.ToLower(strings.TrimSpace(s)));case types.TinyInt,types.SmallInt,types.Integer,types.BigInt:out.Int64,err=strconv.ParseInt(strings.TrimSpace(s),10,64);case types.Float,types.Double:out.Float64,err=strconv.ParseFloat(strings.TrimSpace(s),64);case types.Decimal:out.Int128,err=types.ParseDecimalText(s,t.Decimal.Width,t.Decimal.Scale);case types.Varchar:out.Bytes=[]byte(s);case types.DateType:var tm time.Time;tm,err=time.Parse("2006-01-02",strings.TrimSpace(s));if err==nil{out.Int64=tm.Unix()/86400};case types.TimestampType:var tm time.Time;tm,err=time.ParseInLocation("2006-01-02 15:04:05.999999999",strings.TrimSpace(s),time.UTC);if err==nil{switch t.Timestamp.Unit{case types.TimestampNanos:out.Int64=tm.UnixNano();case types.TimestampMillis:out.Int64=tm.UnixMilli();default:out.Int64=tm.UnixMicro()}};default:err=fmt.Errorf("CAST target type %d unsupported",t.ID)};return out,err}

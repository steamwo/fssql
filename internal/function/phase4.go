package function

import (
	"fmt"

	"github.com/steamwo/fssql/internal/core/types"
)

const (
	CaseWhenID ID = 100 + iota
	ConcatID
	LengthID
	SubstrID
	StrposID
	ReplaceID
	RepeatID
	LowerID
	HexID
	FloorID
	ModID
	LeastID
	GreatestID
	CoalesceID
	NullIfID
	MD5ID
	StrftimeID
	StrptimeID
	DateTruncID
	TimezoneID
	RandomID
	CastID
)
const (
	FirstCastFunctionID ID = 3000
	FirstDynamicScalarID ID = 5000
	FirstDynamicTableID ID = 6000
)

func NewPhase4Catalog() *Catalog {
	c:=NewBuiltinCatalog();must:=func(d Definition){if err:=c.Register(d);err!=nil{panic(err)}}
	must(Definition{ID:CaseWhenID,Name:"__fssql_case_when",Kind:Scalar,MinArgs:3,MaxArgs:3,Bind:bindCaseWhen})
	must(Definition{ID:ConcatID,Name:"__fssql_concat",Kind:Scalar,MinArgs:2,MaxArgs:2,Bind:bindFixed(types.LogicalType{ID:types.Varchar},types.LogicalType{ID:types.Varchar},types.LogicalType{ID:types.Varchar})})
	must(Definition{ID:LengthID,Name:"length",Kind:Scalar,MinArgs:1,MaxArgs:1,Bind:bindFixed(types.LogicalType{ID:types.BigInt},types.LogicalType{ID:types.Varchar})})
	must(Definition{ID:SubstrID,Name:"substr",Kind:Scalar,MinArgs:2,MaxArgs:3,Bind:bindSubstr})
	must(Definition{ID:StrposID,Name:"strpos",Kind:Scalar,MinArgs:2,MaxArgs:2,Bind:bindFixed(types.LogicalType{ID:types.BigInt},types.LogicalType{ID:types.Varchar},types.LogicalType{ID:types.Varchar})})
	must(Definition{ID:ReplaceID,Name:"replace",Kind:Scalar,MinArgs:3,MaxArgs:3,Bind:bindFixed(types.LogicalType{ID:types.Varchar},types.LogicalType{ID:types.Varchar},types.LogicalType{ID:types.Varchar},types.LogicalType{ID:types.Varchar})})
	must(Definition{ID:RepeatID,Name:"repeat",Kind:Scalar,MinArgs:2,MaxArgs:2,Bind:bindFixed(types.LogicalType{ID:types.Varchar},types.LogicalType{ID:types.Varchar},types.LogicalType{ID:types.BigInt})})
	must(Definition{ID:LowerID,Name:"lower",Kind:Scalar,MinArgs:1,MaxArgs:1,Bind:bindFixed(types.LogicalType{ID:types.Varchar},types.LogicalType{ID:types.Varchar})})
	must(Definition{ID:HexID,Name:"hex",Kind:Scalar,MinArgs:1,MaxArgs:1,Bind:bindHex})
	must(Definition{ID:FloorID,Name:"floor",Kind:Scalar,MinArgs:1,MaxArgs:1,Bind:bindFloor})
	must(Definition{ID:ModID,Name:"mod",Kind:Scalar,MinArgs:2,MaxArgs:2,Bind:bindCommonNumeric})
	must(Definition{ID:LeastID,Name:"least",Kind:Scalar,MinArgs:1,MaxArgs:-1,Bind:bindCommonComparable})
	must(Definition{ID:GreatestID,Name:"greatest",Kind:Scalar,MinArgs:1,MaxArgs:-1,Bind:bindCommonComparable})
	must(Definition{ID:CoalesceID,Name:"coalesce",Kind:Scalar,MinArgs:1,MaxArgs:-1,Bind:bindCommonComparable})
	must(Definition{ID:NullIfID,Name:"nullif",Kind:Scalar,MinArgs:2,MaxArgs:2,Bind:bindNullIf})
	must(Definition{ID:MD5ID,Name:"md5",Kind:Scalar,MinArgs:1,MaxArgs:1,Bind:bindTextOrBlobToText})
	must(Definition{ID:StrftimeID,Name:"strftime",Kind:Scalar,MinArgs:2,MaxArgs:2,Bind:bindStrftime})
	must(Definition{ID:StrptimeID,Name:"strptime",Kind:Scalar,MinArgs:2,MaxArgs:2,Bind:bindFixed(types.LogicalType{ID:types.TimestampType},types.LogicalType{ID:types.Varchar},types.LogicalType{ID:types.Varchar})})
	must(Definition{ID:DateTruncID,Name:"date_trunc",Kind:Scalar,MinArgs:2,MaxArgs:2,Bind:bindDateTrunc})
	must(Definition{ID:TimezoneID,Name:"timezone",Kind:Scalar,MinArgs:2,MaxArgs:2,Bind:bindTimezone})
	must(Definition{ID:RandomID,Name:"random",Kind:Scalar,MinArgs:0,MaxArgs:0,Bind:func(BindInput)(BindResult,error){return BindResult{ReturnType:types.LogicalType{ID:types.Double}},nil}})
	registerPhase4Casts(c);return c
}
func bindFixed(result types.LogicalType,args ...types.LogicalType)BindFunc{return func(in BindInput)(BindResult,error){if len(in.Arguments)!=len(args){return BindResult{},fmt.Errorf("expected %d arguments",len(args))};return BindResult{ReturnType:result,Arguments:append([]types.LogicalType(nil),args...)},nil}}
func bindSubstr(in BindInput)(BindResult,error){if len(in.Arguments)!=2&&len(in.Arguments)!=3{return BindResult{},fmt.Errorf("substr expects 2 or 3 arguments")};args:=[]types.LogicalType{{ID:types.Varchar},{ID:types.BigInt}};if len(in.Arguments)==3{args=append(args,types.LogicalType{ID:types.BigInt})};return BindResult{ReturnType:types.LogicalType{ID:types.Varchar},Arguments:args},nil}
func bindHex(in BindInput)(BindResult,error){if len(in.Arguments)!=1{return BindResult{},fmt.Errorf("hex expects 1 argument")};if in.Arguments[0].SQLNull{return BindResult{ReturnType:types.LogicalType{ID:types.Varchar},Arguments:[]types.LogicalType{{ID:types.Varchar}}},nil};t:=in.Arguments[0].Type;if t.ID!=types.Varchar{return BindResult{},fmt.Errorf("hex expects VARCHAR or BLOB")};return BindResult{ReturnType:types.LogicalType{ID:types.Varchar},Arguments:[]types.LogicalType{t}},nil}
func bindTextOrBlobToText(in BindInput)(BindResult,error){return bindHex(in)}
func bindFloor(in BindInput)(BindResult,error){if len(in.Arguments)!=1||in.Arguments[0].SQLNull{return BindResult{},fmt.Errorf("floor requires typed numeric input")};t:=in.Arguments[0].Type;if !t.IsNumeric(){return BindResult{},fmt.Errorf("floor expects numeric input")};return BindResult{ReturnType:t,Arguments:[]types.LogicalType{t}},nil}
func bindCommonNumeric(in BindInput)(BindResult,error){if len(in.Arguments)<1{return BindResult{},fmt.Errorf("numeric function requires arguments")};t,err:=phase4CommonType(in.Arguments,true);if err!=nil{return BindResult{},err};args:=make([]types.LogicalType,len(in.Arguments));for i:=range args{args[i]=t};return BindResult{ReturnType:t,Arguments:args},nil}
func bindCommonComparable(in BindInput)(BindResult,error){if len(in.Arguments)<1{return BindResult{},fmt.Errorf("function requires arguments")};t,err:=phase4CommonType(in.Arguments,false);if err!=nil{return BindResult{},err};args:=make([]types.LogicalType,len(in.Arguments));for i:=range args{args[i]=t};return BindResult{ReturnType:t,Arguments:args},nil}
func bindNullIf(in BindInput)(BindResult,error){if len(in.Arguments)!=2{return BindResult{},fmt.Errorf("nullif expects 2 arguments")};return bindCommonComparable(in)}
func bindCaseWhen(in BindInput)(BindResult,error){if len(in.Arguments)!=3{return BindResult{},fmt.Errorf("CASE requires condition, then, else")};common,err:=phase4CommonType(in.Arguments[1:],false);if err!=nil{return BindResult{},err};return BindResult{ReturnType:common,Arguments:[]types.LogicalType{{ID:types.Boolean},common,common}},nil}
func bindStrftime(in BindInput)(BindResult,error){if len(in.Arguments)!=2||in.Arguments[0].SQLNull{return BindResult{},fmt.Errorf("strftime requires typed temporal input")};t:=in.Arguments[0].Type;if t.ID!=types.DateType&&t.ID!=types.TimestampType{return BindResult{},fmt.Errorf("strftime expects DATE or TIMESTAMP")};return BindResult{ReturnType:types.LogicalType{ID:types.Varchar},Arguments:[]types.LogicalType{t,types.LogicalType{ID:types.Varchar}}},nil}
func bindDateTrunc(in BindInput)(BindResult,error){if len(in.Arguments)!=2||in.Arguments[1].SQLNull{return BindResult{},fmt.Errorf("date_trunc requires temporal input")};t:=in.Arguments[1].Type;if t.ID!=types.DateType&&t.ID!=types.TimestampType{return BindResult{},fmt.Errorf("date_trunc expects DATE or TIMESTAMP")};return BindResult{ReturnType:t,Arguments:[]types.LogicalType{types.LogicalType{ID:types.Varchar},t}},nil}
func bindTimezone(in BindInput)(BindResult,error){if len(in.Arguments)!=2||in.Arguments[1].SQLNull{return BindResult{},fmt.Errorf("timezone requires TIMESTAMP input")};t:=in.Arguments[1].Type;if t.ID!=types.TimestampType{return BindResult{},fmt.Errorf("timezone expects TIMESTAMP")};out:=t;out.Timestamp.AdjustedToUTC=!t.Timestamp.AdjustedToUTC;return BindResult{ReturnType:out,Arguments:[]types.LogicalType{types.LogicalType{ID:types.Varchar},t}},nil}
func phase4CommonType(args []ArgumentType,numericOnly bool)(types.LogicalType,error){var current types.LogicalType;have:=false;for _,arg:=range args{if arg.SQLNull{continue};if !have{current=arg.Type;have=true;continue};if current.Same(arg.Type){continue};if current.IsNumeric()&&arg.Type.IsNumeric(){var err error;current,err=phase4CommonNumeric(current,arg.Type);if err!=nil{return types.LogicalType{},err};continue};return types.LogicalType{},fmt.Errorf("incompatible scalar argument types %d and %d",current.ID,arg.Type.ID)};if !have{return types.LogicalType{},fmt.Errorf("cannot infer scalar type from only NULL arguments")};if numericOnly&&!current.IsNumeric(){return types.LogicalType{},fmt.Errorf("numeric function requires numeric input")};return current,nil}
func phase4CommonNumeric(a,b types.LogicalType)(types.LogicalType,error){if a.ID==types.Double||b.ID==types.Double{return types.LogicalType{ID:types.Double},nil};if a.ID==types.Float||b.ID==types.Float{return types.LogicalType{ID:types.Double},nil};if a.ID==types.Decimal||b.ID==types.Decimal{scale:=uint8(0);intDigits:=uint8(1);for _,t:=range []types.LogicalType{a,b}{if t.ID==types.Decimal{if t.Decimal.Scale>scale{scale=t.Decimal.Scale};d:=t.Decimal.Width-t.Decimal.Scale;if d>intDigits{intDigits=d}}else{d:=numericIntegerDigits(t);if d>intDigits{intDigits=d}}};width:=intDigits+scale;if width>38{width=38};return types.NewDecimal(width,scale)};if a.ID==types.HugeInt||b.ID==types.HugeInt{return types.LogicalType{ID:types.HugeInt},nil};if a.Unsigned!=b.Unsigned{return types.LogicalType{},fmt.Errorf("mixed signed/unsigned arithmetic requires explicit CAST")};order:=map[types.LogicalTypeID]int{types.TinyInt:1,types.SmallInt:2,types.Integer:3,types.BigInt:4};id:=a.ID;if order[b.ID]>order[id]{id=b.ID};return types.LogicalType{ID:id,Unsigned:a.Unsigned},nil}
func numericIntegerDigits(t types.LogicalType)uint8{switch t.ID{case types.TinyInt:return 3;case types.SmallInt:return 5;case types.Integer:return 10;case types.BigInt:return 20;case types.HugeInt:return 38;default:return 1}}
func registerPhase4Casts(c *Catalog){next:=FirstCastFunctionID;register:=func(name string,target types.LogicalType){id:=next;next++;if err:=c.Register(Definition{ID:id,Name:"__fssql_cast_"+name,Kind:Scalar,MinArgs:1,MaxArgs:1,Bind:func(in BindInput)(BindResult,error){if len(in.Arguments)!=1{return BindResult{},fmt.Errorf("CAST expects one argument")};if in.Arguments[0].SQLNull{return BindResult{ReturnType:target,Arguments:[]types.LogicalType{target}},nil};return BindResult{ReturnType:target,Arguments:[]types.LogicalType{in.Arguments[0].Type}},nil}});err!=nil{panic(err)}};register("boolean",types.LogicalType{ID:types.Boolean});register("tinyint",types.LogicalType{ID:types.TinyInt});register("smallint",types.LogicalType{ID:types.SmallInt});register("integer",types.LogicalType{ID:types.Integer});register("bigint",types.LogicalType{ID:types.BigInt});register("utinyint",types.UTinyIntType());register("usmallint",types.USmallIntType());register("uinteger",types.UIntegerType());register("ubigint",types.UBigIntType());register("float",types.LogicalType{ID:types.Float});register("double",types.LogicalType{ID:types.Double});register("varchar",types.LogicalType{ID:types.Varchar});register("blob",types.BlobType());register("date",types.LogicalType{ID:types.DateType});register("timestamp",types.LogicalType{ID:types.TimestampType});register("timestamp_ns",types.TimestampNSType());register("timestamptz",types.TimestampTZType());for width:=uint8(1);width<=38;width++{for scale:=uint8(0);scale<=width;scale++{t,_:=types.NewDecimal(width,scale);register(fmt.Sprintf("decimal_%d_%d",width,scale),t)}};register("hugeint",types.LogicalType{ID:types.HugeInt})}

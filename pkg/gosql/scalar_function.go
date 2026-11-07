package gosql

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"strings"
	"time"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/execution/expression"
	"github.com/steamwo/fssql/internal/function"
)

// ScalarNullHandling controls whether a registered UDF sees NULL arguments.
type ScalarNullHandling uint8
const (
	ScalarNullPropagate ScalarNullHandling = iota // any NULL input produces NULL without invoking Eval
	ScalarNullCall                                // Eval receives invalid ScalarValue arguments
)

// ScalarValue is the public typed UDF value. Exactly the field appropriate for
// Type is meaningful. Variable-width input data is copied before Eval returns;
// returned String/Bytes are copied into managed vector storage immediately.
type ScalarValue struct {
	Type    Type
	Valid   bool
	Bool    bool
	Int64   int64
	Uint64  uint64
	Float64 float64
	Int128  Int128
	String  string
	Bytes   []byte
	Decimal string
	Time    time.Time
}

// ScalarFunction 是不可变的 DB 作用域元数据与标量回调。
// Open 注册时不会把回调或密钥写入 SQL/计划状态。
type ScalarFunction struct {
	Name          string
	Arguments     []Type
	Result        Type
	NullHandling  ScalarNullHandling
	Deterministic bool
	Eval          func([]ScalarValue) (ScalarValue, error)
}

// NewHMACSHA256ScalarFunction 创建替换遮蔽 UDF，输入一个 VARCHAR 的原始字节并输出小写十六进制 HMAC。
// key 会复制到定义中，且仅用于当前 DB 配置。
func NewHMACSHA256ScalarFunction(key []byte) (ScalarFunction, error) {
	if len(key) == 0 {
		return ScalarFunction{}, fmt.Errorf("gosql: HMAC key is required")
	}
	owned := append([]byte(nil), key...)
	// 每个工厂定义最多保留一个已 Reset 的空闲 hash；获取和归还均不阻塞并发调用。
	states := make(chan hash.Hash, 1)
	states <- hmac.New(sha256.New, owned)
	return ScalarFunction{
		Name:          "data_plane_hmac_sha256",
		Arguments:     []Type{{ID: "VARCHAR"}},
		Result:        Type{ID: "VARCHAR"},
		NullHandling:  ScalarNullPropagate,
		Deterministic: true,
		Eval: func(values []ScalarValue) (ScalarValue, error) {
			if len(values) != 1 || !values[0].Valid {
				return ScalarValue{Type: Type{ID: "VARCHAR"}, Valid: false}, nil
			}
			var mac hash.Hash
			select {
			case mac = <-states:
			default:
				mac = hmac.New(sha256.New, owned)
			}
			defer func() {
				mac.Reset()
				select {
				case states <- mac:
				default:
				}
			}()
			_, _ = mac.Write([]byte(values[0].String))
			// digest 和 encoded 均为调用局部缓冲，结果字符串在返回前独立复制。
			var digest [sha256.Size]byte
			var encoded [sha256.Size * 2]byte
			hex.Encode(encoded[:], mac.Sum(digest[:0]))
			return ScalarValue{Type: Type{ID: "VARCHAR"}, Valid: true, String: string(encoded[:])}, nil
		},
	}, nil
}

// registerScalarFunctions 注册公共标量定义，并通过统一运行时适配器保留当前 Eval 回调语义。
func registerScalarFunctions(base *function.Catalog, definitions []ScalarFunction) (*function.Catalog, expression.ScalarRegistry, error) {
	catalog:=base;if catalog==nil{catalog=function.NewScalarCatalog()}
	registry:=make(expression.ScalarRegistry,len(definitions));seen:=make(map[string]struct{},len(definitions))
	for i,definition:=range definitions{
		name:=strings.TrimSpace(definition.Name);if !validFunctionIdentifier(name){return nil,nil,fmt.Errorf("gosql: invalid scalar function name %q",definition.Name)};key:=strings.ToLower(name);if _,ok:=seen[key];ok{return nil,nil,fmt.Errorf("gosql: duplicate scalar function %q",name)};seen[key]=struct{}{}
		if definition.Eval==nil{return nil,nil,fmt.Errorf("gosql: scalar function %s has nil Eval",name)}
		args:=make([]types.LogicalType,len(definition.Arguments));for j,arg:=range definition.Arguments{t,err:=internalTableFunctionType(arg);if err!=nil{return nil,nil,fmt.Errorf("gosql: scalar function %s argument %d: %w",name,j,err)};args[j]=t}
		result,err:=internalTableFunctionType(definition.Result);if err!=nil{return nil,nil,fmt.Errorf("gosql: scalar function %s result: %w",name,err)}
		id:=function.FirstDynamicScalarID+function.ID(i)
		argCopy:=append([]types.LogicalType(nil),args...);resultCopy:=result
		next,err:=catalog.With(function.Definition{ID:id,Name:name,Kind:function.Scalar,MinArgs:len(args),MaxArgs:len(args),Bind:func(in function.BindInput)(function.BindResult,error){if len(in.Arguments)!=len(argCopy){return function.BindResult{},fmt.Errorf("scalar function expects %d arguments",len(argCopy))};return function.BindResult{ReturnType:resultCopy,Arguments:append([]types.LogicalType(nil),argCopy...)},nil}});if err!=nil{return nil,nil,fmt.Errorf("gosql: register scalar function %s: %w",name,err)};catalog=next
		nullMode:=expression.ScalarNullPropagate;if definition.NullHandling==ScalarNullCall{nullMode=expression.ScalarNullCall}
		defCopy := definition
		runtime := expression.ScalarRuntime{
			Deterministic: definition.Deterministic,
			NullHandling:  nullMode,
			Eval: func(values []expression.ScalarValue) (expression.ScalarValue, error) {
				public := make([]ScalarValue, len(values))
				for j, v := range values {
					public[j] = publicScalarValue(v)
				}
				got, err := defCopy.Eval(public)
				if err != nil {
					return expression.ScalarValue{}, err
				}
				return internalScalarValue(got, resultCopy)
			},
		}
		registry[id] = runtime
	}
	return catalog,registry,nil
}

func publicScalarValue(v expression.ScalarValue) ScalarValue {
	out:=ScalarValue{Type:publicType(v.Type),Valid:v.Valid};if !v.Valid{return out};if v.Type.IsUnsignedInteger(){out.Uint64=v.Uint64;return out}
	switch v.Type.ID{case types.Boolean:out.Bool=v.Bool;case types.TinyInt,types.SmallInt,types.Integer,types.BigInt:out.Int64=v.Int64;case types.HugeInt:out.Int128=Int128{Lo:v.Int128.Lo,Hi:v.Int128.Hi};case types.Float,types.Double:out.Float64=v.Float64;case types.Decimal:out.Decimal=types.FormatDecimalText(v.Int128,v.Type.Decimal.Scale);case types.Varchar:if v.Type.Binary{out.Bytes=append([]byte(nil),v.Bytes...)}else{out.String=string(v.Bytes)};case types.DateType:out.Time=time.Unix(v.Int64*86400,0).UTC();case types.TimestampType:switch v.Type.Timestamp.Unit{case types.TimestampNanos:out.Time=time.Unix(0,v.Int64).UTC();case types.TimestampMillis:out.Time=time.UnixMilli(v.Int64).UTC();default:out.Time=time.UnixMicro(v.Int64).UTC()}};return out
}

func internalScalarValue(v ScalarValue,target types.LogicalType)(expression.ScalarValue,error){out:=expression.ScalarValue{Type:target,Valid:v.Valid};if !v.Valid{return out,nil};if target.IsUnsignedInteger(){out.Uint64=v.Uint64;return out,nil};switch target.ID{case types.Boolean:out.Bool=v.Bool;case types.TinyInt,types.SmallInt,types.Integer,types.BigInt:out.Int64=v.Int64;case types.HugeInt:out.Int128=types.Int128{Lo:v.Int128.Lo,Hi:v.Int128.Hi};case types.Float,types.Double:out.Float64=v.Float64;case types.Decimal:parsed,err:=types.ParseDecimalText(v.Decimal,target.Decimal.Width,target.Decimal.Scale);if err!=nil{return expression.ScalarValue{},err};out.Int128=parsed;case types.Varchar:if target.Binary{out.Bytes=append([]byte(nil),v.Bytes...)}else{out.Bytes=[]byte(v.String)};case types.DateType:y,m,d:=v.Time.Date();out.Int64=time.Date(y,m,d,0,0,0,0,time.UTC).Unix()/86400;case types.TimestampType:switch target.Timestamp.Unit{case types.TimestampNanos:out.Int64=v.Time.UnixNano();case types.TimestampMillis:out.Int64=v.Time.UnixMilli();default:out.Int64=v.Time.UnixMicro()};default:return expression.ScalarValue{},fmt.Errorf("gosql: scalar result type %s is unsupported",logicalTypeNameOf(target))};return out,nil}

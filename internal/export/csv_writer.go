package export

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

type CSVWriteOptions struct {
	Delimiter  byte
	Quote      byte
	Newline    string
	Header     bool
	NullString string
	BufferSize int
}

type CSVWriter struct {
	schema  []Column
	opts    CSVWriteOptions
	out     *managedOutput
	started bool
}

func NewCSVWriter(schema []Column, opts CSVWriteOptions, scope *memory.Scope, bm *memory.BufferManager, sink ByteSink) (*CSVWriter, error) {
	if len(schema) == 0 { return nil, fmt.Errorf("export: CSV schema is empty") }
	if opts.Delimiter == 0 { opts.Delimiter = ',' }
	if opts.Quote == 0 { opts.Quote = '"' }
	if opts.Newline == "" { opts.Newline = "\n" }
	if opts.BufferSize <= 0 { opts.BufferSize = 64 << 10 }
	if opts.Delimiter == opts.Quote || strings.ContainsRune(opts.Newline,rune(opts.Delimiter)) || strings.ContainsRune(opts.Newline,rune(opts.Quote)) { return nil,fmt.Errorf("export: invalid CSV delimiter/quote/newline combination") }
	out,err:=newManagedOutput(scope,bm,sink,opts.BufferSize);if err!=nil{return nil,err}
	return &CSVWriter{schema:append([]Column(nil),schema...),opts:opts,out:out},nil
}
func(w *CSVWriter)WriteChunk(ctx context.Context,chunk *vector.DataChunk)error{if w==nil||w.out==nil||w.out.terminal{return ErrWriterClosed};if err:=validateChunkSchema(w.schema,chunk);err!=nil{return err};if err:=w.ensureStarted(ctx);err!=nil{return err};for row:=0;row<chunk.Cardinality;row++{for col:=range w.schema{if col!=0{if err:=w.out.WriteContextByte(ctx,w.opts.Delimiter);err!=nil{return err}};if err:=w.writeField(ctx,chunk.Columns[col],w.schema[col].Type,row);err!=nil{return fmt.Errorf("export: CSV row %d column %d: %w",row+1,col+1,err)}};if err:=w.out.WriteString(ctx,w.opts.Newline);err!=nil{return err}};return nil}
func(w *CSVWriter)Flush(ctx context.Context)error{if w==nil||w.out==nil{return ErrWriterClosed};if err:=w.ensureStarted(ctx);err!=nil{return err};return w.out.Flush(ctx)}
func(w *CSVWriter)Close(ctx context.Context)(WriteResult,error){if w==nil||w.out==nil{return WriteResult{},ErrWriterClosed};if err:=w.ensureStarted(ctx);err!=nil{_ = w.out.Abort(context.Background());return WriteResult{},err};return w.out.Close(ctx)}
func(w *CSVWriter)Abort(ctx context.Context)error{if w==nil||w.out==nil{return nil};return w.out.Abort(ctx)}
func(w *CSVWriter)ensureStarted(ctx context.Context)error{if w.started{return nil};w.started=true;if !w.opts.Header{return nil};for i:=range w.schema{if i!=0{if err:=w.out.WriteContextByte(ctx,w.opts.Delimiter);err!=nil{return err}};if err:=w.writeStringField(ctx,w.schema[i].Name,false);err!=nil{return err}};return w.out.WriteString(ctx,w.opts.Newline)}

func(w *CSVWriter)writeField(ctx context.Context,vec *vector.Vector,typ types.LogicalType,row int)error{
	if !vec.IsValid(row){return w.out.WriteString(ctx,w.opts.NullString)}
	if typ.IsBlob(){b,_:=vec.Bytes(row);if len(b)==0{return w.writeStringField(ctx,"",w.opts.NullString=="")};const digits="0123456789ABCDEF";for _,v:=range b{if err:=w.out.WriteContextByte(ctx,digits[v>>4]);err!=nil{return err};if err:=w.out.WriteContextByte(ctx,digits[v&0x0f]);err!=nil{return err}};return nil}
	if typ.ID==types.Varchar{b,_:=vec.Bytes(row);return w.writeStringField(ctx,string(b),w.opts.NullString==""&&len(b)==0)}
	if typ.ID==types.List||typ.ID==types.Struct{if err:=w.out.WriteContextByte(ctx,w.opts.Quote);err!=nil{return err};quoted:=csvQuotedOutput{out:w.out,quote:w.opts.Quote};if err:=writeDuckDBTextValue(ctx,&quoted,vec,typ,row);err!=nil{return err};return w.out.WriteContextByte(ctx,w.opts.Quote)}
	return writeCSVScalar(ctx,w.out,vec,typ,row)
}
func(w *CSVWriter)writeStringField(ctx context.Context,value string,forceQuote bool)error{needsQuote:=forceQuote||strings.IndexByte(value,w.opts.Delimiter)>=0||strings.IndexByte(value,w.opts.Quote)>=0||strings.ContainsAny(value,"\r\n")||value==w.opts.NullString;if !needsQuote{return w.out.WriteString(ctx,value)};if err:=w.out.WriteContextByte(ctx,w.opts.Quote);err!=nil{return err};for start:=0;;{i:=strings.IndexByte(value[start:],w.opts.Quote);if i<0{if err:=w.out.WriteString(ctx,value[start:]);err!=nil{return err};break};i+=start;if err:=w.out.WriteString(ctx,value[start:i]);err!=nil{return err};if err:=w.out.WriteContextByte(ctx,w.opts.Quote);err!=nil{return err};if err:=w.out.WriteContextByte(ctx,w.opts.Quote);err!=nil{return err};start=i+1};return w.out.WriteContextByte(ctx,w.opts.Quote)}

func writeCSVScalar(ctx context.Context,out valueOutput,vec *vector.Vector,typ types.LogicalType,row int)error{
	var tmp [96]byte
	if typ.IsUnsignedInteger(){switch typ.ID{case types.TinyInt:v,_:=vec.Uint8(row);return out.Write(ctx,strconv.AppendUint(tmp[:0],uint64(v),10));case types.SmallInt:v,_:=vec.Uint16(row);return out.Write(ctx,strconv.AppendUint(tmp[:0],uint64(v),10));case types.Integer:v,_:=vec.Uint32(row);return out.Write(ctx,strconv.AppendUint(tmp[:0],uint64(v),10));case types.BigInt:v,_:=vec.Uint64(row);return out.Write(ctx,strconv.AppendUint(tmp[:0],v,10))}}
	switch typ.ID{
	case types.Boolean:v,_:=vec.Bool(row);if v{return out.WriteString(ctx,"true")};return out.WriteString(ctx,"false")
	case types.TinyInt:v,_:=vec.Int8(row);return out.Write(ctx,strconv.AppendInt(tmp[:0],int64(v),10))
	case types.SmallInt:v,_:=vec.Int16(row);return out.Write(ctx,strconv.AppendInt(tmp[:0],int64(v),10))
	case types.Integer:v,_:=vec.Int32(row);return out.Write(ctx,strconv.AppendInt(tmp[:0],int64(v),10))
	case types.BigInt:v,_:=vec.Int64(row);return out.Write(ctx,strconv.AppendInt(tmp[:0],v,10))
	case types.HugeInt:v,_:=vec.Int128(row);return out.Write(ctx,appendInt128(tmp[:0],v))
	case types.Float:v,_:=vec.Float32(row);return out.Write(ctx,strconv.AppendFloat(tmp[:0],float64(v),'g',-1,32))
	case types.Double:v,_:=vec.Float64(row);return out.Write(ctx,strconv.AppendFloat(tmp[:0],v,'g',-1,64))
	case types.Decimal:if typ.Decimal.Width<=18{v,_:=vec.Int64(row);return out.Write(ctx,appendScaledInt64(tmp[:0],v,int(typ.Decimal.Scale)))};v,_:=vec.Int128(row);return out.Write(ctx,appendScaledInt128(tmp[:0],v,int(typ.Decimal.Scale)))
	case types.DateType:v,_:=vec.Int32(row);t:=time.Unix(int64(v)*86400,0).UTC();return out.Write(ctx,t.AppendFormat(tmp[:0],"2006-01-02"))
	case types.TimestampType:v,_:=vec.Int64(row);return out.Write(ctx,appendTimestampText(tmp[:0],v,typ))
	default:return fmt.Errorf("unsupported CSV scalar type %d",typ.ID)}
}

func appendTimestampText(dst []byte,raw int64,typ types.LogicalType)[]byte{var t time.Time;var layout string;switch typ.Timestamp.Unit{case types.TimestampNanos:sec:=raw/1_000_000_000;nanos:=raw%1_000_000_000;t=time.Unix(sec,nanos).UTC();layout="2006-01-02 15:04:05.999999999";case types.TimestampMillis:t=time.UnixMilli(raw).UTC();layout="2006-01-02 15:04:05.999";default:t=time.UnixMicro(raw).UTC();layout="2006-01-02 15:04:05.999999"};if typ.Timestamp.AdjustedToUTC{return t.AppendFormat(dst,time.RFC3339Nano)};return t.AppendFormat(dst,layout)}

type csvQuotedOutput struct{out *managedOutput;quote byte}
func(q *csvQuotedOutput)Write(ctx context.Context,p []byte)error{for _,b:=range p{if err:=q.WriteContextByte(ctx,b);err!=nil{return err}};return nil}
func(q *csvQuotedOutput)WriteString(ctx context.Context,s string)error{for i:=0;i<len(s);i++{if err:=q.WriteContextByte(ctx,s[i]);err!=nil{return err}};return nil}
func(q *csvQuotedOutput)WriteContextByte(ctx context.Context,b byte)error{if err:=q.out.WriteContextByte(ctx,b);err!=nil{return err};if b==q.quote{return q.out.WriteContextByte(ctx,b)};return nil}

func writeDuckDBTextValue(ctx context.Context,out valueOutput,vec *vector.Vector,typ types.LogicalType,row int)error{
	if !vec.IsValid(row){return out.WriteString(ctx,"NULL")}
	if typ.ID==types.Varchar{b,_:=vec.Bytes(row);if err:=out.WriteContextByte(ctx,'\'');err!=nil{return err};for i:=0;i<len(b);i++{if b[i]=='\''{if err:=out.WriteContextByte(ctx,'\'');err!=nil{return err}};if err:=out.WriteContextByte(ctx,b[i]);err!=nil{return err}};return out.WriteContextByte(ctx,'\'')}
	if typ.ID==types.List{entry,_:=vec.ListEntry(row);children:=vec.Children();if typ.Child==nil||len(children)!=1{return fmt.Errorf("malformed LIST vector")};if err:=out.WriteContextByte(ctx,'[');err!=nil{return err};for i:=uint32(0);i<entry.Length;i++{if i>0{if err:=out.WriteString(ctx,", ");err!=nil{return err}};if err:=writeDuckDBTextValue(ctx,out,children[0],*typ.Child,int(entry.Offset+i));err!=nil{return err}};return out.WriteContextByte(ctx,']')}
	if typ.ID==types.Struct{children:=vec.Children();if len(children)!=len(typ.Fields){return fmt.Errorf("malformed STRUCT vector")};if err:=out.WriteContextByte(ctx,'{');err!=nil{return err};for i,f:=range typ.Fields{if i>0{if err:=out.WriteString(ctx,", ");err!=nil{return err}};if err:=out.WriteContextByte(ctx,'\'');err!=nil{return err};if err:=out.WriteString(ctx,f.Name);err!=nil{return err};if err:=out.WriteString(ctx,"': ");err!=nil{return err};if err:=writeDuckDBTextValue(ctx,out,children[i],f.Type,row);err!=nil{return err}};return out.WriteContextByte(ctx,'}')}
	if typ.ID==types.DateType||typ.ID==types.TimestampType{if err:=out.WriteContextByte(ctx,'\'');err!=nil{return err};if err:=writeCSVScalar(ctx,out,vec,typ,row);err!=nil{return err};return out.WriteContextByte(ctx,'\'')}
	if typ.ID==types.Float{v,_:=vec.Float32(row);if math.IsNaN(float64(v)){return out.WriteString(ctx,"nan")}}
	if typ.ID==types.Double{v,_:=vec.Float64(row);if math.IsNaN(v){return out.WriteString(ctx,"nan")}}
	return writeCSVScalar(ctx,out,vec,typ,row)
}

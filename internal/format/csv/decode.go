package csv

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

type DecodeOptions struct {
	Lex         LexConfig
	Final       bool
	NullLiteral []byte
	Projection  []int
	MaxRows     int
}

func DecodeChunk(input []byte, schema []types.LogicalType, opts DecodeOptions, scope *memory.Scope, bm *memory.BufferManager) (*vector.DataChunk, int, error) {
	if scope == nil || bm == nil { return nil, 0, fmt.Errorf("csv: managed memory scope and buffer manager are required") }
	if len(schema) == 0 { return nil, 0, fmt.Errorf("csv: schema must contain at least one column") }
	for _, index := range opts.Projection { if index < 0 || index >= len(schema) { return nil, 0, fmt.Errorf("csv: projection index %d out of range", index) } }
	if opts.Lex == (LexConfig{}) { opts.Lex = DefaultLexConfig() }
	maxRows := opts.MaxRows
	if maxRows <= 0 || maxRows > vector.DefaultVectorSize { maxRows = vector.DefaultVectorSize }
	rows, consumed, err := countChunkRows(input, opts.Lex, opts.Final, maxRows)
	if err != nil { return nil, 0, err }
	if rows == 0 { return nil, consumed, nil }
	cols := make([]*vector.Vector, 0, len(schema))
	cleanup := func() { for _, col := range cols { col.Release() } }
	for index, typ := range schema {
		var col *vector.Vector
		if decodeColumnSelected(opts.Projection, index) { col, err = vector.NewFlat(scope, bm, typ, rows) } else { col, err = vector.NewConstant(scope, bm, typ, rows); if err == nil { col.SetNull(0) } }
		if err != nil { cleanup(); return nil, 0, err }
		cols = append(cols, col)
	}
	chunk, err := vector.NewDataChunk(cols...)
	if err != nil { cleanup(); return nil, 0, err }
	cols = nil
	if err := decodeRows(input[:consumed], schema, opts, chunk, scope, bm); err != nil { chunk.Release(); return nil, 0, err }
	return chunk, consumed, nil
}

func countChunkRows(input []byte, cfg LexConfig, final bool, maxRows int) (rows, consumed int, err error) {
	m, err := NewMachine(cfg); if err != nil { return 0, 0, err }
	lastRecordStart := 0
	for i := 0; i < len(input); i++ {
		if n := m.advanceData(input[i:]); n > 0 { i += n - 1; continue }
		event, stepErr := m.Step(input[i]); if stepErr != nil { return 0, 0, stepErr }
		if event.Has(EventRecordEnd) { rows++; consumed = i + 1; if input[i] == '\r' && i+1 < len(input) && input[i+1] == '\n' { consumed++ }; lastRecordStart = consumed; if rows == maxRows { return rows, consumed, nil } }
	}
	if !final { return rows, consumed, nil }
	if err := m.Finish(); err != nil { return 0, 0, err }
	if lastRecordStart < len(input) { rows++; consumed = len(input) }
	return rows, consumed, nil
}

func decodeRows(input []byte, schema []types.LogicalType, opts DecodeOptions, chunk *vector.DataChunk, scope *memory.Scope, bm *memory.BufferManager) error {
	m, err := NewMachine(opts.Lex); if err != nil { return err }
	initial := 256; if len(input) < initial { initial = len(input) }; if initial < 1 { initial = 1 }
	var scratch *memory.Buffer
	defer func() { if scratch != nil { scratch.Release() } }()
	var scratchBytes []byte
	used, row, col := 0, 0, 0
	fieldStart, fieldEnd := 0, 0
	quotedField := false
	selected := make([]bool, len(schema)); for i := range selected { selected[i] = decodeColumnSelected(opts.Projection, i) }
	appendData := func(data []byte) error {
		if scratch == nil { scratch, err = bm.Allocate(scope, memory.TagCSVDecode, initial); if err != nil { return err }; scratchBytes = scratch.Bytes() }
		if len(data) > len(scratchBytes)-used { newCap := len(scratchBytes)*2; if newCap < used+len(data) { newCap=used+len(data) }; if newCap < 64 { newCap=64 }; if err := scratch.Resize(newCap,nil); err != nil { return err }; scratchBytes=scratch.Bytes() }
		copy(scratchBytes[used:],data); used+=len(data); return nil
	}
	finishField := func() error {
		if row >= chunk.Cardinality { return fmt.Errorf("csv: decoded more rows than planned") }
		if col >= len(schema) { return fmt.Errorf("csv: row %d has more than %d columns",row+1,len(schema)) }
		if selected[col] { raw:=scratchBytes[:used]; if !quotedField { raw=input[fieldStart:fieldEnd] }; if err:=decodeField(chunk.Columns[col],schema[col],row,raw,opts.NullLiteral); err!=nil { return fmt.Errorf("csv: row %d column %d: %w",row+1,col+1,err) } }
		used=0; col++; quotedField=false; return nil
	}
	finishRecord := func() error { if col != len(schema) { return fmt.Errorf("csv: row %d has %d columns, want %d",row+1,col,len(schema)) }; row++; col=0; return nil }
	for i:=0;i<len(input);i++ { fieldEnd=i; if i==fieldStart { quotedField=input[i]==opts.Lex.Quote }; if n:=m.advanceData(input[i:]); n>0 { if quotedField&&col<len(selected)&&selected[col] { if err:=appendData(input[i:i+n]);err!=nil{return err} }; i+=n-1;continue }; skipLF:=m.skipLF&&input[i]=='\n'; event,stepErr:=m.Step(input[i]);if stepErr!=nil{return stepErr};if skipLF{fieldStart=i+1};if event.Has(EventData)&&quotedField&&col<len(selected)&&selected[col]{if err:=appendData(input[i:i+1]);err!=nil{return err}};if event.Has(EventFieldEnd){if err:=finishField();err!=nil{return err};fieldStart=i+1};if event.Has(EventRecordEnd){if err:=finishRecord();err!=nil{return err}} }
	if err:=m.Finish();err!=nil{return err};if row<chunk.Cardinality{fieldEnd=len(input);if err:=finishField();err!=nil{return err};if err:=finishRecord();err!=nil{return err}};if row!=chunk.Cardinality{return fmt.Errorf("csv: decoded %d rows, planned %d",row,chunk.Cardinality)};return nil
}

func decodeColumnSelected(projection []int,column int)bool{if len(projection)==0{return true};for _,index:=range projection{if index==column{return true}};return false}

func decodeField(dst *vector.Vector, typ types.LogicalType, row int, raw, nullLiteral []byte) error {
	if nullLiteral != nil && bytes.Equal(raw,nullLiteral){dst.SetNull(row);return nil}
	if typ.IsUnsignedInteger(){bits:=64;switch typ.ID{case types.TinyInt:bits=8;case types.SmallInt:bits=16;case types.Integer:bits=32};value,err:=strconv.ParseUint(string(raw),10,bits);if err!=nil{return fmt.Errorf("invalid unsigned integer %q",raw)};switch typ.ID{case types.TinyInt:dst.SetUint8(row,uint8(value));case types.SmallInt:dst.SetUint16(row,uint16(value));case types.Integer:dst.SetUint32(row,uint32(value));case types.BigInt:dst.SetUint64(row,value)};return nil}
	if typ.IsBlob(){return dst.SetBytes(row,raw)}
	switch typ.ID{
	case types.Boolean:value,ok:=parseBool(raw);if !ok{return fmt.Errorf("invalid BOOLEAN %q",raw)};dst.SetBool(row,value)
	case types.TinyInt:value,err:=strconv.ParseInt(string(raw),10,8);if err!=nil{return fmt.Errorf("invalid TINYINT %q",raw)};dst.SetInt8(row,int8(value))
	case types.SmallInt:value,err:=strconv.ParseInt(string(raw),10,16);if err!=nil{return fmt.Errorf("invalid SMALLINT %q",raw)};dst.SetInt16(row,int16(value))
	case types.Integer:value,err:=strconv.ParseInt(string(raw),10,32);if err!=nil{return fmt.Errorf("invalid INTEGER %q",raw)};dst.SetInt32(row,int32(value))
	case types.BigInt:value,err:=strconv.ParseInt(string(raw),10,64);if err!=nil{return fmt.Errorf("invalid BIGINT %q",raw)};dst.SetInt64(row,value)
	case types.Float:value,err:=strconv.ParseFloat(string(raw),32);if err!=nil||math.IsInf(value,0){return fmt.Errorf("invalid FLOAT %q",raw)};dst.SetFloat32(row,float32(value))
	case types.Double:value,err:=strconv.ParseFloat(string(raw),64);if err!=nil||math.IsInf(value,0){return fmt.Errorf("invalid DOUBLE %q",raw)};dst.SetFloat64(row,value)
	case types.Varchar:return dst.SetStringBytes(row,raw)
	case types.DateType:t,err:=time.Parse("2006-01-02",string(raw));if err!=nil{return fmt.Errorf("invalid DATE %q",raw)};dst.SetInt32(row,int32(t.Unix()/86400))
	case types.TimestampType:value,err:=parseTimestamp(raw,typ);if err!=nil{return err};dst.SetInt64(row,value)
	default:return fmt.Errorf("unsupported decode type %d",typ.ID)}
	return nil
}

func parseTimestamp(raw []byte,typ types.LogicalType)(int64,error){s:=strings.TrimSpace(string(raw));var t time.Time;var err error;if typ.Timestamp.AdjustedToUTC{for _,layout:=range []string{time.RFC3339Nano,"2006-01-02 15:04:05.999999999Z07:00","2006-01-02 15:04:05Z07:00"}{t,err=time.Parse(layout,s);if err==nil{break}}}else{for _,layout:=range []string{"2006-01-02 15:04:05.999999999","2006-01-02 15:04:05",time.RFC3339Nano}{t,err=time.ParseInLocation(layout,s,time.UTC);if err==nil{break}}};if err!=nil{return 0,fmt.Errorf("invalid timestamp %q",raw)};switch typ.Timestamp.Unit{case types.TimestampNanos:return t.UnixNano(),nil;case types.TimestampMillis:return t.UnixMilli(),nil;default:return t.UnixMicro(),nil}}
func parseBool(raw []byte)(bool,bool){if bytes.EqualFold(raw,[]byte("true")){return true,true};if bytes.EqualFold(raw,[]byte("false")){return false,true};return false,false}

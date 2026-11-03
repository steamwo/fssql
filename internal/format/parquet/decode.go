package parquet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	coretypes "github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

func DecodePlainInto(dst *vector.Vector, start, count int, physical PhysicalType, data []byte) (int, error) {
	if dst == nil { return 0, errors.New("parquet: nil destination vector") }
	if start < 0 || count < 0 || start+count > dst.Count() { return 0, errors.New("parquet: destination bounds") }
	off:=0
	switch physical{
	case TypeBoolean:
		need:=(count+7)/8;if len(data)<need{return 0,errors.New("parquet: truncated boolean plain data")};for i:=0;i<count;i++{dst.SetBool(start+i,data[i>>3]&(1<<uint(i&7))!=0)};return need,nil
	case TypeInt32:
		need:=count*4;if len(data)<need{return 0,errors.New("parquet: truncated int32 plain data")};for i:=0;i<count;i++{raw:=binary.LittleEndian.Uint32(data[off:off+4]);if dst.Type.ID==coretypes.Decimal{if err:=setDecimalFromInt64(dst,start+i,int64(int32(raw)),9);err!=nil{return 0,err}}else if err:=setInt32Logical(dst,start+i,raw);err!=nil{return 0,err};off+=4}
	case TypeInt64:
		need:=count*8;if len(data)<need{return 0,errors.New("parquet: truncated int64 plain data")};for i:=0;i<count;i++{raw:=binary.LittleEndian.Uint64(data[off:off+8]);if dst.Type.ID==coretypes.Decimal{if err:=setDecimalFromInt64(dst,start+i,int64(raw),18);err!=nil{return 0,err}}else if dst.Type.IsUnsignedInteger(){dst.SetUint64(start+i,raw)}else{dst.SetInt64(start+i,int64(raw))};off+=8}
	case TypeFloat:
		need:=count*4;if len(data)<need{return 0,errors.New("parquet: truncated float plain data")};for i:=0;i<count;i++{dst.SetFloat32(start+i,math.Float32frombits(binary.LittleEndian.Uint32(data[off:off+4])));off+=4}
	case TypeDouble:
		need:=count*8;if len(data)<need{return 0,errors.New("parquet: truncated double plain data")};for i:=0;i<count;i++{dst.SetFloat64(start+i,math.Float64frombits(binary.LittleEndian.Uint64(data[off:off+8])));off+=8}
	case TypeByteArray:
		for i:=0;i<count;i++{if off+4>len(data){return 0,errors.New("parquet: truncated byte-array length")};n:=int(binary.LittleEndian.Uint32(data[off:off+4]));off+=4;if n<0||off+n>len(data){return 0,errors.New("parquet: truncated byte-array value")};if err:=dst.SetStringBytes(start+i,data[off:off+n]);err!=nil{return 0,err};off+=n}
	default:return 0,fmt.Errorf("parquet: PLAIN physical type %d not implemented",physical)}
	return off,nil
}

func CopyDictionaryIndexes(dst *vector.Vector,start int,dict *vector.Vector,indexes []uint32)error{if dst==nil||dict==nil{return errors.New("parquet: nil dictionary vector")};if start<0||start+len(indexes)>dst.Count(){return errors.New("parquet: dictionary destination bounds")};if !dst.Type.Same(dict.Type){return errors.New("parquet: dictionary logical type mismatch")};for i,idx:=range indexes{if int(idx)>=dict.Count(){return fmt.Errorf("parquet: dictionary index %d out of range %d",idx,dict.Count())};if err:=vector.CopyValue(dict,int(idx),dst,start+i);err!=nil{return err}};return nil}

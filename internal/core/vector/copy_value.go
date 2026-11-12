package vector

import (
	"errors"

	"github.com/steamwo/fssql/internal/core/types"
)

// CopyValue copies one scalar cell between own vectors without converting
// through interface{} or allocating per row.
func CopyValue(src *Vector, srcRow int, dst *Vector, dstRow int) error {
	if src == nil || dst == nil {
		return errors.New("nil vector")
	}
	if !src.Type.Same(dst.Type) {
		return errors.New("vector type mismatch")
	}
	if !src.IsValid(srcRow) {
		dst.SetNull(dstRow)
		return nil
	}
	if src.Type.IsUnsignedInteger() {
		switch src.Type.ID {
		case types.TinyInt:
			x, _ := src.Uint8(srcRow)
			dst.SetUint8(dstRow, x)
		case types.SmallInt:
			x, _ := src.Uint16(srcRow)
			dst.SetUint16(dstRow, x)
		case types.Integer:
			x, _ := src.Uint32(srcRow)
			dst.SetUint32(dstRow, x)
		case types.BigInt:
			x, _ := src.Uint64(srcRow)
			dst.SetUint64(dstRow, x)
		}
		return nil
	}
	switch src.Type.ID {
	case types.List:
		return copyListValue(src,srcRow,dst,dstRow)
	case types.Struct:
		base,index:=src.NestedRow(srcRow)
		if len(base.children)!=len(src.Type.Fields){return errors.New("invalid STRUCT children")}
		if len(dst.children)==0{children:=make([]*Vector,len(src.Type.Fields));for i,f:=range src.Type.Fields{v,err:=NewFlat(dst.scope,dst.bm,f.Type,dst.count);if err!=nil{for _,c:=range children{if c!=nil{c.Release()}};return err};children[i]=v};dst.SetChildren(children...)}
		for i,child:=range base.children{if err:=CopyValue(child,index,dst.children[i],dstRow);err!=nil{return err}};dst.validity.SetValid(dstRow,true)
	case types.Boolean:
		x, _ := src.Bool(srcRow)
		dst.SetBool(dstRow, x)
	case types.TinyInt:
		x, _ := src.Int8(srcRow)
		dst.SetInt8(dstRow, x)
	case types.SmallInt:
		x, _ := src.Int16(srcRow)
		dst.SetInt16(dstRow, x)
	case types.Integer, types.DateType:
		x, _ := src.Int32(srcRow)
		dst.SetInt32(dstRow, x)
	case types.BigInt, types.TimestampType:
		x, _ := src.Int64(srcRow)
		dst.SetInt64(dstRow, x)
	case types.Float:
		x, _ := src.Float32(srcRow)
		dst.SetFloat32(dstRow, x)
	case types.Double:
		x, _ := src.Float64(srcRow)
		dst.SetFloat64(dstRow, x)
	case types.HugeInt:
		x, _ := src.Int128(srcRow)
		dst.SetInt128(dstRow, x)
	case types.Decimal:
		if src.Type.Decimal.Width <= 18 {
			x, _ := src.Int64(srcRow)
			dst.SetInt64(dstRow, x)
		} else {
			x, _ := src.Int128(srcRow)
			dst.SetInt128(dstRow, x)
		}
	case types.Varchar:
		value, _ := src.Bytes(srcRow)
		return dst.SetStringBytes(dstRow, value)
	default:
		return errors.New("copy unsupported vector type")
	}
	return nil
}

func copyListValue(src *Vector,srcRow int,dst *Vector,dstRow int)error{
	entry,_:=src.ListEntry(srcRow);base,_:=src.NestedRow(srcRow);if src.Type.Child==nil||len(base.children)!=1{return errors.New("invalid LIST child")}
	need:=dst.listUsed+int(entry.Length);if uint64(need)>uint64(^uint32(0)){return errors.New("LIST child count overflow")}
	if len(dst.children)==0||dst.children[0].Count()<need{capacity:=32;if len(dst.children)>0{capacity=dst.children[0].Count()*2};if capacity<need{capacity=need};child,err:=NewFlat(dst.scope,dst.bm,*src.Type.Child,capacity);if err!=nil{return err};if len(dst.children)>0{old:=dst.children[0];for i:=0;i<dst.listUsed;i++{if err:=CopyValue(old,i,child,i);err!=nil{child.Release();return err}};old.Release()};dst.SetChildren(child)}
	for i:=0;i<int(entry.Length);i++{if err:=CopyValue(base.children[0],int(entry.Offset)+i,dst.children[0],dst.listUsed+i);err!=nil{return err}}
	dst.SetListEntry(dstRow,ListEntry{Offset:uint32(dst.listUsed),Length:entry.Length});dst.listUsed=need;dst.validity.SetValid(dstRow,true);return nil
}

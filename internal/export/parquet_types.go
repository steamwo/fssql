package export

import (
	"errors"
	"fmt"

	coretypes "github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/format/parquet"
)

// parquetPhysicalType 将核心逻辑类型映射到受控 Parquet physical type。
func parquetPhysicalType(typ coretypes.LogicalType) (parquet.PhysicalType, error) {
	switch typ.ID {
	case coretypes.Boolean:
		return parquet.TypeBoolean, nil
	case coretypes.TinyInt, coretypes.SmallInt, coretypes.Integer, coretypes.DateType:
		return parquet.TypeInt32, nil
	case coretypes.BigInt, coretypes.TimestampType:
		return parquet.TypeInt64, nil
	case coretypes.Float:
		return parquet.TypeFloat, nil
	case coretypes.Double:
		return parquet.TypeDouble, nil
	case coretypes.Decimal:
		if typ.Decimal.Width <= 18 {
			return parquet.TypeInt64, nil
		}
		return parquet.TypeFixedLenByteArray, nil
	case coretypes.Varchar:
		return parquet.TypeByteArray, nil
	case coretypes.HugeInt:
		return 0, errors.New("HUGEINT Parquet mapping is not defined without changing logical identity")
	case coretypes.List, coretypes.Struct:
		return 0, errors.New("nested Parquet export requires level-aware leaf lowering")
	default:
		return 0, fmt.Errorf("unsupported logical type %d", typ.ID)
	}
}

// parquetSchemaElement 生成一个 optional top-level scalar column schema element。
func parquetSchemaElement(col Column) (parquet.SchemaElement, error) {
	physical, err := parquetPhysicalType(col.Type)
	if err != nil {
		return parquet.SchemaElement{}, err
	}
	rep := parquet.Optional
	e := parquet.SchemaElement{Type: &physical, Repetition: &rep, Name: col.Name}
	switch col.Type.ID {
	case coretypes.TinyInt:
		e.Logical = &parquet.LogicalType{Kind: parquet.LogicalInteger, Integer: &parquet.IntegerLogical{BitWidth: 8, IsSigned: true}}
	case coretypes.SmallInt:
		e.Logical = &parquet.LogicalType{Kind: parquet.LogicalInteger, Integer: &parquet.IntegerLogical{BitWidth: 16, IsSigned: true}}
	case coretypes.Integer:
		e.Logical = &parquet.LogicalType{Kind: parquet.LogicalInteger, Integer: &parquet.IntegerLogical{BitWidth: 32, IsSigned: true}}
	case coretypes.BigInt:
		e.Logical = &parquet.LogicalType{Kind: parquet.LogicalInteger, Integer: &parquet.IntegerLogical{BitWidth: 64, IsSigned: true}}
	case coretypes.Varchar:
		conv := parquet.ConvertedUTF8
		e.Converted = &conv
		e.Logical = &parquet.LogicalType{Kind: parquet.LogicalString}
	case coretypes.DateType:
		conv := parquet.ConvertedDate
		e.Converted = &conv
		e.Logical = &parquet.LogicalType{Kind: parquet.LogicalDate}
	case coretypes.TimestampType:
		conv := parquet.ConvertedTimestampMicros
		e.Converted = &conv
		e.Logical = &parquet.LogicalType{Kind: parquet.LogicalTimestamp, Timestamp: &parquet.TimestampLogical{IsAdjustedToUTC: false, Unit: parquet.UnitMicros}}
	case coretypes.Decimal:
		conv := parquet.ConvertedDecimal
		scale, precision := int32(col.Type.Decimal.Scale), int32(col.Type.Decimal.Width)
		e.Converted, e.Scale, e.Precision = &conv, &scale, &precision
		e.Logical = &parquet.LogicalType{Kind: parquet.LogicalDecimal, Decimal: &parquet.DecimalLogical{Scale: scale, Precision: precision}}
		if col.Type.Decimal.Width > 18 {
			length := int32(16)
			e.TypeLength = &length
		}
	}
	return e, nil
}

// copyParquetScalar 把一个输入 Vector 值复制到 row-group managed vector；NULL 只复制 validity。
func copyParquetScalar(dst *vector.Vector, dstRow int, src *vector.Vector, srcRow int, typ coretypes.LogicalType) error {
	if !src.IsValid(srcRow) {
		dst.SetNull(dstRow)
		return nil
	}
	switch typ.ID {
	case coretypes.Boolean:
		v, _ := src.Bool(srcRow)
		dst.SetBool(dstRow, v)
	case coretypes.TinyInt:
		v, _ := src.Int8(srcRow)
		dst.SetInt8(dstRow, v)
	case coretypes.SmallInt:
		v, _ := src.Int16(srcRow)
		dst.SetInt16(dstRow, v)
	case coretypes.Integer, coretypes.DateType:
		v, _ := src.Int32(srcRow)
		dst.SetInt32(dstRow, v)
	case coretypes.BigInt, coretypes.TimestampType:
		v, _ := src.Int64(srcRow)
		dst.SetInt64(dstRow, v)
	case coretypes.Float:
		v, _ := src.Float32(srcRow)
		dst.SetFloat32(dstRow, v)
	case coretypes.Double:
		v, _ := src.Float64(srcRow)
		dst.SetFloat64(dstRow, v)
	case coretypes.Decimal:
		if typ.Decimal.Width <= 18 {
			v, _ := src.Int64(srcRow)
			dst.SetInt64(dstRow, v)
		} else {
			v, _ := src.Int128(srcRow)
			dst.SetInt128(dstRow, v)
		}
	case coretypes.Varchar:
		v, _ := src.String(srcRow)
		return dst.SetString(dstRow, v)
	default:
		return fmt.Errorf("unsupported scalar type %d", typ.ID)
	}
	return nil
}

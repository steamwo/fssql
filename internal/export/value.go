package export

import (
	"context"
	"fmt"
	"math"
	"math/bits"
	"strconv"
	"time"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

// valueOutput 是值编码器需要的最小流式输出接口。
type valueOutput interface {
	Write(context.Context, []byte) error
	WriteByte(context.Context, byte) error
	WriteString(context.Context, string) error
}

// writeJSONValue 将一个核心 Vector 值直接编码到流，不建立 map[string]any 或 per-row JSON 对象。
func writeJSONValue(ctx context.Context, out valueOutput, vec *vector.Vector, typ types.LogicalType, row int) error {
	if !vec.IsValid(row) {
		return out.WriteString(ctx, "null")
	}
	var tmp [96]byte
	switch typ.ID {
	case types.Boolean:
		v, _ := vec.Bool(row)
		if v {
			return out.WriteString(ctx, "true")
		}
		return out.WriteString(ctx, "false")
	case types.TinyInt:
		v, _ := vec.Int8(row)
		return out.Write(ctx, strconv.AppendInt(tmp[:0], int64(v), 10))
	case types.SmallInt:
		v, _ := vec.Int16(row)
		return out.Write(ctx, strconv.AppendInt(tmp[:0], int64(v), 10))
	case types.Integer:
		v, _ := vec.Int32(row)
		return out.Write(ctx, strconv.AppendInt(tmp[:0], int64(v), 10))
	case types.BigInt:
		v, _ := vec.Int64(row)
		return out.Write(ctx, strconv.AppendInt(tmp[:0], v, 10))
	case types.HugeInt:
		v, _ := vec.Int128(row)
		return out.Write(ctx, appendInt128(tmp[:0], v))
	case types.Float:
		v, _ := vec.Float32(row)
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return fmt.Errorf("export: non-finite FLOAT cannot be encoded as JSON")
		}
		return out.Write(ctx, strconv.AppendFloat(tmp[:0], float64(v), 'g', -1, 32))
	case types.Double:
		v, _ := vec.Float64(row)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("export: non-finite DOUBLE cannot be encoded as JSON")
		}
		return out.Write(ctx, strconv.AppendFloat(tmp[:0], v, 'g', -1, 64))
	case types.Decimal:
		if typ.Decimal.Width <= 18 {
			v, _ := vec.Int64(row)
			return out.Write(ctx, appendScaledInt64(tmp[:0], v, int(typ.Decimal.Scale)))
		}
		v, _ := vec.Int128(row)
		return out.Write(ctx, appendScaledInt128(tmp[:0], v, int(typ.Decimal.Scale)))
	case types.Varchar:
		v, _ := vec.String(row)
		return writeJSONString(ctx, out, v)
	case types.DateType:
		v, _ := vec.Int32(row)
		t := time.Unix(int64(v)*86400, 0).UTC()
		buf := t.AppendFormat(tmp[:0], "2006-01-02")
		if err := out.WriteByte(ctx, '"'); err != nil {
			return err
		}
		if err := out.Write(ctx, buf); err != nil {
			return err
		}
		return out.WriteByte(ctx, '"')
	case types.TimestampType:
		v, _ := vec.Int64(row)
		t := time.UnixMicro(v).UTC()
		buf := t.AppendFormat(tmp[:0], "2006-01-02 15:04:05.999999")
		if err := out.WriteByte(ctx, '"'); err != nil {
			return err
		}
		if err := out.Write(ctx, buf); err != nil {
			return err
		}
		return out.WriteByte(ctx, '"')
	case types.List:
		entry, _ := vec.ListEntry(row)
		children := vec.Children()
		if typ.Child == nil || len(children) != 1 {
			return fmt.Errorf("export: malformed LIST vector")
		}
		if err := out.WriteByte(ctx, '['); err != nil {
			return err
		}
		for i := uint32(0); i < entry.Length; i++ {
			if i != 0 {
				if err := out.WriteByte(ctx, ','); err != nil {
					return err
				}
			}
			if err := writeJSONValue(ctx, out, children[0], *typ.Child, int(entry.Offset+i)); err != nil {
				return err
			}
		}
		return out.WriteByte(ctx, ']')
	case types.Struct:
		children := vec.Children()
		if len(children) != len(typ.Fields) {
			return fmt.Errorf("export: malformed STRUCT vector")
		}
		if err := out.WriteByte(ctx, '{'); err != nil {
			return err
		}
		for i := range typ.Fields {
			if i != 0 {
				if err := out.WriteByte(ctx, ','); err != nil {
					return err
				}
			}
			if err := writeJSONString(ctx, out, typ.Fields[i].Name); err != nil {
				return err
			}
			if err := out.WriteByte(ctx, ':'); err != nil {
				return err
			}
			if err := writeJSONValue(ctx, out, children[i], typ.Fields[i].Type, row); err != nil {
				return err
			}
		}
		return out.WriteByte(ctx, '}')
	default:
		return fmt.Errorf("export: unsupported logical type %d", typ.ID)
	}
}

// writeJSONString 流式转义 JSON 字符串，仅对 JSON 必须字符做转义并原样保留 UTF-8。
func writeJSONString(ctx context.Context, out valueOutput, s string) error {
	if err := out.WriteByte(ctx, '"'); err != nil {
		return err
	}
	start := 0
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b != '"' && b != '\\' && b >= 0x20 {
			continue
		}
		if start < i {
			if err := out.WriteString(ctx, s[start:i]); err != nil {
				return err
			}
		}
		switch b {
		case '"', '\\':
			var esc [2]byte
			esc[0] = '\\'
			esc[1] = b
			if err := out.Write(ctx, esc[:]); err != nil {
				return err
			}
		case '\b':
			if err := out.WriteString(ctx, `\b`); err != nil {
				return err
			}
		case '\f':
			if err := out.WriteString(ctx, `\f`); err != nil {
				return err
			}
		case '\n':
			if err := out.WriteString(ctx, `\n`); err != nil {
				return err
			}
		case '\r':
			if err := out.WriteString(ctx, `\r`); err != nil {
				return err
			}
		case '\t':
			if err := out.WriteString(ctx, `\t`); err != nil {
				return err
			}
		default:
			const hex = "0123456789abcdef"
			var esc [6]byte
			esc[0], esc[1], esc[2], esc[3] = '\\', 'u', '0', '0'
			esc[4], esc[5] = hex[b>>4], hex[b&0xf]
			if err := out.Write(ctx, esc[:]); err != nil {
				return err
			}
		}
		start = i + 1
	}
	if start < len(s) {
		if err := out.WriteString(ctx, s[start:]); err != nil {
			return err
		}
	}
	return out.WriteByte(ctx, '"')
}

// appendInt128 将 two's-complement Int128 追加为十进制文本，不使用 big.Int。
func appendInt128(dst []byte, v types.Int128) []byte {
	negative := v.Hi < 0
	hi, lo := uint64(v.Hi), v.Lo
	if negative {
		lo = ^lo + 1
		hi = ^hi
		if lo == 0 {
			hi++
		}
	}
	var digits [40]byte
	pos := len(digits)
	if hi == 0 && lo == 0 {
		pos--
		digits[pos] = '0'
	} else {
		for hi != 0 || lo != 0 {
			qhi, rem := bits.Div64(0, hi, 10)
			qlo, rem := bits.Div64(rem, lo, 10)
			pos--
			digits[pos] = byte('0' + rem)
			hi, lo = qhi, qlo
		}
	}
	if negative {
		dst = append(dst, '-')
	}
	return append(dst, digits[pos:]...)
}

// appendScaledInt64 按 DECIMAL scale 插入小数点并保留固定 scale 位数。
func appendScaledInt64(dst []byte, v int64, scale int) []byte {
	var tmp [24]byte
	digits := strconv.AppendInt(tmp[:0], v, 10)
	return appendScaledDigits(dst, digits, scale)
}

// appendScaledInt128 按 DECIMAL scale 格式化 128-bit unscaled value。
func appendScaledInt128(dst []byte, v types.Int128, scale int) []byte {
	var tmp [48]byte
	digits := appendInt128(tmp[:0], v)
	return appendScaledDigits(dst, digits, scale)
}

func appendScaledDigits(dst, signed []byte, scale int) []byte {
	if scale <= 0 {
		return append(dst, signed...)
	}
	negative := len(signed) > 0 && signed[0] == '-'
	digits := signed
	if negative {
		dst = append(dst, '-')
		digits = signed[1:]
	}
	if len(digits) <= scale {
		dst = append(dst, '0', '.')
		for i := len(digits); i < scale; i++ {
			dst = append(dst, '0')
		}
		return append(dst, digits...)
	}
	cut := len(digits) - scale
	dst = append(dst, digits[:cut]...)
	dst = append(dst, '.')
	return append(dst, digits[cut:]...)
}

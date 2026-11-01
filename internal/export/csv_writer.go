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

// CSVWriteOptions 控制 CSV 文件级编码规则；默认值与常见 DuckDB CSV COPY 行为保持一致方向。
type CSVWriteOptions struct {
	// Delimiter 是字段分隔符，默认逗号。
	Delimiter byte
	// Quote 是引用字符，默认双引号。
	Quote byte
	// Newline 是记录结束符，默认 LF。
	Newline string
	// Header 决定是否写列名记录。
	Header bool
	// NullString 是 NULL 的文本表示；默认空字段。
	NullString string
	// BufferSize 是格式层固定 Managed Buffer 大小。
	BufferSize int
}

// CSVWriter 将 DataChunk 流式编码为 CSV，不直接依赖任何远端协议。
type CSVWriter struct {
	// schema 是绑定后固定的输出 schema。
	schema []Column
	// opts 是规范化后的 CSV 选项。
	opts CSVWriteOptions
	// out 是受 MemoryManager 管理的固定缓冲输出。
	out *managedOutput
	// started 表示 header（如有）已处理。
	started bool
}

// NewCSVWriter 创建 CSV writer，并在返回前完成 ExportBuffer 分配。
func NewCSVWriter(schema []Column, opts CSVWriteOptions, scope *memory.Scope, bm *memory.BufferManager, sink ByteSink) (*CSVWriter, error) {
	if len(schema) == 0 {
		return nil, fmt.Errorf("export: CSV schema is empty")
	}
	if opts.Delimiter == 0 {
		opts.Delimiter = ','
	}
	if opts.Quote == 0 {
		opts.Quote = '"'
	}
	if opts.Newline == "" {
		opts.Newline = "\n"
	}
	if opts.BufferSize <= 0 {
		opts.BufferSize = 64 << 10
	}
	if opts.Delimiter == opts.Quote || strings.ContainsRune(opts.Newline, rune(opts.Delimiter)) || strings.ContainsRune(opts.Newline, rune(opts.Quote)) {
		return nil, fmt.Errorf("export: invalid CSV delimiter/quote/newline combination")
	}
	out, err := newManagedOutput(scope, bm, sink, opts.BufferSize)
	if err != nil {
		return nil, err
	}
	copySchema := append([]Column(nil), schema...)
	return &CSVWriter{schema: copySchema, opts: opts, out: out}, nil
}

// WriteChunk 写出一个完整 DataChunk；任一行失败都会返回错误，由上层将当前文件视为 incomplete 并 Abort。
func (w *CSVWriter) WriteChunk(ctx context.Context, chunk *vector.DataChunk) error {
	if w == nil || w.out == nil || w.out.terminal {
		return ErrWriterClosed
	}
	if err := validateChunkSchema(w.schema, chunk); err != nil {
		return err
	}
	if err := w.ensureStarted(ctx); err != nil {
		return err
	}
	for row := 0; row < chunk.Cardinality; row++ {
		for col := range w.schema {
			if col != 0 {
				if err := w.out.WriteByte(ctx, w.opts.Delimiter); err != nil {
					return err
				}
			}
			if err := w.writeField(ctx, chunk.Columns[col], w.schema[col].Type, row); err != nil {
				return fmt.Errorf("export: CSV row %d column %d: %w", row+1, col+1, err)
			}
		}
		if err := w.out.WriteString(ctx, w.opts.Newline); err != nil {
			return err
		}
	}
	return nil
}

// Flush 刷出当前格式缓冲，不关闭目标文件。
func (w *CSVWriter) Flush(ctx context.Context) error {
	if w == nil || w.out == nil {
		return ErrWriterClosed
	}
	if err := w.ensureStarted(ctx); err != nil {
		return err
	}
	return w.out.Flush(ctx)
}

// Close 确保空结果也能按 Header 选项产生合法 CSV，然后关闭下一层。
func (w *CSVWriter) Close(ctx context.Context) (WriteResult, error) {
	if w == nil || w.out == nil {
		return WriteResult{}, ErrWriterClosed
	}
	if err := w.ensureStarted(ctx); err != nil {
		_ = w.out.Abort(context.Background())
		return WriteResult{}, err
	}
	return w.out.Close(ctx)
}

// Abort 丢弃当前 incomplete CSV file，并释放 Managed Buffer。
func (w *CSVWriter) Abort(ctx context.Context) error {
	if w == nil || w.out == nil {
		return nil
	}
	return w.out.Abort(ctx)
}

// ensureStarted 只写一次 header，避免跨多个 chunk 重复列名。
func (w *CSVWriter) ensureStarted(ctx context.Context) error {
	if w.started {
		return nil
	}
	w.started = true
	if !w.opts.Header {
		return nil
	}
	for i := range w.schema {
		if i != 0 {
			if err := w.out.WriteByte(ctx, w.opts.Delimiter); err != nil {
				return err
			}
		}
		if err := w.writeStringField(ctx, w.schema[i].Name, false); err != nil {
			return err
		}
	}
	return w.out.WriteString(ctx, w.opts.Newline)
}

// writeField 根据 LogicalType 写一个 CSV 字段；LIST/STRUCT 使用 DuckDB 风格文本并整体 CSV quote。
func (w *CSVWriter) writeField(ctx context.Context, vec *vector.Vector, typ types.LogicalType, row int) error {
	if !vec.IsValid(row) {
		return w.out.WriteString(ctx, w.opts.NullString)
	}
	if typ.ID == types.Varchar {
		s, _ := vec.String(row)
		return w.writeStringField(ctx, s, w.opts.NullString == "" && s == "")
	}
	if typ.ID == types.List || typ.ID == types.Struct {
		if err := w.out.WriteByte(ctx, w.opts.Quote); err != nil {
			return err
		}
		quoted := csvQuotedOutput{out: w.out, quote: w.opts.Quote}
		if err := writeDuckDBTextValue(ctx, &quoted, vec, typ, row); err != nil {
			return err
		}
		return w.out.WriteByte(ctx, w.opts.Quote)
	}
	return writeCSVScalar(ctx, w.out, vec, typ, row)
}

// writeStringField 按 RFC4180 规则仅在必要时 quote；forceQuote 用于区分空字符串与默认空 NULL。
func (w *CSVWriter) writeStringField(ctx context.Context, value string, forceQuote bool) error {
	needsQuote := forceQuote || strings.IndexByte(value, w.opts.Delimiter) >= 0 || strings.IndexByte(value, w.opts.Quote) >= 0 || strings.ContainsAny(value, "\r\n") || value == w.opts.NullString
	if !needsQuote {
		return w.out.WriteString(ctx, value)
	}
	if err := w.out.WriteByte(ctx, w.opts.Quote); err != nil {
		return err
	}
	for start := 0; ; {
		i := strings.IndexByte(value[start:], w.opts.Quote)
		if i < 0 {
			if err := w.out.WriteString(ctx, value[start:]); err != nil {
				return err
			}
			break
		}
		i += start
		if err := w.out.WriteString(ctx, value[start:i]); err != nil {
			return err
		}
		if err := w.out.WriteByte(ctx, w.opts.Quote); err != nil {
			return err
		}
		if err := w.out.WriteByte(ctx, w.opts.Quote); err != nil {
			return err
		}
		start = i + 1
	}
	return w.out.WriteByte(ctx, w.opts.Quote)
}

func writeCSVScalar(ctx context.Context, out valueOutput, vec *vector.Vector, typ types.LogicalType, row int) error {
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
		return out.Write(ctx, strconv.AppendFloat(tmp[:0], float64(v), 'g', -1, 32))
	case types.Double:
		v, _ := vec.Float64(row)
		return out.Write(ctx, strconv.AppendFloat(tmp[:0], v, 'g', -1, 64))
	case types.Decimal:
		if typ.Decimal.Width <= 18 {
			v, _ := vec.Int64(row)
			return out.Write(ctx, appendScaledInt64(tmp[:0], v, int(typ.Decimal.Scale)))
		}
		v, _ := vec.Int128(row)
		return out.Write(ctx, appendScaledInt128(tmp[:0], v, int(typ.Decimal.Scale)))
	case types.DateType:
		v, _ := vec.Int32(row)
		t := time.Unix(int64(v)*86400, 0).UTC()
		return out.Write(ctx, t.AppendFormat(tmp[:0], "2006-01-02"))
	case types.TimestampType:
		v, _ := vec.Int64(row)
		t := time.UnixMicro(v).UTC()
		return out.Write(ctx, t.AppendFormat(tmp[:0], "2006-01-02 15:04:05.999999"))
	default:
		return fmt.Errorf("unsupported CSV scalar type %d", typ.ID)
	}
}

// csvQuotedOutput 在已打开的外层 CSV quote 中把 quote 字节加倍。
type csvQuotedOutput struct {
	out   *managedOutput
	quote byte
}

func (q *csvQuotedOutput) Write(ctx context.Context, p []byte) error {
	for _, b := range p {
		if err := q.WriteByte(ctx, b); err != nil {
			return err
		}
	}
	return nil
}
func (q *csvQuotedOutput) WriteString(ctx context.Context, s string) error {
	for i := 0; i < len(s); i++ {
		if err := q.WriteByte(ctx, s[i]); err != nil {
			return err
		}
	}
	return nil
}
func (q *csvQuotedOutput) WriteByte(ctx context.Context, b byte) error {
	if err := q.out.WriteByte(ctx, b); err != nil {
		return err
	}
	if b == q.quote {
		return q.out.WriteByte(ctx, b)
	}
	return nil
}

// writeDuckDBTextValue 为 CSV 的嵌套值生成可读的 DuckDB 风格 LIST/STRUCT 文本。
func writeDuckDBTextValue(ctx context.Context, out valueOutput, vec *vector.Vector, typ types.LogicalType, row int) error {
	if !vec.IsValid(row) {
		return out.WriteString(ctx, "NULL")
	}
	if typ.ID == types.Varchar {
		s, _ := vec.String(row)
		if err := out.WriteByte(ctx, '\''); err != nil {
			return err
		}
		for i := 0; i < len(s); i++ {
			if s[i] == '\'' {
				if err := out.WriteByte(ctx, '\''); err != nil {
					return err
				}
			}
			if err := out.WriteByte(ctx, s[i]); err != nil {
				return err
			}
		}
		return out.WriteByte(ctx, '\'')
	}
	if typ.ID == types.List {
		entry, _ := vec.ListEntry(row)
		children := vec.Children()
		if typ.Child == nil || len(children) != 1 {
			return fmt.Errorf("malformed LIST vector")
		}
		if err := out.WriteByte(ctx, '['); err != nil {
			return err
		}
		for i := uint32(0); i < entry.Length; i++ {
			if i > 0 {
				if err := out.WriteString(ctx, ", "); err != nil {
					return err
				}
			}
			if err := writeDuckDBTextValue(ctx, out, children[0], *typ.Child, int(entry.Offset+i)); err != nil {
				return err
			}
		}
		return out.WriteByte(ctx, ']')
	}
	if typ.ID == types.Struct {
		children := vec.Children()
		if len(children) != len(typ.Fields) {
			return fmt.Errorf("malformed STRUCT vector")
		}
		if err := out.WriteByte(ctx, '{'); err != nil {
			return err
		}
		for i, f := range typ.Fields {
			if i > 0 {
				if err := out.WriteString(ctx, ", "); err != nil {
					return err
				}
			}
			if err := out.WriteByte(ctx, '\''); err != nil {
				return err
			}
			if err := out.WriteString(ctx, f.Name); err != nil {
				return err
			}
			if err := out.WriteString(ctx, "': "); err != nil {
				return err
			}
			if err := writeDuckDBTextValue(ctx, out, children[i], f.Type, row); err != nil {
				return err
			}
		}
		return out.WriteByte(ctx, '}')
	}
	if typ.ID == types.DateType || typ.ID == types.TimestampType {
		if err := out.WriteByte(ctx, '\''); err != nil {
			return err
		}
		if err := writeCSVScalar(ctx, out, vec, typ, row); err != nil {
			return err
		}
		return out.WriteByte(ctx, '\'')
	}
	if typ.ID == types.Float {
		v, _ := vec.Float32(row)
		if math.IsNaN(float64(v)) {
			return out.WriteString(ctx, "nan")
		}
	}
	if typ.ID == types.Double {
		v, _ := vec.Float64(row)
		if math.IsNaN(v) {
			return out.WriteString(ctx, "nan")
		}
	}
	return writeCSVScalar(ctx, out, vec, typ, row)
}

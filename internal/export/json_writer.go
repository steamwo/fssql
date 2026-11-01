package export

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
)

// JSONWriteMode 区分 JSON ARRAY 与逐行 NDJSON；二者共享同一个 typed value encoder。
type JSONWriteMode uint8

const (
	JSONModeArray JSONWriteMode = iota
	JSONModeNDJSON
)

// JSONWriteOptions 控制 JSON/NDJSON 文件级编码。
type JSONWriteOptions struct {
	// Mode 决定输出单个 JSON array 还是每行一个 JSON object。
	Mode JSONWriteMode
	// BufferSize 是格式层固定 Managed Buffer 大小。
	BufferSize int
}

// JSONWriter 直接从 DataChunk typed vectors 生成 JSON，不走 map[string]any 热路径。
type JSONWriter struct {
	// schema 是绑定后固定的列名和 LogicalType。
	schema []Column
	// opts 是规范化后的 JSON 选项。
	opts JSONWriteOptions
	// out 是固定容量 Managed Buffer 输出。
	out *managedOutput
	// started 表示 ARRAY 模式的 '[' 已写出。
	started bool
	// rows 记录已经写出的对象数，用于 ARRAY 逗号分隔。
	rows int64
}

// NewJSONWriter 创建 JSON/NDJSON writer，并先申请格式缓冲。
func NewJSONWriter(schema []Column, opts JSONWriteOptions, scope *memory.Scope, bm *memory.BufferManager, sink ByteSink) (*JSONWriter, error) {
	if len(schema) == 0 {
		return nil, fmt.Errorf("export: JSON schema is empty")
	}
	if opts.Mode != JSONModeArray && opts.Mode != JSONModeNDJSON {
		return nil, fmt.Errorf("export: invalid JSON mode %d", opts.Mode)
	}
	if opts.BufferSize <= 0 {
		opts.BufferSize = 64 << 10
	}
	out, err := newManagedOutput(scope, bm, sink, opts.BufferSize)
	if err != nil {
		return nil, err
	}
	return &JSONWriter{schema: append([]Column(nil), schema...), opts: opts, out: out}, nil
}

// WriteChunk 将每行编码为 object；ARRAY/NDJSON 只在 framing 上不同。
func (w *JSONWriter) WriteChunk(ctx context.Context, chunk *vector.DataChunk) error {
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
		if w.opts.Mode == JSONModeArray && w.rows > 0 {
			if err := w.out.WriteByte(ctx, ','); err != nil {
				return err
			}
		}
		if err := w.writeObject(ctx, chunk, row); err != nil {
			return fmt.Errorf("export: JSON row %d: %w", row+1, err)
		}
		if w.opts.Mode == JSONModeNDJSON {
			if err := w.out.WriteByte(ctx, '\n'); err != nil {
				return err
			}
		}
		w.rows++
	}
	return nil
}

// Flush 刷出格式缓冲；ARRAY 模式不会提前写结束括号。
func (w *JSONWriter) Flush(ctx context.Context) error {
	if w == nil || w.out == nil {
		return ErrWriterClosed
	}
	if err := w.ensureStarted(ctx); err != nil {
		return err
	}
	return w.out.Flush(ctx)
}

// Close 补全 ARRAY framing，并完成当前文件。
func (w *JSONWriter) Close(ctx context.Context) (WriteResult, error) {
	if w == nil || w.out == nil {
		return WriteResult{}, ErrWriterClosed
	}
	if err := w.ensureStarted(ctx); err != nil {
		_ = w.out.Abort(context.Background())
		return WriteResult{}, err
	}
	if w.opts.Mode == JSONModeArray {
		if err := w.out.WriteByte(ctx, ']'); err != nil {
			_ = w.out.Abort(context.Background())
			return WriteResult{}, err
		}
	}
	return w.out.Close(ctx)
}

// Abort 丢弃 incomplete JSON/NDJSON file。
func (w *JSONWriter) Abort(ctx context.Context) error {
	if w == nil || w.out == nil {
		return nil
	}
	return w.out.Abort(ctx)
}

func (w *JSONWriter) ensureStarted(ctx context.Context) error {
	if w.started {
		return nil
	}
	w.started = true
	if w.opts.Mode == JSONModeArray {
		return w.out.WriteByte(ctx, '[')
	}
	return nil
}

// writeObject 按 schema 顺序写 object，保持输出确定性。
func (w *JSONWriter) writeObject(ctx context.Context, chunk *vector.DataChunk, row int) error {
	if err := w.out.WriteByte(ctx, '{'); err != nil {
		return err
	}
	for col := range w.schema {
		if col > 0 {
			if err := w.out.WriteByte(ctx, ','); err != nil {
				return err
			}
		}
		if err := writeJSONString(ctx, w.out, w.schema[col].Name); err != nil {
			return err
		}
		if err := w.out.WriteByte(ctx, ':'); err != nil {
			return err
		}
		if err := writeJSONValue(ctx, w.out, chunk.Columns[col], w.schema[col].Type, row); err != nil {
			return err
		}
	}
	return w.out.WriteByte(ctx, '}')
}

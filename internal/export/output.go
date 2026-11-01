package export

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/steamwo/fssql/internal/core/memory"
)

// ByteSink 是格式层与压缩/VFS 层之间的字节流边界，保持 Protocol × Compression × Format 正交。
type ByteSink interface {
	// Write 写入一段顺序字节；实现必须观察 ctx。
	Write(context.Context, []byte) (int, error)
	// Flush 推进当前层缓冲，但不定义数据集提交语义。
	Flush(context.Context) error
	// Close 完成当前文件并返回文件级结果。
	Close(context.Context) (WriteResult, error)
	// Abort 丢弃 incomplete file 并释放资源；应当可重复调用。
	Abort(context.Context) error
}

// managedOutput 使用固定大小 Managed Buffer 聚合格式层的小写入，避免 per-row heap buffer 无界增长。
type managedOutput struct {
	// sink 是下一层字节接收器，可以是 compression writer 或 VFS sink。
	sink ByteSink
	// buffer 是由 BufferManager 分配的固定容量 ExportBuffer。
	buffer *memory.Buffer
	// used 是当前 buffer 已填充的有效字节数。
	used int
	// terminal 表示输出已 Close/Abort。
	terminal bool
}

// newManagedOutput 在写任何数据前先申请受管缓冲区。
func newManagedOutput(scope *memory.Scope, bm *memory.BufferManager, sink ByteSink, size int) (*managedOutput, error) {
	if scope == nil || bm == nil || sink == nil {
		return nil, errors.New("export: managed output requires scope, buffer manager and sink")
	}
	if size <= 0 {
		return nil, fmt.Errorf("export: invalid output buffer size %d", size)
	}
	buf, err := bm.Allocate(scope, memory.TagExportBuffer, size)
	if err != nil {
		return nil, err
	}
	return &managedOutput{sink: sink, buffer: buf}, nil
}

// Write 将任意长度输入分块复制进固定 Managed Buffer；缓冲满时立即向下一层 flush。
func (o *managedOutput) Write(ctx context.Context, p []byte) error {
	if o == nil || o.terminal || o.buffer == nil {
		return ErrWriterClosed
	}
	for len(p) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		space := o.buffer.Len() - o.used
		if space == 0 {
			if err := o.flushBuffer(ctx); err != nil {
				return err
			}
			space = o.buffer.Len()
		}
		n := len(p)
		if n > space {
			n = space
		}
		copy(o.buffer.Bytes()[o.used:o.used+n], p[:n])
		o.used += n
		p = p[n:]
	}
	return nil
}

// WriteContextByte 写入一个字节并显式携带调用上下文，不冒充 io.ByteWriter。
func (o *managedOutput) WriteContextByte(ctx context.Context, b byte) error {
	var one [1]byte
	one[0] = b
	return o.Write(ctx, one[:])
}

// WriteString 分块写入字符串；不会把整个字符串复制到普通 Go heap。
func (o *managedOutput) WriteString(ctx context.Context, s string) error {
	for len(s) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		space := o.buffer.Len() - o.used
		if space == 0 {
			if err := o.flushBuffer(ctx); err != nil {
				return err
			}
			space = o.buffer.Len()
		}
		n := len(s)
		if n > space {
			n = space
		}
		copy(o.buffer.Bytes()[o.used:o.used+n], s[:n])
		o.used += n
		s = s[n:]
	}
	return nil
}

// Flush 刷出格式缓冲并继续调用下一层 Flush。
func (o *managedOutput) Flush(ctx context.Context) error {
	if o == nil || o.terminal {
		return ErrWriterClosed
	}
	if err := o.flushBuffer(ctx); err != nil {
		return err
	}
	return o.sink.Flush(ctx)
}

// Close 先刷净受管缓冲，再关闭下一层；无论成功失败都释放本层 Managed Buffer。
func (o *managedOutput) Close(ctx context.Context) (WriteResult, error) {
	if o == nil || o.terminal {
		return WriteResult{}, ErrWriterClosed
	}
	o.terminal = true
	if err := o.flushBuffer(ctx); err != nil {
		abortErr := o.sink.Abort(context.Background())
		o.releaseBuffer()
		return WriteResult{}, errors.Join(err, abortErr)
	}
	result, err := o.sink.Close(ctx)
	if err != nil {
		abortErr := o.sink.Abort(context.Background())
		o.releaseBuffer()
		return WriteResult{}, errors.Join(err, abortErr)
	}
	o.releaseBuffer()
	return result, nil
}

// Abort 放弃下一层 incomplete file，并释放格式缓冲区。
func (o *managedOutput) Abort(ctx context.Context) error {
	if o == nil || o.terminal {
		return nil
	}
	o.terminal = true
	err := o.sink.Abort(ctx)
	o.releaseBuffer()
	return err
}

// flushBuffer 将当前有效字节完整写入下一层；short write 被视为失败而非静默截断。
func (o *managedOutput) flushBuffer(ctx context.Context) error {
	if o.used == 0 {
		return nil
	}
	data := o.buffer.Bytes()[:o.used]
	n, err := o.sink.Write(ctx, data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	o.used = 0
	return nil
}

// releaseBuffer 是本层唯一 Managed Buffer 释放点。
func (o *managedOutput) releaseBuffer() {
	if o.buffer != nil {
		o.buffer.Release()
		o.buffer = nil
	}
	o.used = 0
}

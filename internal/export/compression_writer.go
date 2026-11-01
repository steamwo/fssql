package export

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/steamwo/fssql/internal/core/memory"
)

const zstdRawBlockSize = 128 << 10

// CompressionSink 把格式层字节流与协议 sink 解耦，并提供 identity/gzip/zstd 顺序写基线。
type CompressionSink struct {
	// codec 是规范化压缩名称：identity/gzip/zstd。
	codec string
	// downstream 是压缩后的下一层，通常最终适配 VFS WriteHandle。
	downstream ByteSink
	// reservation 记账 gzip 等第三方/标准库内部工作内存。
	reservation *memory.Reservation
	// gzipWriter 仅在 gzip 模式存在。
	gzipWriter *gzip.Writer
	// bridge 把 io.Writer 调用绑定到当前 Write/Flush/Close 的 context。
	bridge *contextSinkWriter
	// zstdBlock 是 zstd raw-block baseline 的 Managed Buffer。
	zstdBlock *memory.Buffer
	// zstdUsed 是当前尚未决定 last-bit 的块长度。
	zstdUsed int
	// zstdHeader 表示 frame header 已经写出。
	zstdHeader bool
	// terminal 表示 Close/Abort 后不可继续写入。
	terminal bool
}

// NewCompressionSink 创建压缩层；zstd v1 基线写出标准 raw blocks，保持 sequential/non-splittable 能力语义。
func NewCompressionSink(codec string, scope *memory.Scope, bm *memory.BufferManager, downstream ByteSink) (*CompressionSink, error) {
	if scope == nil || bm == nil || downstream == nil {
		return nil, errors.New("export: compression sink requires managed memory and downstream")
	}
	codec = strings.ToLower(strings.TrimSpace(codec))
	if codec == "" || codec == "none" {
		codec = "identity"
	}
	s := &CompressionSink{codec: codec, downstream: downstream}
	switch codec {
	case "identity":
		return s, nil
	case "gzip":
		// 标准库 flate 内部持有较大工作区；先 reservation 再构造，确保 engine memory 可归因。
		res, err := scope.Reserve(memory.TagExportBuffer, 1<<20)
		if err != nil {
			return nil, err
		}
		s.reservation = res
		s.bridge = &contextSinkWriter{sink: downstream}
		s.gzipWriter = gzip.NewWriter(s.bridge)
		return s, nil
	case "zstd":
		buf, err := bm.Allocate(scope, memory.TagExportBuffer, zstdRawBlockSize)
		if err != nil {
			return nil, err
		}
		s.zstdBlock = buf
		return s, nil
	default:
		return nil, fmt.Errorf("export: unsupported compression %q", codec)
	}
}

// Write 接收未压缩字节并保持调用方 context 贯穿到下一层。
func (s *CompressionSink) Write(ctx context.Context, p []byte) (int, error) {
	if s == nil || s.terminal {
		return 0, ErrWriterClosed
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	switch s.codec {
	case "identity":
		return s.downstream.Write(ctx, p)
	case "gzip":
		s.bridge.ctx = ctx
		n, err := s.gzipWriter.Write(p)
		s.bridge.ctx = nil
		return n, err
	case "zstd":
		return s.writeZstd(ctx, p)
	default:
		return 0, fmt.Errorf("export: invalid compression state")
	}
}

// Flush 推进压缩器与下游，但不会结束当前压缩 frame。
func (s *CompressionSink) Flush(ctx context.Context) error {
	if s == nil || s.terminal {
		return ErrWriterClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	switch s.codec {
	case "identity":
		return s.downstream.Flush(ctx)
	case "gzip":
		s.bridge.ctx = ctx
		err := s.gzipWriter.Flush()
		s.bridge.ctx = nil
		if err != nil {
			return err
		}
		return s.downstream.Flush(ctx)
	case "zstd":
		if err := s.ensureZstdHeader(ctx); err != nil {
			return err
		}
		if s.zstdUsed > 0 {
			if err := s.emitZstdBlock(ctx, false, s.zstdBlock.Bytes()[:s.zstdUsed]); err != nil {
				return err
			}
			s.zstdUsed = 0
		}
		return s.downstream.Flush(ctx)
	default:
		return fmt.Errorf("export: invalid compression state")
	}
}

// Close 完成压缩 frame 后关闭下游；任一失败都会 Abort incomplete target 并释放本层资源。
func (s *CompressionSink) Close(ctx context.Context) (WriteResult, error) {
	if s == nil || s.terminal {
		return WriteResult{}, ErrWriterClosed
	}
	s.terminal = true
	var err error
	switch s.codec {
	case "identity":
	case "gzip":
		s.bridge.ctx = ctx
		err = s.gzipWriter.Close()
		s.bridge.ctx = nil
	case "zstd":
		if e := s.ensureZstdHeader(ctx); e != nil {
			err = e
		} else {
			err = s.emitZstdBlock(ctx, true, s.zstdBlock.Bytes()[:s.zstdUsed])
			s.zstdUsed = 0
		}
	default:
		err = fmt.Errorf("export: invalid compression state")
	}
	if err != nil {
		abortErr := s.downstream.Abort(context.Background())
		s.release()
		return WriteResult{}, errors.Join(err, abortErr)
	}
	result, closeErr := s.downstream.Close(ctx)
	if closeErr != nil {
		abortErr := s.downstream.Abort(context.Background())
		s.release()
		return WriteResult{}, errors.Join(closeErr, abortErr)
	}
	s.release()
	return result, nil
}

// Abort 放弃压缩流和下游 incomplete file，并保证 reservation/buffer 归零。
func (s *CompressionSink) Abort(ctx context.Context) error {
	if s == nil || s.terminal {
		return nil
	}
	s.terminal = true
	err := s.downstream.Abort(ctx)
	s.release()
	return err
}

func (s *CompressionSink) writeZstd(ctx context.Context, p []byte) (int, error) {
	if err := s.ensureZstdHeader(ctx); err != nil {
		return 0, err
	}
	written := 0
	for len(p) > 0 {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		if s.zstdUsed == s.zstdBlock.Len() {
			if err := s.emitZstdBlock(ctx, false, s.zstdBlock.Bytes()[:s.zstdUsed]); err != nil {
				return written, err
			}
			s.zstdUsed = 0
		}
		n := len(p)
		space := s.zstdBlock.Len() - s.zstdUsed
		if n > space {
			n = space
		}
		copy(s.zstdBlock.Bytes()[s.zstdUsed:s.zstdUsed+n], p[:n])
		s.zstdUsed += n
		written += n
		p = p[n:]
	}
	return written, nil
}

// ensureZstdHeader 写最小标准 frame header：unknown content size、无 checksum、无 dictionary，128KiB window 与 raw block 上限一致。
func (s *CompressionSink) ensureZstdHeader(ctx context.Context) error {
	if s.zstdHeader {
		return nil
	}
	// descriptor=0 表示非 single-segment 且 content size unknown；window descriptor 0x38 表示 128KiB window。
	header := []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x38}
	n, err := s.downstream.Write(ctx, header)
	if err != nil {
		return err
	}
	if n != len(header) {
		return io.ErrShortWrite
	}
	s.zstdHeader = true
	return nil
}

// emitZstdBlock 写三字节 block header 与 raw payload；block size 固定受 128KiB Managed Buffer 限制。
func (s *CompressionSink) emitZstdBlock(ctx context.Context, last bool, p []byte) error {
	if len(p) > zstdRawBlockSize {
		return fmt.Errorf("export: zstd raw block too large")
	}
	h := uint32(len(p)) << 3
	if last {
		h |= 1
	}
	var header [3]byte
	header[0] = byte(h)
	header[1] = byte(h >> 8)
	header[2] = byte(h >> 16)
	n, err := s.downstream.Write(ctx, header[:])
	if err != nil {
		return err
	}
	if n != len(header) {
		return io.ErrShortWrite
	}
	if len(p) == 0 {
		return nil
	}
	n, err = s.downstream.Write(ctx, p)
	if err != nil {
		return err
	}
	if n != len(p) {
		return io.ErrShortWrite
	}
	return nil
}

// release 是压缩层唯一资源释放点。
func (s *CompressionSink) release() {
	if s.reservation != nil {
		s.reservation.Release()
		s.reservation = nil
	}
	if s.zstdBlock != nil {
		s.zstdBlock.Release()
		s.zstdBlock = nil
	}
	s.gzipWriter = nil
	s.bridge = nil
}

// contextSinkWriter 把 gzip 的 io.Writer 接口桥接回 ctx-aware ByteSink；只在单次串行调用期间设置 ctx。
type contextSinkWriter struct {
	sink ByteSink
	ctx  context.Context
}

func (w *contextSinkWriter) Write(p []byte) (int, error) {
	ctx := w.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	return w.sink.Write(ctx, p)
}

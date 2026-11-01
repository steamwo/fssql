package export

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
)

// ErrWriterClosed 表示 pooled writer 已进入 Close/Abort 终态。
var ErrWriterClosed = errors.New("export: pooled writer is closed")

// WriteResult 描述一个格式 writer 完成后的文件级结果，供 Manifest 记录 COMPLETE 状态。
type WriteResult struct {
	// Bytes 是最终文件字节数。
	Bytes int64
	// Checksum 是可选的完整文件校验值。
	Checksum string
}

// FormatWriter 是所有导出格式统一消费自研 DataChunk 的生命周期接口。
type FormatWriter interface {
	// WriteChunk 将一个 DataChunk 编码到目标；实现必须保持内部 Vector ABI，不得转为 Arrow 核心路径。
	WriteChunk(context.Context, *vector.DataChunk) error
	// Flush 刷出格式层已完成的数据，但不代表文件已经最终提交。
	Flush(context.Context) error
	// Close 完成文件并返回文件级结果。
	Close(context.Context) (WriteResult, error)
	// Abort 放弃当前 incomplete file 并释放全部资源；必须可重复调用。
	Abort(context.Context) error
}

// WriterFactory 在 WriterPool 已经取得 slot 和内存 reservation 后创建具体 writer。
type WriterFactory func(context.Context) (FormatWriter, error)

// WriterPool 以固定 slot 和每 writer reservation 限制并发 writer 数量及其基础内存占用。
type WriterPool struct {
	// slots 是有界信号量；容量即最大同时存活 writer 数。
	slots chan struct{}
	// scope 是本 Query 的 Managed MemoryScope。
	scope *memory.Scope
	// perWriterBytes 是创建 writer 之前必须先 Reserve 的基础预算。
	perWriterBytes int64
	// active 仅记录当前 lease 数量，用于 failure/leak assertion。
	active atomic.Int64
}

// NewWriterPool 创建有界 writer 池；maxWriters 和 perWriterBytes 必须为正值。
func NewWriterPool(maxWriters int, perWriterBytes int64, scope *memory.Scope) (*WriterPool, error) {
	if maxWriters <= 0 {
		return nil, fmt.Errorf("export: invalid max writers %d", maxWriters)
	}
	if perWriterBytes <= 0 {
		return nil, fmt.Errorf("export: invalid per-writer reservation %d", perWriterBytes)
	}
	if scope == nil {
		return nil, errors.New("export: writer pool requires memory scope")
	}
	return &WriterPool{slots: make(chan struct{}, maxWriters), scope: scope, perWriterBytes: perWriterBytes}, nil
}

// Active 返回当前仍持有 pool slot 的 writer 数，用于 lifecycle gate。
func (p *WriterPool) Active() int64 {
	if p == nil {
		return 0
	}
	return p.active.Load()
}

// Acquire 先取得 bounded slot，再 Reserve 内存，最后调用 factory；任一步失败都会回滚已取得资源。
func (p *WriterPool) Acquire(ctx context.Context, factory WriterFactory) (*PooledWriter, error) {
	if p == nil || factory == nil {
		return nil, errors.New("export: invalid writer pool acquire")
	}
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	reservation, err := p.scope.Reserve(memory.TagExportBuffer, p.perWriterBytes)
	if err != nil {
		<-p.slots
		return nil, err
	}
	writer, err := factory(ctx)
	if err != nil {
		reservation.Release()
		<-p.slots
		return nil, err
	}
	if writer == nil {
		reservation.Release()
		<-p.slots
		return nil, errors.New("export: writer factory returned nil")
	}
	p.active.Add(1)
	return &PooledWriter{pool: p, writer: writer, reservation: reservation}, nil
}

// PooledWriter 把具体 writer 的终态与 pool slot/reservation 的释放绑定在一起。
type PooledWriter struct {
	// pool 是拥有当前 slot 的 WriterPool。
	pool *WriterPool
	// writer 是真正的格式 writer。
	writer FormatWriter
	// reservation 是该 writer 创建前获得的基础内存预算。
	reservation *memory.Reservation
	// mu 串行化 Write/Flush/Close/Abort，避免终态切换与仍在进行的写入竞态。
	mu sync.Mutex
	// terminal 表示 writer 已经 Close 或 Abort；进入终态后禁止再次写入或关闭。
	terminal bool
	// once 保证 pool ownership 在所有终态路径中只释放一次。
	once sync.Once
}

// WriteChunk 将数据转发给具体格式 writer。
func (w *PooledWriter) WriteChunk(ctx context.Context, chunk *vector.DataChunk) error {
	if w == nil {
		return ErrWriterClosed
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.terminal || w.writer == nil {
		return ErrWriterClosed
	}
	return w.writer.WriteChunk(ctx, chunk)
}

// Flush 转发格式层 flush。
func (w *PooledWriter) Flush(ctx context.Context) error {
	if w == nil {
		return ErrWriterClosed
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.terminal || w.writer == nil {
		return ErrWriterClosed
	}
	return w.writer.Flush(ctx)
}

// Close 完成文件；若具体 writer Close 失败，会额外 Abort，再释放 pool ownership。
func (w *PooledWriter) Close(ctx context.Context) (WriteResult, error) {
	if w == nil {
		return WriteResult{}, ErrWriterClosed
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.terminal || w.writer == nil {
		return WriteResult{}, ErrWriterClosed
	}
	// 先标记终态，确保底层 Close 即使失败也不会再次被写入；失败文件由 Abort 清理。
	w.terminal = true
	result, err := w.writer.Close(ctx)
	if err != nil {
		abortErr := w.writer.Abort(context.Background())
		w.release()
		return WriteResult{}, errors.Join(err, abortErr)
	}
	w.release()
	return result, nil
}

// Abort 放弃 incomplete file，并无条件释放 slot 与 reservation。
func (w *PooledWriter) Abort(ctx context.Context) error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.terminal || w.writer == nil {
		return nil
	}
	w.terminal = true
	err := w.writer.Abort(ctx)
	w.release()
	return err
}

// release 是唯一 ownership 释放点，保证 reservation、active counter 和 bounded slot 一起归零。
func (w *PooledWriter) release() {
	w.once.Do(func() {
		w.reservation.Release()
		w.pool.active.Add(-1)
		<-w.pool.slots
	})
}

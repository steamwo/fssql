package blocking

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
)

const maxBridgeQueue = 64

// Operator 是现有 blocking operator 的最小统一接口。
// Consume 只在 dependency pipeline 阶段调用；Emit 在 dependency Finalize 之后才允许启动。
type Operator interface {
	Consume(context.Context, *vector.DataChunk) error
	Emit(context.Context, func(*vector.DataChunk) error) error
	Release()
}

type emitItem struct {
	chunk *vector.DataChunk // chunk 的 ownership 从 Operator.Emit callback 转移到 root Pipeline。
}

// Bridge 把 Consume/Emit 型 blocking operator 适配为 dependency Sink + root Source。
// 它只保留有界 handoff queue；不会把完整 blocking output 复制到 Go heap 或额外 materialize。
type Bridge struct {
	op Operator // op 是 Distinct/Sort/Aggregate 等真实 blocking operator。

	consumeMu sync.Mutex // consumeMu 串行化非线程安全 operator 的 Consume，并与 Finalize 建立边界。
	stateMu   sync.Mutex // stateMu 保护 finalized/closed/start/cancel/terminalErr。
	finalized bool       // finalized 表示 dependency pipeline 已完成全部 Consume。
	closed    bool       // closed 表示 root Source 生命周期已经结束。
	started   bool       // started 表示 Emit goroutine 已启动。
	cancel    context.CancelFunc
	terminal  error // terminal 保存 Emit 的最终错误；channel 关闭后由所有 Next 观察。

	items chan emitItem // items 是 bounded ownership handoff；容量由构造参数限制。
	done  chan struct{} // done 在 Emit goroutine 完全退出后关闭。

	releaseOnce sync.Once // releaseOnce 保证 operator ownership 只释放一次。
}

var _ pipeline.Sink = (*Bridge)(nil)
var _ pipeline.CloseableSource = (*Bridge)(nil)

// New 创建 blocking bridge。queueCapacity 必须是有限正数，避免 blocking result 被无界缓存在 Go heap。
func New(op Operator, queueCapacity int) (*Bridge, error) {
	if op == nil {
		return nil, fmt.Errorf("blocking bridge requires operator")
	}
	if queueCapacity <= 0 || queueCapacity > maxBridgeQueue {
		return nil, fmt.Errorf("blocking bridge queue capacity must be in [1,%d]", maxBridgeQueue)
	}
	return &Bridge{
		op:    op,
		items: make(chan emitItem, queueCapacity),
		done:  make(chan struct{}),
	}, nil
}

// Consume 实现 dependency pipeline Sink；同一 blocking state 的写入始终串行。
func (b *Bridge) Consume(ctx context.Context, chunk *vector.DataChunk) error {
	if b == nil || b.op == nil {
		return fmt.Errorf("blocking bridge is uninitialized")
	}
	if chunk == nil {
		return fmt.Errorf("blocking bridge cannot consume nil chunk")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b.consumeMu.Lock()
	defer b.consumeMu.Unlock()
	b.stateMu.Lock()
	if b.closed {
		b.stateMu.Unlock()
		return fmt.Errorf("blocking bridge is closed")
	}
	if b.finalized {
		b.stateMu.Unlock()
		return fmt.Errorf("blocking bridge already finalized")
	}
	b.stateMu.Unlock()
	return b.op.Consume(ctx, chunk)
}

// Finalize 关闭 dependency Consume 阶段；它不调用 Emit，避免在 dependency pipeline 内 materialize 全部结果。
func (b *Bridge) Finalize(ctx context.Context) error {
	if b == nil || b.op == nil {
		return fmt.Errorf("blocking bridge is uninitialized")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// 与 Consume 使用同一把锁，确保 Finalize 返回时没有仍在修改 blocking state 的 worker。
	b.consumeMu.Lock()
	defer b.consumeMu.Unlock()
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if b.closed {
		return fmt.Errorf("blocking bridge is closed")
	}
	if b.finalized {
		return fmt.Errorf("blocking bridge already finalized")
	}
	b.finalized = true
	return nil
}

// Next 实现 root Pipeline Source；第一次读取时才启动 Emit，并通过 bounded channel 转移 chunk ownership。
func (b *Bridge) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	if b == nil || b.op == nil {
		return nil, false, fmt.Errorf("blocking bridge is uninitialized")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := b.startEmitter(ctx); err != nil {
		return nil, false, err
	}
	select {
	case item, ok := <-b.items:
		if !ok {
			return nil, false, b.terminalError()
		}
		if item.chunk == nil {
			return nil, false, fmt.Errorf("blocking bridge received nil emitted chunk")
		}
		return item.chunk, true, nil
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
}

// startEmitter 在 finalized gate 后只启动一个 Emit goroutine；所有 Pipeline worker 共享同一输出流。
func (b *Bridge) startEmitter(ctx context.Context) error {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if b.closed {
		return fmt.Errorf("blocking bridge is closed")
	}
	if !b.finalized {
		return fmt.Errorf("blocking bridge source used before dependency finalized")
	}
	if b.started {
		return nil
	}
	emitCtx, cancel := context.WithCancel(ctx)
	b.cancel = cancel
	b.started = true
	go b.runEmitter(emitCtx)
	return nil
}

// runEmitter 调用真实 Operator.Emit。成功 callback 后 chunk ownership 由 items/root Pipeline 接管。
func (b *Bridge) runEmitter(ctx context.Context) {
	err := b.op.Emit(ctx, func(chunk *vector.DataChunk) error {
		if chunk == nil {
			return fmt.Errorf("blocking operator emitted nil chunk")
		}
		select {
		case b.items <- emitItem{chunk: chunk}:
			return nil
		case <-ctx.Done():
			// callback 未成功返回时 ownership 仍在 callback；必须自行释放，避免取消路径泄漏。
			chunk.Release()
			return ctx.Err()
		}
	})
	b.stateMu.Lock()
	b.terminal = err
	b.stateMu.Unlock()
	close(b.items)
	close(b.done)
}

// terminalError 在 Emit stream 结束后返回稳定终态；context cancellation 不被静默改写。
func (b *Bridge) terminalError() error {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	return b.terminal
}

// Close 终止 emitter、释放 channel 中尚未交给 Pipeline 的 chunk，并最终释放 blocking operator。
// Pipeline 保证 Close 在所有 Source.Next worker 退出后调用，因此 drain 不与正常消费者竞争。
func (b *Bridge) Close() error {
	if b == nil {
		return nil
	}
	b.stateMu.Lock()
	if b.closed {
		b.stateMu.Unlock()
		return nil
	}
	b.closed = true
	started := b.started
	cancel := b.cancel
	b.stateMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if started {
		// emitter 可能正阻塞在 bounded send；持续 drain 直到它关闭 channel 才能保证退出。
		for item := range b.items {
			if item.chunk != nil {
				item.chunk.Release()
			}
		}
		<-b.done
	}
	b.releaseOnce.Do(func() { b.op.Release() })
	return nil
}

// EmissionError 仅供 lifecycle/diagnostic gate 读取 Emit 终态；正常执行错误由 Next 直接返回。
func (b *Bridge) EmissionError() error {
	if b == nil {
		return errors.New("blocking bridge is nil")
	}
	return b.terminalError()
}

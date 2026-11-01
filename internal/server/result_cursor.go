package server

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/steamwo/fssql/internal/core/vector"
)

var (
	// ErrResultCursorClosed 表示 consumer 已关闭 ResultCursor，生产者不得继续提交结果。
	ErrResultCursorClosed = errors.New("server: result cursor closed")
	// ErrResultCursorFinished 表示 producer 已声明结果流完成，不能再 Push 新 chunk。
	ErrResultCursorFinished = errors.New("server: result cursor finished")
	// ErrNilResultChunk 表示调用方试图提交或释放 nil DataChunk。
	ErrNilResultChunk = errors.New("server: nil result chunk")
	// ErrResultChunkNotOwned 表示 consumer 尝试释放不属于当前 ResultCursor 的 chunk，或重复释放同一 chunk。
	ErrResultChunkNotOwned = errors.New("server: result chunk is not consumer-owned")
	// ErrResultCursorOwnershipInvariant 表示固定容量 queued/in-flight ownership 表内部状态损坏。
	ErrResultCursorOwnershipInvariant = errors.New("server: result cursor ownership invariant violated")
)

// resultOwnershipState 描述固定 in-flight 槽位的 consumer ownership 状态。
type resultOwnershipState uint8

const (
	// resultOwnershipFree 表示槽位可用于下一次 Next ownership transfer。
	resultOwnershipFree resultOwnershipState = iota
	// resultOwnershipHeld 表示 chunk 已交给 consumer，等待 ReleaseChunk。
	resultOwnershipHeld
	// resultOwnershipReleasing 表示 ReleaseChunk 正在 cursor 锁外释放 Managed Buffer。
	resultOwnershipReleasing
)

// ResultCursorStats 是 ResultCursor 的不可变背压状态快照。
type ResultCursorStats struct {
	// CapacityChunks 是 queued + consumer-owned chunk 的固定总容量。
	CapacityChunks int
	// BufferedChunks 是当前尚未被 consumer 领取的 chunk 数量。
	BufferedChunks int
	// ConsumerOwnedChunks 是已经交给 consumer、但尚未完成 ReleaseChunk 的 chunk 数量。
	ConsumerOwnedChunks int
	// PeakBufferedChunks 是 cursor 生命周期内的最大排队 chunk 数。
	PeakBufferedChunks int
	// Finished 表示 producer 已调用 Finish。
	Finished bool
	// Closed 表示 consumer 已调用 Close。
	Closed bool
}

// ResultCursor 是 AD-037 QueryHandle 下的有界 native DataChunk 结果游标。
//
// Ownership 规则：
//   - Push 成功后 chunk ownership 转移给 ResultCursor；producer 不得再次访问或 Release。
//   - Next 成功后 chunk ownership 转移给 consumer；consumer 必须最终调用 ReleaseChunk。
//   - queued + consumer-held chunk 总数始终不超过 CapacityChunks；consumer 不 Release 会把背压传播回 producer。
//   - ReleaseChunk 先释放 DataChunk Managed Buffer，再确认 consumer ownership 已结束。
//   - Push 失败时 ownership 仍属于 producer。
//   - Close 会 Release 所有仍在队列中的 chunk，但不会代替 consumer 释放已经由 Next 取走的 chunk。
//
// DataChunk payload 已经由 MemoryManager 管理，因此 Cursor 不重复 Reserve payload bytes。
// 大 VARCHAR 等可变长 payload 的绝对上界仍由 Query MemoryScope 约束。
// queued/in-flight chunk 不得跨越其所属 QueryContext 的终态；I-10 要求 Query 终态前 reservation 归零。
type ResultCursor struct {
	// mu 保护 ring、producer/consumer terminal state、consumer ownership 和 peak 指标。
	mu sync.Mutex
	// changed 在 Push/Next/ReleaseChunk/Finish/Close 后唤醒等待方。
	changed *sync.Cond
	// ring 是固定大小的 queued chunk 指针数组，不随 slow client 持续增长。
	ring []*vector.DataChunk
	// head 是下一个 consumer chunk 的槽位。
	head int
	// tail 是下一个 producer 写入的槽位。
	tail int
	// count 是当前已占用 ring 槽位数。
	count int
	// inflight 是与 ring 等容量的 consumer-held chunk 表；禁止用 Go map 进入结果 hot path。
	inflight []*vector.DataChunk
	// inflightState 与 inflight 一一对应，区分 held/releasing，防止 double-release。
	inflightState []resultOwnershipState
	// inflightCount 是 consumer 已取得但尚未完成 ReleaseChunk 的 chunk 数量。
	inflightCount int
	// peak 是 cursor 生命周期内最大排队数。
	peak int
	// finished 表示 producer 已结束；terminalErr 在队列排空后交给 consumer。
	finished bool
	// terminalErr 是 producer 结束原因；nil 表示成功并最终返回 io.EOF。
	terminalErr error
	// closed 表示 consumer 放弃后续结果；Close 会释放所有 queued chunk。
	closed bool
}

// NewResultCursor 创建固定 chunk 容量的 ResultCursor；该容量同时约束 queued 与 consumer-held ownership。
func NewResultCursor(capacityChunks int) (*ResultCursor, error) {
	if capacityChunks <= 0 {
		return nil, errors.New("server: result cursor capacity must be positive")
	}
	cursor := &ResultCursor{
		ring:          make([]*vector.DataChunk, capacityChunks),
		inflight:      make([]*vector.DataChunk, capacityChunks),
		inflightState: make([]resultOwnershipState, capacityChunks),
	}
	cursor.changed = sync.NewCond(&cursor.mu)
	return cursor, nil
}

// Push 把 chunk ownership 放入有界 ring；queued + consumer-held 达到总容量时同步阻塞，从源头向执行 pipeline 施加背压。
func (c *ResultCursor) Push(ctx context.Context, chunk *vector.DataChunk) error {
	if chunk == nil {
		return ErrNilResultChunk
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	stop := c.broadcastOnCancel(ctx)
	defer stop()
	for c.count+c.inflightCount == len(c.ring) && !c.finished && !c.closed {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.changed.Wait()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.closed {
		return ErrResultCursorClosed
	}
	if c.finished {
		return ErrResultCursorFinished
	}
	c.ring[c.tail] = chunk
	c.tail = (c.tail + 1) % len(c.ring)
	c.count++
	if c.count > c.peak {
		c.peak = c.count
	}
	c.changed.Broadcast()
	return nil
}

// Next 返回下一个 native DataChunk 并把 ownership 转移到固定容量 in-flight 表。
// producer 成功 Finish 且队列排空后返回 io.EOF；失败 Finish 则返回对应 terminal error。
// 每个成功返回的 chunk 都必须通过 ReleaseChunk 结束 consumer ownership。
func (c *ResultCursor) Next(ctx context.Context) (*vector.DataChunk, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	stop := c.broadcastOnCancel(ctx)
	defer stop()
	for c.count == 0 && !c.finished && !c.closed {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c.changed.Wait()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.count > 0 {
		ownershipSlot := -1
		for i, state := range c.inflightState {
			if state == resultOwnershipFree {
				ownershipSlot = i
				break
			}
		}
		if ownershipSlot < 0 {
			return nil, ErrResultCursorOwnershipInvariant
		}
		chunk := c.ring[c.head]
		c.ring[c.head] = nil
		c.head = (c.head + 1) % len(c.ring)
		c.count--
		c.inflight[ownershipSlot] = chunk
		c.inflightState[ownershipSlot] = resultOwnershipHeld
		c.inflightCount++
		c.changed.Broadcast()
		return chunk, nil
	}
	if c.closed {
		return nil, ErrResultCursorClosed
	}
	if c.terminalErr != nil {
		return nil, c.terminalErr
	}
	return nil, io.EOF
}

// ReleaseChunk 释放一个由 Next 交付给 consumer 的 DataChunk，并在 Managed Buffer 真正释放后结束 consumer ownership。
// 固定 ownership 表按指针校验 chunk，避免 double-release；释放顺序不能反转，否则 Query terminal 可能越过 I-10。
func (c *ResultCursor) ReleaseChunk(chunk *vector.DataChunk) error {
	if chunk == nil {
		return ErrNilResultChunk
	}

	c.mu.Lock()
	ownershipSlot := -1
	for i, owned := range c.inflight {
		if owned == chunk && c.inflightState[i] == resultOwnershipHeld {
			ownershipSlot = i
			break
		}
	}
	if ownershipSlot < 0 {
		c.mu.Unlock()
		return ErrResultChunkNotOwned
	}
	c.inflightState[ownershipSlot] = resultOwnershipReleasing
	c.mu.Unlock()

	chunk.Release()

	c.mu.Lock()
	c.inflight[ownershipSlot] = nil
	c.inflightState[ownershipSlot] = resultOwnershipFree
	c.inflightCount--
	c.changed.Broadcast()
	c.mu.Unlock()
	return nil
}

// Finish 结束 producer 侧结果流；已排队 chunk 仍可被 consumer 依次读取。
// 第一次 Finish 生效，重复 Finish 返回 ErrResultCursorFinished，避免不同终态原因互相覆盖。
func (c *ResultCursor) Finish(err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrResultCursorClosed
	}
	if c.finished {
		return ErrResultCursorFinished
	}
	c.finished = true
	c.terminalErr = err
	c.changed.Broadcast()
	return nil
}

// Close 表示 consumer 放弃剩余结果；所有 queued chunk 都会立即 Release，producer/consumer 等待者被唤醒。
// 已经由 Next 取走的 chunk 仍属于 consumer，必须通过 ReleaseChunk 结束 ownership。
func (c *ResultCursor) Close() {
	var release []*vector.DataChunk
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	for c.count > 0 {
		chunk := c.ring[c.head]
		c.ring[c.head] = nil
		c.head = (c.head + 1) % len(c.ring)
		c.count--
		if chunk != nil {
			release = append(release, chunk)
		}
	}
	c.changed.Broadcast()
	c.mu.Unlock()

	// Release 可能触及 MemoryManager 锁；在 cursor mutex 外执行，避免扩大临界区。
	for _, chunk := range release {
		chunk.Release()
	}
}

// Stats 返回不暴露内部 ring/in-flight 数组的 cursor 状态快照。
func (c *ResultCursor) Stats() ResultCursorStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return ResultCursorStats{
		CapacityChunks:      len(c.ring),
		BufferedChunks:      c.count,
		ConsumerOwnedChunks: c.inflightCount,
		PeakBufferedChunks:  c.peak,
		Finished:            c.finished,
		Closed:              c.closed,
	}
}

// broadcastOnCancel 让 context cancellation 唤醒 sync.Cond 等待者；不创建常驻 goroutine。
func (c *ResultCursor) broadcastOnCancel(ctx context.Context) func() {
	stopAfterFunc := context.AfterFunc(ctx, func() {
		c.mu.Lock()
		c.changed.Broadcast()
		c.mu.Unlock()
	})
	return func() { stopAfterFunc() }
}

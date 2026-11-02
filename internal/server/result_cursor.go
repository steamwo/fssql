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
	resultOwnershipFree resultOwnershipState = iota
	resultOwnershipHeld
	resultOwnershipReleasing
)

// ResultCursorStats 是 ResultCursor 的不可变背压状态快照。
type ResultCursorStats struct {
	CapacityChunks      int
	BufferedChunks      int
	ConsumerOwnedChunks int
	PeakBufferedChunks  int
	Finished            bool
	Closed              bool
}

// ResultCursor 是 AD-037 QueryHandle 下的有界 native DataChunk 结果游标。
// Push 成功后 ownership 转移给 cursor；Next 成功后转移给 consumer；consumer 必须 ReleaseChunk。
// queued + consumer-held 总数始终受固定容量约束；schema 与 payload 一样在 query result lifetime 内冻结。
type ResultCursor struct {
	mu      sync.Mutex
	changed *sync.Cond

	ring  []*vector.DataChunk
	head  int
	tail  int
	count int

	inflight      []*vector.DataChunk
	inflightState []resultOwnershipState
	inflightCount int
	peak          int

	finished    bool
	terminalErr error
	closed      bool

	columns      []ResultColumn // columns 是 planning 后冻结的结果 schema，不持有 Vector payload。
	schemaFrozen bool           // schemaFrozen 表示 Stable adapter 已完成 schema publication。
	schemaStarted bool          // schemaStarted 表示至少一次 Push 已成功，之后严禁修改 schema。
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

// Push 把 chunk ownership 放入有界 ring；queued + consumer-held 达到总容量时同步阻塞。
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
	c.schemaStarted = true
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

// ReleaseChunk 释放一个由 Next 交付给 consumer 的 DataChunk，并在 Managed Buffer 真正释放后结束 ownership。
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

// Close 表示 consumer 放弃剩余结果；queued chunk 立即 Release，已由 Next 取走的 chunk 仍须 ReleaseChunk。
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

func noopResultCursorStop() {}

// broadcastOnCancel 让 context cancellation 唤醒 sync.Cond 等待者；不创建常驻 goroutine。
func (c *ResultCursor) broadcastOnCancel(ctx context.Context) func() {
	if ctx.Done() == nil {
		return noopResultCursorStop
	}
	stopAfterFunc := context.AfterFunc(ctx, func() {
		c.mu.Lock()
		c.changed.Broadcast()
		c.mu.Unlock()
	})
	return func() { stopAfterFunc() }
}

package server

import (
	"context"
)

// WaitDrained 等待 producer Finish 且所有 queued chunk ownership 已转移给 consumer。
// consumer Close 会立即返回 ErrResultCursorClosed；调用方可据此取消对应 Query。
// 注意：该方法只表示 ring 已排空，不表示 consumer 已释放 Next 取走的 Managed DataChunk。
func (c *ResultCursor) WaitDrained(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	stop := c.broadcastOnCancel(ctx)
	defer stop()
	for !c.closed && !(c.finished && c.count == 0) {
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
	return nil
}

// WaitConsumerReleased 等待所有由 Next 交付给 consumer 的固定容量 in-flight chunk 完成 ReleaseChunk。
// QueryService 在发布任何 terminal state 前必须调用它，保证 Managed Buffer reservation 已真正归零。
func (c *ResultCursor) WaitConsumerReleased(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inflightCount == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	stop := c.broadcastOnCancel(ctx)
	defer stop()
	for c.inflightCount > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.changed.Wait()
	}
	return nil
}

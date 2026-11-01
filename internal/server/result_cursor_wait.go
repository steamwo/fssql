package server

import (
	"context"
)

// WaitDrained 等待 producer Finish 且所有 queued chunk ownership 已转移给 consumer。
// consumer Close 会立即返回 ErrResultCursorClosed；调用方可据此取消对应 Query。
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

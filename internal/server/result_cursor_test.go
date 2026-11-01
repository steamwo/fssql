package server

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

// testResultChunk 创建一个由指定 MemoryScope 管理的单列 int64 DataChunk。
func testResultChunk(t *testing.T, scope *memory.Scope, bm *memory.BufferManager, value int64) *vector.DataChunk {
	t.Helper()
	column, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.BigInt}, 1)
	if err != nil {
		t.Fatal(err)
	}
	column.SetInt64(0, value)
	chunk, err := vector.NewDataChunk(column)
	if err != nil {
		column.Release()
		t.Fatal(err)
	}
	return chunk
}

// chunkInt64 读取测试 chunk 的首个 int64 值。
func chunkInt64(t *testing.T, chunk *vector.DataChunk) int64 {
	t.Helper()
	if chunk == nil || len(chunk.Columns) != 1 {
		t.Fatalf("invalid chunk: %+v", chunk)
	}
	value, ok := chunk.Columns[0].Int64(0)
	if !ok {
		t.Fatal("chunk value is NULL")
	}
	return value
}

func TestResultCursorBackpressuresSlowConsumer(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	cursor, err := NewResultCursor(1)
	if err != nil {
		t.Fatal(err)
	}
	first := testResultChunk(t, scope, bm, 1)
	second := testResultChunk(t, scope, bm, 2)
	if err := cursor.Push(context.Background(), first); err != nil {
		t.Fatal(err)
	}

	pushed := make(chan error, 1)
	go func() { pushed <- cursor.Push(context.Background(), second) }()
	select {
	case err := <-pushed:
		t.Fatalf("second Push returned before consumer drained queue: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	chunk, err := cursor.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := chunkInt64(t, chunk); got != 1 {
		t.Fatalf("first value=%d, want 1", got)
	}
	if err := cursor.ReleaseChunk(chunk); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-pushed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked producer did not resume after consumer Next")
	}
	chunk, err = cursor.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := chunkInt64(t, chunk); got != 2 {
		t.Fatalf("second value=%d, want 2", got)
	}
	if err := cursor.ReleaseChunk(chunk); err != nil {
		t.Fatal(err)
	}
	cursor.Close()
	if scope.Used() != 0 {
		t.Fatalf("managed bytes leaked: %d", scope.Used())
	}
}

func TestResultCursorFinishDrainsQueuedChunksBeforeEOF(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	cursor, err := NewResultCursor(2)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []int64{10, 20} {
		if err := cursor.Push(context.Background(), testResultChunk(t, scope, bm, value)); err != nil {
			t.Fatal(err)
		}
	}
	if err := cursor.Finish(nil); err != nil {
		t.Fatal(err)
	}
	for _, want := range []int64{10, 20} {
		chunk, err := cursor.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if got := chunkInt64(t, chunk); got != want {
			t.Fatalf("value=%d, want %d", got, want)
		}
		if err := cursor.ReleaseChunk(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := cursor.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("terminal err=%v, want io.EOF", err)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed bytes leaked: %d", scope.Used())
	}
}

func TestResultCursorFinishErrorIsReturnedAfterDrain(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	cursor, err := NewResultCursor(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := cursor.Push(context.Background(), testResultChunk(t, scope, bm, 7)); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("query failed")
	if err := cursor.Finish(failure); err != nil {
		t.Fatal(err)
	}
	chunk, err := cursor.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := cursor.ReleaseChunk(chunk); err != nil {
		t.Fatal(err)
	}
	if _, err := cursor.Next(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("terminal err=%v, want query failure", err)
	}
}

func TestResultCursorWaitConsumerReleasedBlocksUntilManagedChunkReleased(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	cursor, err := NewResultCursor(1)
	if err != nil {
		t.Fatal(err)
	}
	chunk := testResultChunk(t, scope, memory.NewBufferManager(nil), 1)
	if err := cursor.Push(context.Background(), chunk); err != nil {
		t.Fatal(err)
	}
	if err := cursor.Finish(nil); err != nil {
		t.Fatal(err)
	}
	owned, err := cursor.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := cursor.WaitDrained(context.Background()); err != nil {
		t.Fatal(err)
	}

	waited := make(chan error, 1)
	go func() { waited <- cursor.WaitConsumerReleased(context.Background()) }()
	select {
	case err := <-waited:
		t.Fatalf("consumer release barrier returned early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	stats := cursor.Stats()
	if stats.ConsumerOwnedChunks != 1 || scope.Used() == 0 {
		t.Fatalf("consumer ownership not visible: stats=%+v used=%d", stats, scope.Used())
	}
	if err := cursor.ReleaseChunk(owned); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("consumer release barrier did not wake")
	}
	if scope.Used() != 0 || cursor.Stats().ConsumerOwnedChunks != 0 {
		t.Fatalf("consumer ownership leaked: stats=%+v used=%d", cursor.Stats(), scope.Used())
	}
}

func TestResultCursorReleaseChunkRejectsUnownedChunk(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	chunk := testResultChunk(t, scope, memory.NewBufferManager(nil), 1)
	cursor, err := NewResultCursor(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := cursor.ReleaseChunk(chunk); !errors.Is(err, ErrResultChunkNotOwned) {
		t.Fatalf("ReleaseChunk err=%v, want ErrResultChunkNotOwned", err)
	}
	// 未取得 consumer ownership 的 chunk 仍由测试作为 producer owner 释放。
	chunk.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed bytes leaked: %d", scope.Used())
	}
}

func TestResultCursorPushCancellationKeepsProducerOwnership(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	cursor, err := NewResultCursor(1)
	if err != nil {
		t.Fatal(err)
	}
	first := testResultChunk(t, scope, bm, 1)
	second := testResultChunk(t, scope, bm, 2)
	if err := cursor.Push(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := cursor.Push(ctx, second); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Push err=%v, want DeadlineExceeded", err)
	}
	// Push 失败时 ownership 仍属于 producer，因此测试显式 Release second。
	second.Release()
	cursor.Close()
	if scope.Used() != 0 {
		t.Fatalf("managed bytes leaked: %d", scope.Used())
	}
}

func TestResultCursorNextCancellationDoesNotCloseCursor(t *testing.T) {
	cursor, err := NewResultCursor(1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := cursor.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Next err=%v, want DeadlineExceeded", err)
	}
	stats := cursor.Stats()
	if stats.Closed || stats.Finished || stats.BufferedChunks != 0 || stats.ConsumerOwnedChunks != 0 {
		t.Fatalf("stats after canceled Next=%+v", stats)
	}
	cursor.Close()
}

func TestResultCursorCloseReleasesQueuedManagedChunksAndUnblocksProducer(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	cursor, err := NewResultCursor(1)
	if err != nil {
		t.Fatal(err)
	}
	first := testResultChunk(t, scope, bm, 1)
	second := testResultChunk(t, scope, bm, 2)
	if err := cursor.Push(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- cursor.Push(context.Background(), second) }()
	select {
	case err := <-result:
		t.Fatalf("producer should be blocked, got %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	cursor.Close()
	select {
	case err := <-result:
		if !errors.Is(err, ErrResultCursorClosed) {
			t.Fatalf("Push err=%v, want ErrResultCursorClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock producer")
	}
	// second 的 Push 失败，ownership 仍属于 producer。
	second.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed bytes leaked after Close: %d", scope.Used())
	}
	if _, err := cursor.Next(context.Background()); !errors.Is(err, ErrResultCursorClosed) {
		t.Fatalf("Next after Close err=%v", err)
	}
}

func TestResultCursorRejectsPushAfterFinishWithoutTakingOwnership(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	cursor, err := NewResultCursor(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := cursor.Finish(nil); err != nil {
		t.Fatal(err)
	}
	chunk := testResultChunk(t, scope, bm, 1)
	if err := cursor.Push(context.Background(), chunk); !errors.Is(err, ErrResultCursorFinished) {
		t.Fatalf("Push err=%v, want ErrResultCursorFinished", err)
	}
	chunk.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed bytes leaked: %d", scope.Used())
	}
}

func TestResultCursorStatsPeakIsBoundedByCapacity(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	cursor, err := NewResultCursor(2)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []int64{1, 2} {
		if err := cursor.Push(context.Background(), testResultChunk(t, scope, bm, value)); err != nil {
			t.Fatal(err)
		}
	}
	stats := cursor.Stats()
	if stats.CapacityChunks != 2 || stats.BufferedChunks != 2 || stats.ConsumerOwnedChunks != 0 || stats.PeakBufferedChunks != 2 {
		t.Fatalf("stats=%+v", stats)
	}
	cursor.Close()
	if scope.Used() != 0 {
		t.Fatalf("managed bytes leaked: %d", scope.Used())
	}
}

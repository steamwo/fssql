package blocking

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

// fakeOperator 模拟现有 Aggregate/Distinct ownership：callback 成功转移 chunk；callback error 时 operator 自行释放。
type fakeOperator struct {
	scope   *memory.Scope
	buffers *memory.BufferManager
	rows    int
	emitErr error

	active     atomic.Int64
	maxActive  atomic.Int64
	consumed   atomic.Int64
	emitted    atomic.Int64
	released   atomic.Int64
	emitCalled atomic.Int64
}

func (f *fakeOperator) Consume(ctx context.Context, _ *vector.DataChunk) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	active := f.active.Add(1)
	for {
		peak := f.maxActive.Load()
		if active <= peak || f.maxActive.CompareAndSwap(peak, active) {
			break
		}
	}
	for i := 0; i < 32; i++ {
		runtime.Gosched()
	}
	f.consumed.Add(1)
	f.active.Add(-1)
	return nil
}

func (f *fakeOperator) Emit(ctx context.Context, emit func(*vector.DataChunk) error) error {
	f.emitCalled.Add(1)
	for row := 0; row < f.rows; row++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk, err := makeManagedChunk(f.scope, f.buffers, int64(row))
		if err != nil {
			return err
		}
		if err := emit(chunk); err != nil {
			// 与 HashAggregate.Emit 一致：callback error 时 ownership 没有转移，operator 负责释放。
			chunk.Release()
			return err
		}
		f.emitted.Add(1)
	}
	return f.emitErr
}

func (f *fakeOperator) Release() { f.released.Add(1) }

func makeManagedChunk(scope *memory.Scope, buffers *memory.BufferManager, value int64) (*vector.DataChunk, error) {
	column, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.BigInt}, 1)
	if err != nil {
		return nil, err
	}
	column.SetInt64(0, value)
	chunk, err := vector.NewDataChunk(column)
	if err != nil {
		column.Release()
		return nil, err
	}
	return chunk, nil
}

func TestBridgeSerializesConsumeAndRequiresFinalize(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	op := &fakeOperator{scope: scope, buffers: buffers}
	bridge, err := New(op, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := bridge.Next(context.Background()); err == nil {
		t.Fatal("expected source-before-finalize rejection")
	}

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := bridge.Consume(context.Background(), &vector.DataChunk{}); err != nil {
				t.Errorf("consume: %v", err)
			}
		}()
	}
	wg.Wait()
	if op.consumed.Load() != 16 || op.maxActive.Load() != 1 {
		t.Fatalf("consumed=%d maxActive=%d", op.consumed.Load(), op.maxActive.Load())
	}
	if err := bridge.Finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := bridge.Consume(context.Background(), &vector.DataChunk{}); err == nil {
		t.Fatal("expected consume-after-finalize rejection")
	}
	if err := bridge.Close(); err != nil {
		t.Fatal(err)
	}
	if op.released.Load() != 1 || manager.Used() != 0 {
		t.Fatalf("released=%d managed=%d", op.released.Load(), manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBridgeTransfersEmitOwnershipAndPropagatesTerminalError(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	wantErr := errors.New("emit failed")
	op := &fakeOperator{scope: scope, buffers: buffers, rows: 3, emitErr: wantErr}
	bridge, err := New(op, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := bridge.Finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	for want := int64(0); want < 3; want++ {
		chunk, ok, err := bridge.Next(context.Background())
		if err != nil || !ok || chunk == nil {
			t.Fatalf("row=%d ok=%v err=%v", want, ok, err)
		}
		got, valid := chunk.Columns[0].Int64(0)
		if !valid || got != want {
			chunk.Release()
			t.Fatalf("got=%d valid=%v want=%d", got, valid, want)
		}
		chunk.Release()
	}
	if chunk, ok, err := bridge.Next(context.Background()); chunk != nil || ok || !errors.Is(err, wantErr) {
		t.Fatalf("terminal chunk=%v ok=%v err=%v", chunk, ok, err)
	}
	if !errors.Is(bridge.EmissionError(), wantErr) {
		t.Fatalf("emission error=%v", bridge.EmissionError())
	}
	if err := bridge.Close(); err != nil {
		t.Fatal(err)
	}
	if op.emitCalled.Load() != 1 || op.released.Load() != 1 || manager.Used() != 0 {
		t.Fatalf("emit=%d release=%d managed=%d", op.emitCalled.Load(), op.released.Load(), manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBridgeCloseCancelsBlockedEmitterAndDrainsBufferedChunk(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	op := &fakeOperator{scope: scope, buffers: buffers, rows: 64}
	bridge, err := New(op, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := bridge.Finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 第一次 Next 启动 emitter 并消费一个 chunk；随后 emitter 最多只能再缓冲一个 chunk，然后受 bounded queue 背压。
	chunk, ok, err := bridge.Next(context.Background())
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	chunk.Release()
	if err := bridge.Close(); err != nil {
		t.Fatal(err)
	}
	if op.released.Load() != 1 {
		t.Fatalf("release=%d", op.released.Load())
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := bridge.Close(); err != nil || op.released.Load() != 1 {
		t.Fatalf("second close err=%v release=%d", err, op.released.Load())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBridgeRejectsUnboundedQueue(t *testing.T) {
	op := &fakeOperator{}
	for _, capacity := range []int{0, -1, maxBridgeQueue + 1} {
		if _, err := New(op, capacity); err == nil {
			t.Fatalf("capacity=%d should fail", capacity)
		}
	}
}

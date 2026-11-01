package server

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

// BenchmarkResultCursorPushNext 覆盖 native DataChunk 在无背压情况下的 cursor 交接开销。
func BenchmarkResultCursorPushNext(b *testing.B) {
	manager := memory.NewManager(-1)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	column, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.BigInt}, 1)
	if err != nil {
		b.Fatal(err)
	}
	column.SetInt64(0, 1)
	chunk, err := vector.NewDataChunk(column)
	if err != nil {
		b.Fatal(err)
	}
	cursor, err := NewResultCursor(1)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cursor.Push(ctx, chunk); err != nil {
			b.Fatal(err)
		}
		got, err := cursor.Next(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if got != chunk {
			b.Fatal("cursor changed chunk ownership identity")
		}
	}
	b.StopTimer()
	chunk.Release()
	cursor.Close()
	if scope.Used() != 0 {
		b.Fatalf("managed bytes leaked: %d", scope.Used())
	}
}

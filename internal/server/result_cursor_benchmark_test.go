package server

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/vector"
)

// BenchmarkResultCursorPushNext 覆盖 native DataChunk 在无背压情况下的 Push→Next→ReleaseChunk ownership 交接开销。
// 复用空 DataChunk，避免把 Vector/DataChunk allocation 成本混入 ResultCursor hot-path baseline。
func BenchmarkResultCursorPushNext(b *testing.B) {
	cursor, err := NewResultCursor(1)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	chunk := &vector.DataChunk{}
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
		if err := cursor.ReleaseChunk(got); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	cursor.Close()
}

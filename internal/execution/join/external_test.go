package join

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/query"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

func makeGeneratedChunks(t testing.TB, scope *memory.Scope, bm *memory.BufferManager, rows, keyMod int, valueBase int64, allKey *int64) []*vector.DataChunk {
	t.Helper()
	var chunks []*vector.DataChunk
	for start := 0; start < rows; start += vector.DefaultVectorSize {
		n := rows - start
		if n > vector.DefaultVectorSize {
			n = vector.DefaultVectorSize
		}
		keys := make([]nullableInt, n)
		vals := make([]nullableInt, n)
		for i := 0; i < n; i++ {
			row := start + i
			key := int64(row % keyMod)
			if allKey != nil {
				key = *allKey
			}
			keys[i] = ni(key)
			vals[i] = ni(valueBase + int64(row))
		}
		chunks = append(chunks, makeIntChunk(t, scope, bm, keys, vals))
	}
	return chunks
}

func runInnerReference(t *testing.T, rowsLeft, rowsRight, keyMod int) []string {
	t.Helper()
	m := memory.NewManager(32 << 20)
	q := query.New(context.Background(), m)
	bm := memory.NewBufferManager(nil)
	types2 := []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}}
	j, err := New(Config{Type: Inner, LeftTypes: types2, RightTypes: types2, LeftKeys: []int{0}, RightKeys: []int{0}, Scope: q.Memory, Buffers: bm})
	if err != nil {
		t.Fatal(err)
	}
	left := makeGeneratedChunks(t, q.Memory, bm, rowsLeft, keyMod, 100000, nil)
	right := makeGeneratedChunks(t, q.Memory, bm, rowsRight, keyMod, 200000, nil)
	collector := &rowCollector{}
	build := &pipeline.Pipeline{ID: "reference-build", Source: &chunkSource{chunks: right}, Sink: j.BuildSink()}
	probe := &pipeline.Pipeline{ID: "reference-probe", Source: &chunkSource{chunks: left}, Sink: j.ProbeSink(collector), Dependencies: []*pipeline.Pipeline{build}}
	if err := pipeline.RunDAG(q.Context(), probe, 1, q); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	return collector.rows
}

func runExternalInner(t *testing.T, rowsLeft, rowsRight, keyMod int, budget int64, leftEstimate, rightEstimate uint64, allKey *int64) ([]string, ExternalStats, int64) {
	t.Helper()
	m := memory.NewManager(32 << 20)
	quotas := storagetemp.NewTemporaryMemoryManager(storagetemp.Limits{Global: 64 << 20, DefaultResourceGroup: 64 << 20, DefaultQuery: 64 << 20})
	storage, err := storagetemp.NewTemporaryStorageManager(t.TempDir(), quotas)
	if err != nil {
		t.Fatal(err)
	}
	tempScope, err := storage.NewQueryScope("default")
	if err != nil {
		t.Fatal(err)
	}
	q := query.NewWithTemp(context.Background(), m, tempScope)
	bm := memory.NewBufferManager(nil)
	types2 := []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}}
	ext, err := NewExternal(ExternalConfig{
		Type: Inner, LeftTypes: types2, RightTypes: types2, LeftKeys: []int{0}, RightKeys: []int{0},
		LeftEstimatedBytes: leftEstimate, RightEstimatedBytes: rightEstimate, MemoryBudget: budget,
		RadixBits: 4, MaxDepth: 3, MinReductionPercent: 10, SpillBlockBytes: 4096,
		Scope: q.Memory, Buffers: bm, Temp: tempScope, Compression: storagetemp.CompressionRLE,
	})
	if err != nil {
		t.Fatal(err)
	}
	left := makeGeneratedChunks(t, q.Memory, bm, rowsLeft, keyMod, 100000, allKey)
	right := makeGeneratedChunks(t, q.Memory, bm, rowsRight, keyMod, 200000, allKey)
	collector := &rowCollector{}
	var buildChunks, probeChunks []*vector.DataChunk
	if ext.BuildSide() == RightSide {
		buildChunks, probeChunks = right, left
	} else {
		buildChunks, probeChunks = left, right
	}
	build := &pipeline.Pipeline{ID: "external-build", Source: &chunkSource{chunks: buildChunks}, Sink: ext.BuildSink()}
	probe := &pipeline.Pipeline{ID: "external-probe", Source: &chunkSource{chunks: probeChunks}, Sink: ext.ProbeSink(collector), Dependencies: []*pipeline.Pipeline{build}}
	if err := pipeline.RunDAG(q.Context(), probe, 1, q); err != nil {
		t.Fatal(err)
	}
	stats := ext.Stats()
	tempPeak := quotas.GlobalPeak()
	if err := q.Close(); err != nil {
		t.Fatalf("query lifecycle leak: %v", err)
	}
	if quotas.GlobalUsed() != 0 {
		t.Fatalf("temp quota leak: %d", quotas.GlobalUsed())
	}
	return collector.rows, stats, tempPeak
}

func TestExternalForcedSpillMatchesInMemory(t *testing.T) {
	want := runInnerReference(t, 768, 640, 113)
	got, stats, tempPeak := runExternalInner(t, 768, 640, 113, 16<<10, 768*32, 640*32, nil)
	sort.Strings(want)
	sort.Strings(got)
	if len(got) != len(want) {
		t.Fatalf("rows=%d want=%d stats=%+v", len(got), len(want), stats)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d=%q want=%q", i, got[i], want[i])
		}
	}
	if stats.SpilledBuildPartitions == 0 {
		t.Fatalf("forced-spill test did not spill: %+v", stats)
	}
	if stats.ResidentBuildPartitions == 0 {
		t.Fatalf("hybrid test kept no resident partition: %+v", stats)
	}
	if tempPeak == 0 {
		t.Fatal("forced-spill test used no temporary storage")
	}
}

func TestExternalChoosesEstimatedSmallerBuildSideAndPreservesOutputOrder(t *testing.T) {
	got, stats, _ := runExternalInner(t, 12, 30, 7, 16<<10, 100, 1000, nil)
	if stats.BuildSide != LeftSide {
		t.Fatalf("build side=%v want left", stats.BuildSide)
	}
	want := runInnerReference(t, 12, 30, 7)
	sort.Strings(got)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("rows=%d want=%d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("output order/value mismatch at %d: %q vs %q", i, got[i], want[i])
		}
	}
}

func TestExternalSkewFallsBackToBoundedTerminalJoin(t *testing.T) {
	key := int64(7)
	got, stats, _ := runExternalInner(t, 10, 80, 1, 4096, 1000, 800, &key)
	if len(got) != 800 {
		t.Fatalf("rows=%d want=800 stats=%+v", len(got), stats)
	}
	if stats.TerminalSkewPartitions == 0 {
		t.Fatalf("skew did not use terminal strategy: %+v", stats)
	}
}

func TestExternalCancelDuringSpillReleasesManagedState(t *testing.T) {
	m := memory.NewManager(8 << 20)
	quotas := storagetemp.NewTemporaryMemoryManager(storagetemp.Limits{Global: 8 << 20, DefaultResourceGroup: 8 << 20, DefaultQuery: 8 << 20})
	storage, err := storagetemp.NewTemporaryStorageManager(t.TempDir(), quotas)
	if err != nil {
		t.Fatal(err)
	}
	tempScope, err := storage.NewQueryScope("default")
	if err != nil {
		t.Fatal(err)
	}
	q := query.NewWithTemp(context.Background(), m, tempScope)
	bm := memory.NewBufferManager(nil)
	types2 := []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}}
	ext, err := NewExternal(ExternalConfig{Type: Inner, LeftTypes: types2, RightTypes: types2, LeftKeys: []int{0}, RightKeys: []int{0}, LeftEstimatedBytes: 100000, RightEstimatedBytes: 50000, MemoryBudget: 4096, RadixBits: 4, Scope: q.Memory, Buffers: bm, Temp: tempScope})
	if err != nil {
		t.Fatal(err)
	}
	chunks := makeGeneratedChunks(t, q.Memory, bm, 256, 31, 200000, nil)
	sink := ext.BuildSink()
	if err := sink.Consume(context.Background(), chunks[0]); err != nil {
		t.Fatal(err)
	}
	chunks[0].Release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	extra := makeGeneratedChunks(t, q.Memory, bm, 1, 1, 300000, nil)[0]
	err = sink.Consume(ctx, extra)
	extra.Release()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v want canceled", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("query lifecycle leak after cancel: %v", err)
	}
	if m.Used() != 0 {
		t.Fatalf("managed memory leak=%d", m.Used())
	}
	if quotas.GlobalUsed() != 0 {
		t.Fatalf("temp quota leak=%d", quotas.GlobalUsed())
	}
}

func BenchmarkExternalHashJoinForcedSpill(b *testing.B) {
	for i := 0; i < b.N; i++ {
		m := memory.NewManager(32 << 20)
		quotas := storagetemp.NewTemporaryMemoryManager(storagetemp.Limits{Global: 64 << 20, DefaultResourceGroup: 64 << 20, DefaultQuery: 64 << 20})
		storage, err := storagetemp.NewTemporaryStorageManager(b.TempDir(), quotas)
		if err != nil {
			b.Fatal(err)
		}
		tempScope, _ := storage.NewQueryScope("default")
		q := query.NewWithTemp(context.Background(), m, tempScope)
		bm := memory.NewBufferManager(nil)
		types2 := []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}}
		ext, err := NewExternal(ExternalConfig{Type: Inner, LeftTypes: types2, RightTypes: types2, LeftKeys: []int{0}, RightKeys: []int{0}, LeftEstimatedBytes: 65536, RightEstimatedBytes: 65536, MemoryBudget: 16 << 10, RadixBits: 4, Scope: q.Memory, Buffers: bm, Temp: tempScope})
		if err != nil {
			b.Fatal(err)
		}
		left := makeGeneratedChunks(b, q.Memory, bm, 2048, 2048, 100000, nil)
		right := makeGeneratedChunks(b, q.Memory, bm, 2048, 2048, 200000, nil)
		counter := &countSink{}
		build := &pipeline.Pipeline{Source: &chunkSource{chunks: right}, Sink: ext.BuildSink()}
		probe := &pipeline.Pipeline{Source: &chunkSource{chunks: left}, Sink: ext.ProbeSink(counter), Dependencies: []*pipeline.Pipeline{build}}
		if err := pipeline.RunDAG(q.Context(), probe, 1, q); err != nil {
			b.Fatal(err)
		}
		if counter.rows != 2048 {
			b.Fatalf("rows=%d", counter.rows)
		}
		if err := q.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

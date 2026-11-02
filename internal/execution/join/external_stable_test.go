package join

import (
	"context"
	"sort"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/query"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

func runStableExternalJoinChunks(t *testing.T, jt Type, left, right []*vector.DataChunk, q *query.Context, bm *memory.BufferManager, budget int64) ([]string, ExternalStats) {
	t.Helper()
	types2 := []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}}
	j, err := NewStableExternal(ExternalConfig{
		Type: jt, LeftTypes: types2, RightTypes: types2, LeftKeys: []int{0}, RightKeys: []int{0},
		MemoryBudget: budget, RadixBits: 4, MaxDepth: 3, MinReductionPercent: 10, SpillBlockBytes: 1024,
		Scope: q.Memory, Buffers: bm, Temp: q.Temp, Compression: storagetemp.CompressionRLE,
	})
	if err != nil {
		t.Fatal(err)
	}
	collector := &rowCollector{}
	buildChunks, probeChunks := right, left
	if j.BuildSide() == LeftSide {
		buildChunks, probeChunks = left, right
	}
	build := &pipeline.Pipeline{ID: "stable-external-build", Source: &chunkSource{chunks: buildChunks}, Sink: j.BuildSink()}
	probe := &pipeline.Pipeline{ID: "stable-external-probe", Source: &chunkSource{chunks: probeChunks}, Sink: j.ProbeSink(collector), Dependencies: []*pipeline.Pipeline{build}}
	if err := pipeline.RunDAG(q.Context(), probe, 1, q); err != nil {
		j.Close()
		t.Fatal(err)
	}
	stats := j.Stats()
	j.Close()
	return collector.rows, stats
}

func newStableExternalHarness(t *testing.T) (*memory.Manager, *memory.BufferManager, *storagetemp.TemporaryMemoryManager, *query.Context) {
	t.Helper()
	manager := memory.NewManager(64 << 20)
	buffers := memory.NewBufferManager(nil)
	quotas := storagetemp.NewTemporaryMemoryManager(storagetemp.Limits{Global: 64 << 20, DefaultResourceGroup: 64 << 20, DefaultQuery: 64 << 20})
	storage, err := storagetemp.NewTemporaryStorageManager(t.TempDir(), quotas)
	if err != nil {
		t.Fatal(err)
	}
	tempScope, err := storage.NewQueryScope("stable-external-join")
	if err != nil {
		t.Fatal(err)
	}
	return manager, buffers, quotas, query.NewWithTemp(context.Background(), manager, tempScope)
}

// TestStableExternalJoinTypeSuite 验证 external Stable runtime 保持 INNER/outer/semi/anti SQL 结果和 NULL key 语义。
func TestStableExternalJoinTypeSuite(t *testing.T) {
	inner := []string{"2,20,2,200", "2,20,2,201", "2,21,2,200", "2,21,2,201"}
	leftUnmatched := []string{"1,10,N,N", "3,30,N,N", "N,40,N,N"}
	rightUnmatched := []string{"N,N,4,400", "N,N,N,401"}
	cases := []struct {
		name string
		typ  Type
		want []string
	}{
		{"inner", Inner, inner},
		{"left", Left, append(append([]string{}, inner...), leftUnmatched...)},
		{"right", Right, append(append([]string{}, inner...), rightUnmatched...)},
		{"full", Full, append(append(append([]string{}, inner...), leftUnmatched...), rightUnmatched...)},
		{"semi", Semi, []string{"2,20", "2,21"}},
		{"anti", Anti, []string{"1,10", "3,30", "N,40"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			manager, buffers, quotas, q := newStableExternalHarness(t)
			left := makeIntChunk(t, q.Memory, buffers, []nullableInt{ni(1), ni(2), ni(2), ni(3), nullInt()}, []nullableInt{ni(10), ni(20), ni(21), ni(30), ni(40)})
			right := makeIntChunk(t, q.Memory, buffers, []nullableInt{ni(2), ni(2), ni(4), nullInt()}, []nullableInt{ni(200), ni(201), ni(400), ni(401)})
			got, _ := runStableExternalJoinChunks(t, tc.typ, []*vector.DataChunk{left}, []*vector.DataChunk{right}, q, buffers, 32<<10)
			assertRows(t, got, tc.want)
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
			if manager.Used() != 0 || quotas.GlobalUsed() != 0 {
				t.Fatalf("leak managed=%d temp=%d", manager.Used(), quotas.GlobalUsed())
			}
		})
	}
}

func runGeneratedReference(t *testing.T, jt Type, rowsLeft, rowsRight, keyMod int) []string {
	t.Helper()
	manager := memory.NewManager(64 << 20)
	q := query.New(context.Background(), manager)
	buffers := memory.NewBufferManager(nil)
	types2 := []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}}
	j, err := New(Config{Type: jt, LeftTypes: types2, RightTypes: types2, LeftKeys: []int{0}, RightKeys: []int{0}, Scope: q.Memory, Buffers: buffers})
	if err != nil {
		t.Fatal(err)
	}
	left := makeGeneratedChunks(t, q.Memory, buffers, rowsLeft, keyMod, 100000, nil)
	right := makeGeneratedChunks(t, q.Memory, buffers, rowsRight, keyMod, 200000, nil)
	collector := &rowCollector{}
	build := &pipeline.Pipeline{Source: &chunkSource{chunks: right}, Sink: j.BuildSink()}
	probe := &pipeline.Pipeline{Source: &chunkSource{chunks: left}, Sink: j.ProbeSink(collector), Dependencies: []*pipeline.Pipeline{build}}
	if err := pipeline.RunDAG(q.Context(), probe, 1, q); err != nil {
		t.Fatal(err)
	}
	j.Close()
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	return collector.rows
}

// TestStableExternalFullForcedSpillMatchesInMemory 验证 FULL JOIN 在 forced spill 下与已有 in-memory join 等价。
func TestStableExternalFullForcedSpillMatchesInMemory(t *testing.T) {
	want := runGeneratedReference(t, Full, 768, 640, 113)
	manager, buffers, quotas, q := newStableExternalHarness(t)
	left := makeGeneratedChunks(t, q.Memory, buffers, 768, 113, 100000, nil)
	right := makeGeneratedChunks(t, q.Memory, buffers, 640, 113, 200000, nil)
	got, stats := runStableExternalJoinChunks(t, Full, left, right, q, buffers, 8<<10)
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
		t.Fatalf("forced-spill FULL JOIN did not spill: %+v", stats)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 || quotas.GlobalUsed() != 0 {
		t.Fatalf("leak managed=%d temp=%d", manager.Used(), quotas.GlobalUsed())
	}
}

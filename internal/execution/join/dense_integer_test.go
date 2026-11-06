package join

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
)

// denseTestChunk 构造独立的整数键和变长 payload，允许字典嵌套与常量 probe。
func denseTestChunk(t *testing.T, scope *memory.Scope, bm *memory.BufferManager, typ types.LogicalType, values []uint64, nulls map[int]bool, representation string) *vector.DataChunk {
	t.Helper()
	key, err := vector.NewFlat(scope, bm, typ, len(values))
	if err != nil {
		t.Fatal(err)
	}
	if representation == "constant" {
		key.Release()
		key, err = vector.NewConstant(scope, bm, typ, len(values))
	}
	if err != nil {
		t.Fatal(err)
	}
	for row, value := range values {
		if representation == "constant" && row > 0 {
			break
		}
		switch typ.ID {
		case types.TinyInt:
			key.SetUint8(row, uint8(value))
		case types.SmallInt:
			key.SetUint16(row, uint16(value))
		case types.Integer:
			key.SetUint32(row, uint32(value))
		case types.BigInt:
			key.SetUint64(row, value)
		}
		if nulls[row] {
			key.SetNull(row)
		}
	}
	if representation == "dictionary" {
		for depth := 0; depth < 2; depth++ {
			selection := make([]int, len(values))
			for row := range selection {
				selection[row] = len(values) - row - 1
			}
			key, err = vector.NewOwnedDictionary(scope, key, vector.SelectionFromRows(selection))
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	payload, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.Varchar}, len(values))
	if err != nil {
		t.Fatal(err)
	}
	for row := range values {
		if err := payload.SetString(row, fmt.Sprintf("payload-%d-longer-than-inline", row)); err != nil {
			t.Fatal(err)
		}
		if row%5 == 3 {
			payload.SetNull(row)
		}
	}
	chunk, err := vector.NewDataChunk(key, payload)
	if err != nil {
		t.Fatal(err)
	}
	return chunk
}

// denseReferenceRows 使用公开向量读接口做独立嵌套循环，核对键、NULL 和输出侧顺序。
func denseReferenceRows(left, right *vector.DataChunk, nullsEqual bool) []string {
	keyText := func(v *vector.Vector, row int) string {
		switch v.Type.ID {
		case types.TinyInt:
			x, _ := v.Uint8(row)
			return fmt.Sprint(x)
		case types.SmallInt:
			x, _ := v.Uint16(row)
			return fmt.Sprint(x)
		case types.Integer:
			x, _ := v.Uint32(row)
			return fmt.Sprint(x)
		default:
			x, _ := v.Uint64(row)
			return fmt.Sprint(x)
		}
	}
	payload := func(c *vector.DataChunk, row int) string {
		if !c.Columns[1].IsValid(row) {
			return "NULL"
		}
		text, _ := c.Columns[1].String(row)
		return text
	}
	var rows []string
	for l := 0; l < left.Cardinality; l++ {
		for r := 0; r < right.Cardinality; r++ {
			lv, rv := left.Columns[0].IsValid(l), right.Columns[0].IsValid(r)
			if !lv || !rv {
				if lv || rv || !nullsEqual {
					continue
				}
			} else if keyText(left.Columns[0], l) != keyText(right.Columns[0], r) {
				continue
			}
			rows = append(rows, "_|"+payload(left, l)+"|_|"+payload(right, r))
		}
	}
	sort.Strings(rows)
	return rows
}

// TestDenseIntegerJoinRepresentations 覆盖整数符号边界、范围内空洞、NULL、表示形式和双向 build。
func TestDenseIntegerJoinRepresentations(t *testing.T) {
	// integerCase 描述实际键分布和独立 probe 数据。
	type integerCase struct {
		name   string            // name 标识整数范围。
		typ    types.LogicalType // typ 保留 signed/unsigned 语义。
		values []uint64          // values 包含空洞，最后一行由 NULL 覆盖。
		probe  []uint64          // probe 包含匹配、缺失与范围外键。
	}
	cases := []integerCase{
		{"bigint_min", types.LogicalType{ID: types.BigInt}, []uint64{1 << 63, 1<<63 + 1, 1<<63 + 3, 0}, []uint64{1 << 63, 1<<63 + 2, 1<<63 + 3, math.MaxInt64, 0}},
		{"bigint_max", types.LogicalType{ID: types.BigInt}, []uint64{math.MaxInt64 - 3, math.MaxInt64 - 1, math.MaxInt64, 0}, []uint64{math.MaxInt64, math.MaxInt64 - 2, math.MaxInt64 - 3, 1 << 63, 0}},
	}
	for _, id := range []types.LogicalTypeID{types.TinyInt, types.SmallInt, types.Integer, types.BigInt} {
		typ := types.LogicalType{ID: id}
		cases = append(cases, integerCase{fmt.Sprintf("signed%d", id), typ, []uint64{^uint64(1), 0, 1, 3, 0}, []uint64{^uint64(1), ^uint64(0), 0, 2, 3, 127, 0}})
		typ.Unsigned = true
		width, _ := typ.FixedSize()
		maximum := ^uint64(0) >> uint(64-width*8)
		cases = append(cases, integerCase{fmt.Sprintf("unsigned%d", id), typ, []uint64{maximum - 3, maximum - 1, maximum, 0}, []uint64{maximum, maximum - 2, maximum - 3, 0, 0}})
	}
	for _, tc := range cases {
		for _, representation := range []string{"flat", "dictionary", "constant"} {
			for _, side := range []Side{RightSide, LeftSide} {
				for _, nullsEqual := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/build%d/null%v", tc.name, representation, side, nullsEqual), func(t *testing.T) {
						manager, bm, quotas, q := newStableExternalHarness(t)
						build := denseTestChunk(t, q.Memory, bm, tc.typ, tc.values, map[int]bool{len(tc.values) - 1: true}, "flat")
						probe := denseTestChunk(t, q.Memory, bm, tc.typ, tc.probe, map[int]bool{len(tc.probe) - 1: true}, representation)
						ts := []types.LogicalType{tc.typ, {ID: types.Varchar}}
						config := ExternalConfig{Type: Inner, NullsEqual: nullsEqual, LeftTypes: ts, RightTypes: ts, LeftKeys: []int{0}, RightKeys: []int{0}, MemoryBudget: 1 << 20, Scope: q.Memory, Buffers: bm, Temp: q.Temp}
						left, right := probe, build
						if side == LeftSide {
							config.LeftEstimatedBytes, config.RightEstimatedBytes = 1, 2
							left, right = build, probe
						}
						want := denseReferenceRows(left, right, nullsEqual)
						j, err := NewStableExternal(config)
						if err != nil {
							t.Fatal(err)
						}
						mask := []bool{false, true, false, true}
						if err := j.SetRequiredOutputColumns(mask); err != nil {
							t.Fatal(err)
						}
						if err := j.BuildSink().Consume(q.Context(), build); err != nil {
							t.Fatal(err)
						}
						build.Release()
						if err := j.BuildSink().Finalize(q.Context()); err != nil {
							t.Fatal(err)
						}
						if j.core.dense == nil {
							t.Fatal("expected dense integer index")
						}
						collector := &batchJoinCollector{mask: mask}
						sink := j.ProbeSink(collector)
						if err := sink.Consume(q.Context(), probe); err != nil {
							t.Fatal(err)
						}
						probe.Release()
						if err := sink.Finalize(q.Context()); err != nil {
							t.Fatal(err)
						}
						sort.Strings(collector.rows)
						if !reflect.DeepEqual(collector.rows, want) {
							t.Fatalf("got %v want %v", collector.rows, want)
						}
						j.Close()
						if err := q.Close(); err != nil {
							t.Fatal(err)
						}
						if manager.Used() != 0 || quotas.GlobalUsed() != 0 {
							t.Fatal("resource leak")
						}
					})
				}
			}
		}
	}
}

// TestDenseIntegerJoinFallback 保证重复键、重复 NULL、稀疏及跨整个整数域的范围保持通用语义。
func TestDenseIntegerJoinFallback(t *testing.T) {
	for _, tc := range []struct {
		name   string       // name 标识拒绝直接索引的原因。
		values []uint64     // values 是实际构建数据。
		nulls  map[int]bool // nulls 控制 NULL 重复。
	}{
		{"duplicate_holes", []uint64{0, 2, 2, 4}, nil},
		{"duplicate_nulls", []uint64{0, 1, 2, 0, 0}, map[int]bool{3: true, 4: true}},
		{"sparse", []uint64{0, 1 << 40}, nil},
		{"overflow", []uint64{1 << 63, math.MaxInt64}, nil},
		{"all_null", []uint64{0, 0}, map[int]bool{0: true, 1: true}},
		{"empty", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager, bm, quotas, q := newStableExternalHarness(t)
			typ := types.LogicalType{ID: types.BigInt}
			build := denseTestChunk(t, q.Memory, bm, typ, tc.values, tc.nulls, "flat")
			probe := denseTestChunk(t, q.Memory, bm, typ, append([]uint64{3}, tc.values...), map[int]bool{0: true}, "dictionary")
			want := denseReferenceRows(probe, build, true)
			ts := []types.LogicalType{typ, {ID: types.Varchar}}
			j, err := NewStableExternal(ExternalConfig{Type: Inner, NullsEqual: true, LeftTypes: ts, RightTypes: ts, LeftKeys: []int{0}, RightKeys: []int{0}, MemoryBudget: 1 << 20, Scope: q.Memory, Buffers: bm, Temp: q.Temp})
			if err != nil {
				t.Fatal(err)
			}
			mask := []bool{false, true, false, true}
			if err := j.SetRequiredOutputColumns(mask); err != nil {
				t.Fatal(err)
			}
			if err := j.BuildSink().Consume(q.Context(), build); err != nil {
				t.Fatal(err)
			}
			build.Release()
			if err := j.BuildSink().Finalize(q.Context()); err != nil {
				t.Fatal(err)
			}
			if j.core.dense != nil {
				t.Fatal("unexpected dense index")
			}
			collector := &batchJoinCollector{mask: mask}
			sink := j.ProbeSink(collector)
			if err := sink.Consume(q.Context(), probe); err != nil {
				t.Fatal(err)
			}
			probe.Release()
			if err := sink.Finalize(q.Context()); err != nil {
				t.Fatal(err)
			}
			sort.Strings(collector.rows)
			if !reflect.DeepEqual(collector.rows, want) {
				t.Fatalf("got %v want %v", collector.rows, want)
			}
			j.Close()
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
			if manager.Used() != 0 || quotas.GlobalUsed() != 0 {
				t.Fatal("resource leak")
			}
		})
	}
}

// TestDenseIntegerJoinAllocationFallbackAndCancellation 验证可选分配的配额回退、非配额错误及取消回收。
func TestDenseIntegerJoinAllocationFallbackAndCancellation(t *testing.T) {
	for _, failure := range []string{"quota", "allocator", "cancel", "cancel_build", "callback"} {
		t.Run(failure, func(t *testing.T) {
			manager, _, quotas, q := newStableExternalHarness(t)
			sentinel := errors.New("dense allocation failed")
			armed := false
			ctx, cancel := context.WithCancel(q.Context())
			defer cancel()
			bm := memory.NewBufferManager(func(n int) ([]byte, error) {
				if armed && n == 800 {
					switch failure {
					case "quota":
						return nil, memory.ErrLimitExceeded
					case "allocator":
						return nil, sentinel
					case "cancel_build":
						cancel()
					}
				}
				return memory.DefaultAllocator(n)
			})
			ts := []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}}
			j, err := NewExternal(ExternalConfig{Type: Inner, LeftTypes: ts, RightTypes: ts, LeftKeys: []int{0}, RightKeys: []int{0}, MemoryBudget: 1 << 20, Scope: q.Memory, Buffers: bm, Temp: q.Temp})
			if err != nil {
				t.Fatal(err)
			}
			build := makeGeneratedChunks(t, q.Memory, bm, 100, 100, 0, nil)[0]
			if err := j.BuildSink().Consume(q.Context(), build); err != nil {
				t.Fatal(err)
			}
			build.Release()
			armed = true
			err = j.BuildSink().Finalize(ctx)
			if failure == "allocator" {
				if !errors.Is(err, sentinel) {
					t.Fatalf("error %v", err)
				}
			} else if failure == "cancel_build" {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("error %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if (j.dense == nil) != (failure == "quota") {
					t.Fatal("incorrect index selection")
				}
				probe := makeGeneratedChunks(t, q.Memory, bm, 2048, 100, 1000, nil)[0]
				sink := j.ProbeSink(&benchmarkJoinSink{})
				if failure == "callback" {
					sink = j.ProbeSink(cancelBatchSink{cancel: cancel})
				}
				if failure == "cancel" {
					cancel()
				}
				err = sink.Consume(ctx, probe)
				probe.Release()
				if (failure == "cancel" || failure == "callback") && !errors.Is(err, context.Canceled) {
					t.Fatalf("error %v", err)
				}
				if failure == "quota" && err != nil {
					t.Fatal(err)
				}
			}
			j.Close()
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
			if manager.Used() != 0 || quotas.GlobalUsed() != 0 {
				t.Fatal("resource leak")
			}
		})
	}
}

// TestDenseIntegerJoinFourWorkers 验证只读直接索引可跨多个 probe 批次并发借用。
func TestDenseIntegerJoinFourWorkers(t *testing.T) {
	want := runInnerReference(t, 8192, 1000, 1000)
	manager, bm, quotas, q := newStableExternalHarness(t)
	ts := []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}}
	j, err := NewStableExternal(ExternalConfig{Type: Inner, LeftTypes: ts, RightTypes: ts, LeftKeys: []int{0}, RightKeys: []int{0}, MemoryBudget: 1 << 20, Scope: q.Memory, Buffers: bm, Temp: q.Temp})
	if err != nil {
		t.Fatal(err)
	}
	build := makeGeneratedChunks(t, q.Memory, bm, 1000, 1000, 200000, nil)[0]
	if err := j.BuildSink().Consume(q.Context(), build); err != nil {
		t.Fatal(err)
	}
	build.Release()
	if err := j.BuildSink().Finalize(q.Context()); err != nil {
		t.Fatal(err)
	}
	if j.core.dense == nil {
		t.Fatal("expected dense index")
	}
	collector := &lockedRowCollector{}
	probe := &pipeline.Pipeline{ID: "dense-probe", Source: &chunkSource{chunks: makeGeneratedChunks(t, q.Memory, bm, 8192, 1000, 100000, nil)}, Sink: j.ProbeSink(collector)}
	if err := pipeline.RunDAG(q.Context(), probe, 4, q); err != nil {
		t.Fatal(err)
	}
	sort.Strings(collector.rows)
	sort.Strings(want)
	if !reflect.DeepEqual(collector.rows, want) {
		t.Fatalf("rows=%d want=%d", len(collector.rows), len(want))
	}
	j.Close()
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 || quotas.GlobalUsed() != 0 {
		t.Fatal("resource leak")
	}
}

// TestDenseIntegerJoinSpillFallback 验证相同的唯一键 JOIN 在预算迫使溢写时仍返回完整且相同的结果。
func TestDenseIntegerJoinSpillFallback(t *testing.T) {
	want := runInnerReference(t, 2048, 512, 600)
	got, stats, _ := runExternalInner(t, 2048, 512, 600, 4096, 2048*16, 512*16, nil)
	sort.Strings(want)
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows=%d want=%d", len(got), len(want))
	}
	if stats.SpilledBuildPartitions == 0 {
		t.Fatal("expected spill fallback")
	}
}

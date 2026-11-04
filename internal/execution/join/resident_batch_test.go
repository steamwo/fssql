package join

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
)

type batchJoinCollector struct {
	mu   sync.Mutex
	mask []bool
	rows []string
	err  error
}

func (c *batchJoinCollector) Consume(_ context.Context, chunk *vector.DataChunk) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, col := range chunk.Columns {
		if c.mask != nil && !c.mask[i] && (col.Kind != vector.Constant || col.IsValid(0)) {
			return fmt.Errorf("column %d should be compact NULL", i)
		}
	}
	for row := 0; row < chunk.Cardinality; row++ {
		parts := make([]string, len(chunk.Columns))
		for i, col := range chunk.Columns {
			if c.mask != nil && !c.mask[i] {
				parts[i] = "_"
				continue
			}
			if !col.IsValid(row) {
				parts[i] = "NULL"
				continue
			}
			if col.Type.ID == types.Varchar {
				parts[i], _ = col.String(row)
			} else {
				value, _ := col.Int64(row)
				parts[i] = fmt.Sprint(value)
			}
		}
		c.rows = append(c.rows, strings.Join(parts, "|"))
	}
	return nil
}
func (*batchJoinCollector) Finalize(context.Context) error { return nil }

func dictionaryJoinChunk(t *testing.T, scope *memory.Scope, bm *memory.BufferManager, n, base int, side string) *vector.DataChunk {
	t.Helper()
	key, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.Varchar}, 29)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 29; i++ {
		if err := key.SetString(i, fmt.Sprintf("key-%02d-external-string", i)); err != nil {
			t.Fatal(err)
		}
	}
	key.SetNull(28)
	selection := make([]int, n)
	for i := range selection {
		selection[i] = (i + base) % 29
	}
	dict, err := vector.NewOwnedDictionary(scope, key, vector.SelectionFromRows(selection))
	if err != nil {
		t.Fatal(err)
	}
	// Nest dictionaries to exercise borrowed key views through both selections.
	reverse := make([]int, n)
	for i := range reverse {
		reverse[i] = n - 1 - i
	}
	dict, err = vector.NewOwnedDictionary(scope, dict, vector.SelectionFromRows(reverse))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := vector.NewConstant(scope, bm, types.LogicalType{ID: types.Varchar}, n)
	if err != nil {
		t.Fatal(err)
	}
	if err := payload.SetString(0, side+" constant payload longer than inline"); err != nil {
		t.Fatal(err)
	}
	value, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.BigInt}, n)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		value.SetInt64(i, int64(base+i))
	}
	chunk, err := vector.NewDataChunk(dict, payload, value)
	if err != nil {
		t.Fatal(err)
	}
	return chunk
}

func expectedDictionaryJoin(typ Type, left, right *vector.DataChunk, mask []bool) []string {
	var out []string
	matchedRight := make([]bool, right.Cardinality)
	appendRow := func(l, r int) {
		columns := 6
		if typ == Semi || typ == Anti {
			columns = 3
		}
		parts := make([]string, columns)
		for i := range parts {
			if mask != nil && !mask[i] {
				parts[i] = "_"
				continue
			}
			chunk, row, index := left, l, i
			if i >= 3 {
				chunk, row, index = right, r, i-3
			}
			if row < 0 || !chunk.Columns[index].IsValid(row) {
				parts[i] = "NULL"
				continue
			}
			col := chunk.Columns[index]
			if col.Type.ID == types.Varchar {
				parts[i], _ = col.String(row)
			} else {
				value, _ := col.Int64(row)
				parts[i] = fmt.Sprint(value)
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	for l := 0; l < left.Cardinality; l++ {
		matched := false
		lk, lv := left.Columns[0].String(l)
		for r := 0; r < right.Cardinality; r++ {
			rk, rv := right.Columns[0].String(r)
			if !lv || !rv || lk != rk {
				continue
			}
			matched = true
			matchedRight[r] = true
			if typ == Anti {
				continue
			}
			appendRow(l, r)
			if typ == Semi {
				break
			}
		}
		if !matched && (typ == Left || typ == Full || typ == Anti) {
			appendRow(l, -1)
		}
	}
	if typ == Right || typ == Full {
		for r, matched := range matchedRight {
			if !matched {
				appendRow(-1, r)
			}
		}
	}
	sort.Strings(out)
	return out
}

func TestResidentBatchDictionaryConstantAndRequiredOutput(t *testing.T) {
	for _, typ := range []Type{Inner, Left, Right, Full, Semi, Anti} {
		for _, budget := range []int64{1 << 20, 4096} {
			for _, maskKind := range []string{"all", "payload", "none"} {
				sides := []Side{RightSide}
				if typ == Inner {
					sides = append(sides, LeftSide)
				}
				for _, side := range sides {
					t.Run(fmt.Sprintf("type%d/budget%d/%s/build%d", typ, budget, maskKind, side), func(t *testing.T) {
						manager, bm, quotas, q := newStableExternalHarness(t)
						left := dictionaryJoinChunk(t, q.Memory, bm, 151, 10, "left")
						right := dictionaryJoinChunk(t, q.Memory, bm, 117, 1000, "right")
						width := 6
						if typ == Semi || typ == Anti {
							width = 3
						}
						var mask []bool
						if maskKind != "all" {
							mask = make([]bool, width)
							if maskKind == "payload" {
								mask[1] = true
								mask[2] = true
								if width == 6 {
									mask[4] = true
								}
							}
						}
						want := expectedDictionaryJoin(typ, left, right, mask)
						ts := []types.LogicalType{{ID: types.Varchar}, {ID: types.Varchar}, {ID: types.BigInt}}
						config := ExternalConfig{Type: typ, LeftTypes: ts, RightTypes: ts, LeftKeys: []int{0}, RightKeys: []int{0}, MemoryBudget: budget, Scope: q.Memory, Buffers: bm, Temp: q.Temp, RadixBits: 2, MaxDepth: 3, SpillBlockBytes: 1024}
						if side == LeftSide {
							config.LeftEstimatedBytes = 1
							config.RightEstimatedBytes = 2
						}
						j, err := NewStableExternal(config)
						if err != nil {
							t.Fatal(err)
						}
						if err := j.SetRequiredOutputColumns(mask); err != nil {
							t.Fatal(err)
						}
						buildChunk, probeChunk := right, left
						if j.BuildSide() == LeftSide {
							buildChunk, probeChunk = left, right
						}
						collector := &batchJoinCollector{mask: mask}
						build := &pipeline.Pipeline{ID: "batch-build", Source: &chunkSource{chunks: []*vector.DataChunk{buildChunk}}, Sink: j.BuildSink()}
						probe := &pipeline.Pipeline{ID: "batch-probe", Source: &chunkSource{chunks: []*vector.DataChunk{probeChunk}}, Sink: j.ProbeSink(collector), Dependencies: []*pipeline.Pipeline{build}}
						if err := pipeline.RunDAG(q.Context(), probe, 4, q); err != nil {
							j.Close()
							t.Fatal(err)
						}
						sort.Strings(collector.rows)
						if !reflect.DeepEqual(collector.rows, want) {
							t.Fatalf("result mismatch rows=%d want=%d", len(collector.rows), len(want))
						}
						if budget == 4096 && j.Stats().SpilledBuildPartitions == 0 {
							t.Fatal("expected spill")
						}
						j.Close()
						if err := q.Close(); err != nil {
							t.Fatal(err)
						}
						if manager.Used() != 0 || quotas.GlobalUsed() != 0 {
							t.Fatalf("leak memory=%d temp=%d", manager.Used(), quotas.GlobalUsed())
						}
					})
				}
			}
		}
	}
}

type cancelBatchSink struct{ cancel context.CancelFunc }

func (s cancelBatchSink) Consume(ctx context.Context, _ *vector.DataChunk) error {
	s.cancel()
	return ctx.Err()
}
func (cancelBatchSink) Finalize(context.Context) error { return nil }

func TestBatchProbeCancellationReleasesResidentAndSpill(t *testing.T) {
	for _, budget := range []int64{1 << 20, 4096} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			manager, bm, quotas, q := newStableExternalHarness(t)
			ts := []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}}
			j, err := NewStableExternal(ExternalConfig{Type: Full, LeftTypes: ts, RightTypes: ts, LeftKeys: []int{0}, RightKeys: []int{0}, MemoryBudget: budget, Scope: q.Memory, Buffers: bm, Temp: q.Temp})
			if err != nil {
				t.Fatal(err)
			}
			build := makeGeneratedChunks(t, q.Memory, bm, 128, 1, 0, nil)[0]
			if err := j.BuildSink().Consume(q.Context(), build); err != nil {
				t.Fatal(err)
			}
			build.Release()
			if err := j.BuildSink().Finalize(q.Context()); err != nil {
				t.Fatal(err)
			}
			probe := makeGeneratedChunks(t, q.Memory, bm, 128, 1, 1000, nil)[0]
			ctx, cancel := context.WithCancel(q.Context())
			defer cancel()
			sink := j.ProbeSink(cancelBatchSink{cancel: cancel})
			err = sink.Consume(ctx, probe)
			probe.Release()
			if err == nil {
				err = sink.Finalize(ctx)
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("error=%v", err)
			}
			j.Close()
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
			if manager.Used() != 0 || quotas.GlobalUsed() != 0 {
				t.Fatalf("leak memory=%d temp=%d", manager.Used(), quotas.GlobalUsed())
			}
		})
	}
}

func TestBatchProbeManyChunksFourWorkers(t *testing.T) {
	for _, typ := range []Type{Inner, Full} {
		for _, budget := range []int64{1 << 20, 8 << 10} {
			t.Run(fmt.Sprintf("type%d/budget%d", typ, budget), func(t *testing.T) {
				want := runGeneratedReference(t, typ, 4096, 320, 113)
				manager, bm, quotas, q := newStableExternalHarness(t)
				var left []*vector.DataChunk
				for start := 0; start < 4096; start += 256 {
					keys, values := make([]nullableInt, 256), make([]nullableInt, 256)
					for row := range keys {
						keys[row] = ni(int64((start + row) % 113))
						values[row] = ni(int64(100000 + start + row))
					}
					left = append(left, makeIntChunk(t, q.Memory, bm, keys, values))
				}
				right := makeGeneratedChunks(t, q.Memory, bm, 320, 113, 200000, nil)
				got, stats := runStableExternalJoinChunksWorkers(t, typ, left, right, q, bm, budget, 4)
				sort.Strings(got)
				sort.Strings(want)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("result mismatch rows=%d want=%d stats=%+v", len(got), len(want), stats)
				}
				if budget == 8<<10 && stats.SpilledBuildPartitions == 0 {
					t.Fatal("expected spill")
				}
				if err := q.Close(); err != nil {
					t.Fatal(err)
				}
				if manager.Used() != 0 || quotas.GlobalUsed() != 0 {
					t.Fatalf("leak memory=%d temp=%d", manager.Used(), quotas.GlobalUsed())
				}
			})
		}
	}
}

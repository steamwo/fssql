package session

import (
	"container/heap"
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/blocking"
	"github.com/steamwo/fssql/internal/query"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

// DuckDB v1.5.5/v2.0.0 reservoir algorithms: shuffled initial fill, vector-wise
// replacement, then weighted skip sampling after 2048*60 rows. Page IDs are
// reservoir slots; obsolete rows are deleted instead of copying/vacuuming a
// growing chunk. This preserves the RNG/selection sequence with bounded spill.
type duckReservoir struct {
	rng           *sampleRandom
	k, seen       uint64
	pages         []storagetemp.PageID
	weights       reservoirWeights
	next, skipped uint64
	msvc          bool
	metadata      *memory.Reservation
}

type reservoirWeight struct {
	weight float64
	slot   int
}
type reservoirWeights []reservoirWeight

func (h reservoirWeights) Len() int { return len(h) }
func (h reservoirWeights) Less(i, j int) bool {
	return h[i].weight < h[j].weight || h[i].weight == h[j].weight && h[i].slot > h[j].slot
}
func (h reservoirWeights) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *reservoirWeights) Push(v any)   { *h = append(*h, v.(reservoirWeight)) }
func (h *reservoirWeights) Pop() any {
	old := *h
	v := old[len(old)-1]
	*h = old[:len(old)-1]
	return v
}

type duckDBReservoirOperator struct {
	storage               *sampleOperator
	current               *duckReservoir
	finished              []*duckReservoir
	rng                   *sampleRandom
	percent               float64
	percentage, finalized bool
	count                 uint64
	msvc                  bool
	samples               []*duckReservoir
}

func newDuckDBReservoirOperator(q *query.Context, buffers *memory.BufferManager, raw, profile string) (blocking.Operator, error) {
	spec, err := parseSampleSpec(raw)
	if err != nil {
		return nil, err
	}
	if spec.seeded && spec.seed > math.MaxInt64 {
		return nil, fmt.Errorf("session: DuckDB sampling seed exceeds signed int64")
	}
	seed := spec.seed
	if !spec.seeded {
		var b [8]byte
		if _, err := cryptorand.Read(b[:]); err != nil {
			return nil, err
		}
		seed = binary.LittleEndian.Uint64(b[:])
	}
	store, err := storagetemp.NewSpillablePageStore(q.Temp, q.Memory, buffers, storagetemp.PageStoreConfig{SegmentMaxBytes: 64 << 20, Compression: storagetemp.CompressionNone})
	if err != nil {
		return nil, err
	}
	s := &duckDBReservoirOperator{storage: &sampleOperator{q: q, buffers: buffers, store: store}, rng: newSampleRandom(seed), msvc: profile == "duckdb-msvc", percentage: spec.method == 'p'}
	k := spec.amount
	if s.percentage {
		s.percent = math.Float64frombits(spec.amount) / 100
		k = uint64(s.percent * 100000)
		seed = uint64(s.rng.next32())
	}
	s.current = s.newReservoir(k, seed)
	return s, nil
}

func (s *duckDBReservoirOperator) newReservoir(k, seed uint64) *duckReservoir {
	r := &duckReservoir{k: k, rng: newSampleRandom(seed), msvc: s.msvc}
	s.samples = append(s.samples, r)
	return r
}

func (s *duckDBReservoirOperator) Consume(ctx context.Context, chunk *vector.DataChunk) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if chunk == nil || s.finalized {
		return fmt.Errorf("session: invalid DuckDB sample input/state")
	}
	if !s.percentage {
		return s.current.consume(ctx, s.storage, chunk, 0, chunk.Cardinality)
	}
	if s.percent == 0 {
		return nil
	}
	for offset := 0; offset < chunk.Cardinality; {
		if s.count == 100000 {
			s.finished = append(s.finished, s.current)
			s.current = s.newReservoir(uint64(s.percent*100000), uint64(s.rng.next32()))
			s.count = 0
		}
		n := min(chunk.Cardinality-offset, int(100000-s.count))
		if err := s.current.consume(ctx, s.storage, chunk, offset, n); err != nil {
			return err
		}
		s.count += uint64(n)
		offset += n
	}
	return nil
}

func (s *duckDBReservoirOperator) Emit(ctx context.Context, emit func(*vector.DataChunk) error) error {
	if s.finalized {
		return fmt.Errorf("session: DuckDB sample already emitted")
	}
	s.finalized = true
	if !s.percentage {
		return s.emit(ctx, s.current, emit)
	}
	if s.count > 0 && (float64(s.count) > s.percent*100000 || len(s.finished) == 0) {
		resampled := s.newReservoir(uint64(math.Round(s.percent*float64(s.count))), uint64(s.rng.next32()))
		// DuckDB replays the provisional sample in vectors of at most 2048 rows.
		if err := s.emit(ctx, s.current, func(chunk *vector.DataChunk) error {
			err := resampled.consume(ctx, s.storage, chunk, 0, chunk.Cardinality)
			if err == nil {
				chunk.Release()
			}
			return err
		}); err != nil {
			return err
		}
		for _, id := range s.current.pages {
			if err := s.storage.store.DeletePage(id); err != nil {
				return err
			}
		}
		s.finished = append(s.finished, resampled)
	} else {
		s.finished = append(s.finished, s.current)
	}
	for _, sample := range s.finished {
		if err := s.emit(ctx, sample, emit); err != nil {
			return err
		}
	}
	return nil
}

func (s *duckDBReservoirOperator) emit(ctx context.Context, r *duckReservoir, emit func(*vector.DataChunk) error) error {
	order := make([]int, len(r.pages))
	index := 0
	for remaining := len(r.pages); remaining > 0; {
		start := max(0, remaining-2048)
		for slot := start; slot < remaining; slot++ {
			order[index] = slot
			index++
		}
		remaining = start
	}
	if len(r.weights) > 0 {
		copyWeights := append(reservoirWeights(nil), r.weights...)
		for i := range order {
			order[i] = heap.Pop(&copyWeights).(reservoirWeight).slot
		}
	}
	for start := 0; start < len(order); start += 2048 {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := min(start+2048, len(order))
		var output *vector.DataChunk
		for row, slot := range order[start:end] {
			page, err := s.storage.store.ReadPage(ctx, r.pages[slot])
			if err != nil {
				if output != nil {
					output.Release()
				}
				return err
			}
			one, err := vector.DeserializeChunk(page.Bytes(), s.storage.q.Memory, s.storage.buffers)
			page.Release()
			if err != nil {
				if output != nil {
					output.Release()
				}
				return err
			}
			if output == nil {
				// Allocate the final vector directly; dummy repeated row indices only
				// size the chunk and are overwritten below with each selected row.
				output, err = copyRows(s.storage.q.Memory, s.storage.buffers, one, make([]int, end-start))
				if err != nil {
					one.Release()
					return err
				}
			}
			for col := range one.Columns {
				if err := vector.CopyValue(one.Columns[col], 0, output.Columns[col], row); err != nil {
					one.Release()
					output.Release()
					return err
				}
			}
			one.Release()
		}
		if err := emit(output); err != nil {
			output.Release()
			return err
		}
	}
	return nil
}

func (s *duckDBReservoirOperator) Release() {
	if s == nil {
		return
	}
	s.storage.Release()
	for _, sample := range s.samples {
		if sample.metadata != nil {
			sample.metadata.Release()
			sample.metadata = nil
		}
	}
	s.samples = nil
	s.current = nil
	s.finished = nil
}

func (r *duckReservoir) consume(ctx context.Context, s *sampleOperator, chunk *vector.DataChunk, start, n int) error {
	if r.k == 0 || n == 0 {
		return nil
	}
	if uint64(len(r.pages)) < r.k {
		take := min(n, int(min(r.k-uint64(len(r.pages)), uint64(n))))
		indexes := r.randomIndexes(take, take)
		for _, index := range indexes {
			if err := r.save(ctx, s, chunk, start+index, len(r.pages)); err != nil {
				return err
			}
		}
		r.seen += uint64(take)
		start += take
		n -= take
		if n == 0 {
			return nil
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	added := 0
	if len(r.weights) == 0 {
		count := int(math.Round(float64(n) / float64(r.seen+uint64(n)) * float64(r.k)))
		indexes := r.randomIndexes(n, count)
		slots := r.randomIndexes(len(r.pages), count)
		for i := 0; i < count; i++ {
			if err := r.save(ctx, s, chunk, start+indexes[i], slots[i]); err != nil {
				return err
			}
		}
		added = count
	} else {
		remaining, offset := uint64(n), uint64(0)
		for {
			skip := r.next - r.skipped
			if skip >= remaining {
				r.skipped += remaining
				break
			}
			offset += skip
			remaining -= skip
			top := heap.Pop(&r.weights).(reservoirWeight)
			if err := r.save(ctx, s, chunk, start+int(offset), top.slot); err != nil {
				return err
			}
			top.weight = top.weight + r.float32()*(1-top.weight)
			heap.Push(&r.weights, top)
			r.setNext()
			added++
		}
	}
	r.seen += uint64(n)
	if added > 0 && len(r.weights) == 0 && r.seen >= 2048*60 {
		r.fillWeights()
	}
	return nil
}

func (r *duckReservoir) save(ctx context.Context, s *sampleOperator, chunk *vector.DataChunk, row, slot int) error {
	if slot == len(r.pages) {
		if r.metadata == nil {
			var err error
			r.metadata, err = s.q.Memory.Reserve(memory.TagMetadata, 0)
			if err != nil {
				return err
			}
		}
		// Cover slots, heap weights, output ordering and shuffle scratch including
		// slice growth. Candidate metadata is governed even when row data spills.
		if err := r.metadata.Resize(int64(slot+1) * 96); err != nil {
			return err
		}
	}
	one, err := copyRows(s.q.Memory, s.buffers, chunk, []int{row})
	if err != nil {
		return err
	}
	encoded, err := vector.SerializeChunk(one, s.q.Memory, s.buffers)
	one.Release()
	if err != nil {
		return err
	}
	id, err := s.store.WritePage(ctx, encoded.Bytes())
	encoded.Release()
	if err != nil {
		return err
	}
	if slot == len(r.pages) {
		r.pages = append(r.pages, id)
	} else {
		old := r.pages[slot]
		r.pages[slot] = id
		if err := s.store.DeletePage(old); err != nil {
			return err
		}
	}
	return nil
}

func (r *duckReservoir) float32() float64 { return float64(r.rng.next32()) * 0x1p-32 }
func (r *duckReservoir) randomIndexes(n, k int) []int {
	indexes := make([]int, n)
	for i := range indexes {
		indexes[i] = i
	}
	for i := 0; i < k; i++ {
		j := i + int(r.float32()*float64(n-i))
		indexes[i], indexes[j] = indexes[j], indexes[i]
	}
	return indexes[:k]
}

func (r *duckReservoir) setNext() {
	x := math.Log(r.float32()) / math.Log(r.weights[0].weight)
	r.next = 1
	if x >= 1 && !math.IsInf(x, 0) && x < float64(math.MaxUint64) {
		r.next = uint64(math.Ceil(x))
	}
	r.skipped = 0
}

func (r *duckReservoir) fillWeights() {
	weights := make([]float64, len(r.pages))
	bound := 1.0
	for i := range weights {
		bound *= math.Pow(r.rng.Float64(), 1/float64(r.seen-uint64(i)))
		weights[i] = bound
	}
	r.shuffleWeights(weights)
	for i, w := range weights {
		r.weights = append(r.weights, reservoirWeight{weight: math.Max(w, 0x1p-1022), slot: i})
	}
	heap.Init(&r.weights)
	r.setNext()
}

// std::shuffle is implementation-defined. Profiles pin the standard library
// as well as DuckDB: MSVC's modulo/rejection or libstdc++'s paired swaps and
// Lemire multiply-high distribution. No Go stdlib RNG participates.
func (r *duckReservoir) shuffleWeights(weights []float64) {
	if r.msvc {
		for i := 1; i < len(weights); i++ {
			n := uint64(i + 1)
			var v uint64
			for {
				v = uint64(r.rng.next32())
				if v/n < uint64(math.MaxUint32)/n || uint64(math.MaxUint32)%n == n-1 {
					break
				}
			}
			j := int(v % n)
			weights[i], weights[j] = weights[j], weights[i]
		}
		return
	}
	i := 1
	if len(weights) > 0 && uint64(len(weights))*uint64(len(weights)) <= math.MaxUint32 {
		if len(weights)%2 == 0 {
			j := r.uniform(2)
			weights[1], weights[j] = weights[j], weights[1]
			i++
		}
		for i < len(weights) {
			n := i + 1
			x := r.uniform(uint32(n * (n + 1)))
			a, b := x/(n+1), x%(n+1)
			weights[i], weights[a] = weights[a], weights[i]
			i++
			weights[i], weights[b] = weights[b], weights[i]
			i++
		}
		return
	}
	for ; i < len(weights); i++ {
		j := r.uniform(uint32(i + 1))
		weights[i], weights[j] = weights[j], weights[i]
	}
}
func (r *duckReservoir) uniform(n uint32) int {
	for {
		product := uint64(r.rng.next32()) * uint64(n)
		if uint32(product) >= -n%n {
			return int(product >> 32)
		}
	}
}

package session

import (
	"math/bits"
	"math/rand"
)

type samplingRandom interface {
	Float64() float64
	Uint64() uint64
	Int63n(int64) int64
}

func newSamplingRandom(seed uint64, method byte) samplingRandom {
	if method == 'b' || method == 's' {
		return newSampleRandom(seed)
	}
	// Reservoir has its own algorithm contract. Preserve the existing native
	// stream until the complete DuckDB shuffle/replacement path is available.
	return rand.New(rand.NewSource(int64(seed)))
}

// sampleRandom follows DuckDB RandomEngine's PCG-XSH-RR 64/32 seed and double
// construction. Reference: duckdb v1.5.5 src/common/random_engine.cpp and
// third_party/pcg/pcg_random.hpp. This generator is only for SQL sampling.
type sampleRandom struct{ state uint64 }

const sampleMultiplier uint64 = 6364136223846793005
const sampleIncrement uint64 = 1442695040888963407

func newSampleRandom(seed uint64) *sampleRandom {
	return &sampleRandom{state: (seed+sampleIncrement)*sampleMultiplier + sampleIncrement}
}
func (r *sampleRandom) next32() uint32 {
	old := r.state
	r.state = old*sampleMultiplier + sampleIncrement
	x := uint32(((old >> 18) ^ old) >> 27)
	return bits.RotateLeft32(x, -int(old>>59))
}
func (r *sampleRandom) Float64() float64 {
	v := r.Uint64()
	return float64(v) * 0x1p-64
}
func (r *sampleRandom) Uint64() uint64 { return uint64(r.next32())<<32 | uint64(r.next32()) }
func (r *sampleRandom) Int63n(n int64) int64 {
	// Rejection preserves uniformity for the native reservoir path.
	limit := uint64(1) << 63
	limit -= limit % uint64(n)
	for {
		v := (uint64(r.next32())<<32 | uint64(r.next32())) >> 1
		if v < limit {
			return int64(v % uint64(n))
		}
	}
}

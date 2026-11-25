package vfs

import (
	"context"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
)

type inputStatsKey struct{}

// InputStats records logical reads at the format boundary for one query.
type InputStats struct {
	mu              sync.Mutex
	files           map[string]uint8
	matched, read   int
	requests, bytes int64
}

const (
	inputMatched uint8 = 1 << iota
	inputRead
)

func WithInputStats(ctx context.Context) (context.Context, *InputStats) {
	stats := &InputStats{files: make(map[string]uint8)}
	return context.WithValue(ctx, inputStatsKey{}, stats), stats
}

func (s *InputStats) Snapshot() (matched, read int, requests, bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.matched, s.read, s.requests, s.bytes
}

func MatchObserver(ctx context.Context) func(URI) {
	s, _ := ctx.Value(inputStatsKey{}).(*InputStats)
	if s == nil {
		return nil
	}
	return func(uri URI) {
		key := uri.IdentityKey()
		s.mu.Lock()
		if s.files[key]&inputMatched == 0 {
			s.files[key] |= inputMatched
			s.matched++
		}
		s.mu.Unlock()
	}
}

type RangeReadFunc func(context.Context, RangeRequest, *memory.Scope, *memory.BufferManager) (RangeResult, error)

// ReadInputRange observes completed input reads, including footer and sample reads.
func ReadInputRange(ctx context.Context, read RangeReadFunc, req RangeRequest, scope *memory.Scope, bm *memory.BufferManager) (RangeResult, error) {
	result, err := read(ctx, req, scope, bm)
	if err != nil || result.Buffer == nil {
		return result, err
	}
	s, _ := ctx.Value(inputStatsKey{}).(*InputStats)
	if s != nil {
		key, size := req.URI.IdentityKey(), result.Buffer.Len()
		s.mu.Lock()
		if s.files[key]&inputRead == 0 {
			s.files[key] |= inputRead
			s.read++
		}
		s.requests++
		s.bytes += int64(size)
		s.mu.Unlock()
	}
	return result, nil
}

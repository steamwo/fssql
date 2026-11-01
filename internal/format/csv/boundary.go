package csv

import "fmt"

// BoundaryState is the complete lexical state needed to continue scanning at
// an arbitrary byte boundary. SkipLF carries the CRLF normalization state when
// a coarse chunk ends immediately after CR.
type BoundaryState struct {
	Lex    LexState
	SkipLF bool
}

type stateTransition struct {
	End BoundaryState
	Err error
}

// ChunkSummary is a constant-size transition table for one coarse input chunk.
// It allows independently summarized chunks to be prefix-composed without
// assuming that a chunk starts outside a quoted field.
type ChunkSummary struct {
	transitions [8]stateTransition
}

func stateIndex(s BoundaryState) (int, error) {
	if s.Lex > StateAfterQuote {
		return 0, fmt.Errorf("csv: invalid boundary lexical state %d", s.Lex)
	}
	idx := int(s.Lex) * 2
	if s.SkipLF {
		idx++
	}
	return idx, nil
}

func stateForIndex(idx int) BoundaryState {
	return BoundaryState{Lex: LexState(idx / 2), SkipLF: idx%2 == 1}
}

// SummarizeChunk computes the lexical transition for every possible starting
// state using the same Machine as CSV parsing. The summary is independent of
// the actual predecessor chunk and is therefore safe to build in parallel.
func SummarizeChunk(input []byte, cfg LexConfig) (ChunkSummary, error) {
	if cfg == (LexConfig{}) {
		cfg = DefaultLexConfig()
	}
	if err := cfg.Validate(); err != nil {
		return ChunkSummary{}, err
	}
	var summary ChunkSummary
	var machines [8]Machine
	var failed [8]bool
	for i := range machines {
		s := stateForIndex(i)
		machines[i] = Machine{cfg: cfg, state: s.Lex, skipLF: s.SkipLF}
	}
	for _, b := range input {
		for i := range machines {
			if failed[i] {
				continue
			}
			if _, err := machines[i].Step(b); err != nil {
				summary.transitions[i].Err = err
				failed[i] = true
			}
		}
	}
	for i := range machines {
		if failed[i] {
			continue
		}
		summary.transitions[i].End = BoundaryState{Lex: machines[i].state, SkipLF: machines[i].skipLF}
	}
	return summary, nil
}

// ResolveChunkStarts prefix-composes chunk summaries from the canonical file
// start state. It returns one actual start state per chunk plus the final state.
func ResolveChunkStarts(summaries []ChunkSummary) ([]BoundaryState, BoundaryState, error) {
	starts := make([]BoundaryState, len(summaries))
	current := BoundaryState{Lex: StateFieldStart}
	for i := range summaries {
		starts[i] = current
		idx, err := stateIndex(current)
		if err != nil {
			return nil, BoundaryState{}, err
		}
		transition := summaries[i].transitions[idx]
		if transition.Err != nil {
			return nil, BoundaryState{}, fmt.Errorf("csv: chunk %d lexical transition: %w", i, transition.Err)
		}
		current = transition.End
	}
	return starts, current, nil
}

// ValidateFinalBoundaryState applies the normal CSV EOF rule to a composed
// final state.
func ValidateFinalBoundaryState(cfg LexConfig, state BoundaryState) error {
	if cfg == (LexConfig{}) {
		cfg = DefaultLexConfig()
	}
	if _, err := stateIndex(state); err != nil {
		return err
	}
	m := Machine{cfg: cfg, state: state.Lex, skipLF: state.SkipLF}
	return m.Finish()
}

// FirstSafeRecordBoundary returns the byte offset immediately after the first
// complete record separator in input when scanning from start. A CR at the last
// byte of the chunk is intentionally not reported because the planner cannot
// yet know whether the next chunk starts with LF; choosing that split could
// manufacture an empty row when partitions are decoded independently.
func FirstSafeRecordBoundary(input []byte, cfg LexConfig, start BoundaryState) (offset int, ok bool, end BoundaryState, err error) {
	if cfg == (LexConfig{}) {
		cfg = DefaultLexConfig()
	}
	if err := cfg.Validate(); err != nil {
		return 0, false, BoundaryState{}, err
	}
	if _, err := stateIndex(start); err != nil {
		return 0, false, BoundaryState{}, err
	}
	m := Machine{cfg: cfg, state: start.Lex, skipLF: start.SkipLF}
	for i, b := range input {
		event, stepErr := m.Step(b)
		if stepErr != nil {
			return 0, false, BoundaryState{}, stepErr
		}
		if event.Has(EventRecordEnd) {
			if b == '\r' {
				if i+1 == len(input) {
					return 0, false, BoundaryState{Lex: m.state, SkipLF: m.skipLF}, nil
				}
				if input[i+1] == '\n' {
					return i + 2, true, BoundaryState{Lex: StateFieldStart}, nil
				}
			}
			return i + 1, true, BoundaryState{Lex: m.state, SkipLF: m.skipLF}, nil
		}
	}
	return 0, false, BoundaryState{Lex: m.state, SkipLF: m.skipLF}, nil
}

// SplitCandidate describes one already-fetched coarse range. Data ownership is
// external; this planner stores no payload and performs no I/O fallback.
type SplitCandidate struct {
	Base int64
	Data []byte
}

// PlanSafeSplitOffsets returns record-aligned offsets for coarse ranges. A
// candidate with no complete safe boundary contributes no split, which reduces
// parallelism instead of guessing a boundary.
func PlanSafeSplitOffsets(candidates []SplitCandidate, cfg LexConfig) ([]int64, error) {
	if len(candidates) < 2 {
		return nil, nil
	}
	summaries := make([]ChunkSummary, len(candidates))
	for i := range candidates {
		summary, err := SummarizeChunk(candidates[i].Data, cfg)
		if err != nil {
			return nil, err
		}
		summaries[i] = summary
	}
	starts, finalState, err := ResolveChunkStarts(summaries)
	if err != nil {
		return nil, err
	}
	if err := ValidateFinalBoundaryState(cfg, finalState); err != nil {
		return nil, err
	}

	offsets := make([]int64, 0, len(candidates)-1)
	for i := 1; i < len(candidates); i++ {
		offset, ok, _, err := FirstSafeRecordBoundary(candidates[i].Data, cfg, starts[i])
		if err != nil {
			return nil, fmt.Errorf("csv: split candidate %d: %w", i, err)
		}
		if !ok {
			continue
		}
		absolute := candidates[i].Base + int64(offset)
		if len(offsets) == 0 || absolute > offsets[len(offsets)-1] {
			offsets = append(offsets, absolute)
		}
	}
	return offsets, nil
}

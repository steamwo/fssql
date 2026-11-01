package asyncio

import (
	"fmt"
	"sort"
)

type ByteRange struct {
	Offset int64
	Length int64
}

func (r ByteRange) End() int64 { return r.Offset + r.Length }

func (r ByteRange) validate() error {
	if r.Offset < 0 || r.Length <= 0 {
		return fmt.Errorf("asyncio: invalid range offset=%d length=%d", r.Offset, r.Length)
	}
	if r.End() < r.Offset {
		return fmt.Errorf("asyncio: range overflow")
	}
	return nil
}

type RangePart struct {
	InputIndex int
	Offset     int64
	Length     int64
}

type CoalescedRange struct {
	Offset int64
	Length int64
	Parts  []RangePart
}

type RangePlanner struct {
	MaxGap          int64
	MaxMergedLength int64
}

func (p RangePlanner) Plan(input []ByteRange) ([]CoalescedRange, error) {
	if len(input) == 0 {
		return nil, nil
	}
	type indexed struct {
		ByteRange
		index int
	}
	ordered := make([]indexed, len(input))
	for i, r := range input {
		if err := r.validate(); err != nil {
			return nil, err
		}
		ordered[i] = indexed{ByteRange: r, index: i}
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Offset == ordered[j].Offset {
			return ordered[i].Length < ordered[j].Length
		}
		return ordered[i].Offset < ordered[j].Offset
	})
	maxGap := p.MaxGap
	if maxGap < 0 {
		maxGap = 0
	}
	maxLen := p.MaxMergedLength
	if maxLen <= 0 {
		maxLen = 8 << 20
	}

	out := make([]CoalescedRange, 0, len(ordered))
	for _, r := range ordered {
		if len(out) == 0 {
			out = append(out, newCoalesced(r.index, r.ByteRange))
			continue
		}
		last := &out[len(out)-1]
		lastEnd := last.Offset + last.Length
		gap := r.Offset - lastEnd
		if gap < 0 {
			gap = 0
		}
		newEnd := max64(lastEnd, r.End())
		newLen := newEnd - last.Offset
		if gap <= maxGap && newLen <= maxLen {
			last.Parts = append(last.Parts, RangePart{InputIndex: r.index, Offset: r.Offset - last.Offset, Length: r.Length})
			last.Length = newLen
			continue
		}
		out = append(out, newCoalesced(r.index, r.ByteRange))
	}
	return out, nil
}

func newCoalesced(index int, r ByteRange) CoalescedRange {
	return CoalescedRange{Offset: r.Offset, Length: r.Length, Parts: []RangePart{{InputIndex: index, Offset: 0, Length: r.Length}}}
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

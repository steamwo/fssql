package csv

import (
	"strings"
	"testing"
)

func summarizeParts(t *testing.T, parts ...string) ([]ChunkSummary, []BoundaryState, BoundaryState) {
	t.Helper()
	summaries := make([]ChunkSummary, len(parts))
	for i := range parts {
		s, err := SummarizeChunk([]byte(parts[i]), DefaultLexConfig())
		if err != nil {
			t.Fatal(err)
		}
		summaries[i] = s
	}
	starts, end, err := ResolveChunkStarts(summaries)
	if err != nil {
		t.Fatal(err)
	}
	return summaries, starts, end
}

func TestChunkSummaryCarriesQuotedStateAcrossChunks(t *testing.T) {
	_, starts, end := summarizeParts(t, "a,\"quoted", "\nvalue\",x\n", "b,c\n")
	if starts[1].Lex != StateQuoted {
		t.Fatalf("chunk 1 start=%+v, want quoted", starts[1])
	}
	if starts[2].Lex != StateFieldStart {
		t.Fatalf("chunk 2 start=%+v", starts[2])
	}
	if err := ValidateFinalBoundaryState(DefaultLexConfig(), end); err != nil {
		t.Fatal(err)
	}
}

func TestChunkSummaryCarriesEscapedQuoteAcrossChunks(t *testing.T) {
	_, starts, end := summarizeParts(t, "\"a\"", "\"b\",x\n")
	if starts[1].Lex != StateAfterQuote {
		t.Fatalf("chunk 1 start=%+v, want after-quote", starts[1])
	}
	if err := ValidateFinalBoundaryState(DefaultLexConfig(), end); err != nil {
		t.Fatal(err)
	}
}

func TestChunkSummaryCarriesCRLFAcrossChunks(t *testing.T) {
	_, starts, end := summarizeParts(t, "a,b\r", "\nc,d\r\n")
	if !starts[1].SkipLF {
		t.Fatalf("chunk 1 start=%+v, want SkipLF", starts[1])
	}
	if err := ValidateFinalBoundaryState(DefaultLexConfig(), end); err != nil {
		t.Fatal(err)
	}
}

func TestFirstSafeRecordBoundaryDoesNotSplitQuotedNewline(t *testing.T) {
	start := BoundaryState{Lex: StateQuoted}
	offset, ok, _, err := FirstSafeRecordBoundary([]byte("inside\nquote\",x\nnext"), DefaultLexConfig(), start)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected safe boundary")
	}
	want := strings.Index("inside\nquote\",x\nnext", "\nnext") + 1
	if offset != want {
		t.Fatalf("offset=%d want=%d", offset, want)
	}
}

func TestFirstSafeRecordBoundaryRejectsTrailingCRCandidate(t *testing.T) {
	_, ok, end, err := FirstSafeRecordBoundary([]byte("a,b\r"), DefaultLexConfig(), BoundaryState{Lex: StateFieldStart})
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("trailing CR must not be used as independent split")
	}
	if !end.SkipLF {
		t.Fatalf("end=%+v, want SkipLF", end)
	}
}

func TestPlanSafeSplitOffsetsProducesIndependentlyDecodablePartitions(t *testing.T) {
	input := []byte("1,a\n2,\"quoted\nvalue\"\n3,c\r\n4,\"d\"\"e\"\n5,f\n")
	const coarse = 7
	var candidates []SplitCandidate
	for base := 0; base < len(input); base += coarse {
		end := base + coarse
		if end > len(input) {
			end = len(input)
		}
		candidates = append(candidates, SplitCandidate{Base: int64(base), Data: input[base:end]})
	}
	offsets, err := PlanSafeSplitOffsets(candidates, DefaultLexConfig())
	if err != nil {
		t.Fatal(err)
	}
	if len(offsets) == 0 {
		t.Fatal("expected at least one safe split")
	}
	prev := 0
	for _, off64 := range append(offsets, int64(len(input))) {
		off := int(off64)
		part := input[prev:off]
		m, err := NewMachine(DefaultLexConfig())
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range part {
			if _, err := m.Step(b); err != nil {
				t.Fatalf("partition %d:%d invalid: %v", prev, off, err)
			}
		}
		if err := m.Finish(); err != nil {
			t.Fatalf("partition %d:%d ends mid-record: %v", prev, off, err)
		}
		prev = off
	}
}

func TestPlanSafeSplitOffsetsMalformedCSVFails(t *testing.T) {
	candidates := []SplitCandidate{{Base: 0, Data: []byte("a,\"bad")}, {Base: 6, Data: []byte("\n")}}
	if _, err := PlanSafeSplitOffsets(candidates, DefaultLexConfig()); err == nil {
		t.Fatal("expected malformed CSV error")
	}
}

func BenchmarkSummarizeChunk(b *testing.B) {
	line := "123,alpha,\"quoted,value\",true\n"
	input := []byte(strings.Repeat(line, 2048))
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := SummarizeChunk(input, DefaultLexConfig()); err != nil {
			b.Fatal(err)
		}
	}
}

package csv

import "testing"

func mustMachine(t *testing.T, cfg LexConfig) Machine {
	t.Helper()
	m, err := NewMachine(cfg)
	if err != nil {
		t.Fatalf("NewMachine() error = %v", err)
	}
	return m
}

func collectRecordEnds(t *testing.T, input string) []int {
	t.Helper()
	m := mustMachine(t, DefaultLexConfig())
	var ends []int
	for i := 0; i < len(input); i++ {
		event, err := m.Step(input[i])
		if err != nil {
			t.Fatalf("Step(%d) error = %v", i, err)
		}
		if event.Has(EventRecordEnd) {
			ends = append(ends, i)
		}
	}
	if err := m.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
	return ends
}

func TestMachineRecordBoundaries(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []int
	}{
		{name: "LF", input: "a,b\nc,d\n", want: []int{3, 7}},
		{name: "CRLF normalized", input: "a,b\r\nc,d\r\n", want: []int{3, 8}},
		{name: "bare CR", input: "a,b\rc,d", want: []int{3}},
		{name: "quoted newline", input: "a,\"b\nc\",d\ne,f", want: []int{9}},
		{name: "quoted CRLF", input: "a,\"b\r\nc\",d\r\ne,f", want: []int{10}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := collectRecordEnds(t, tt.input)
			if len(got) != len(tt.want) {
				t.Fatalf("record ends = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("record ends = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestMachineEscapedQuote(t *testing.T) {
	m := mustMachine(t, DefaultLexConfig())
	input := "\"a\"\"b\"\n"
	var dataEvents int
	var records int
	for i := 0; i < len(input); i++ {
		event, err := m.Step(input[i])
		if err != nil {
			t.Fatalf("Step(%d) error = %v", i, err)
		}
		if event.Has(EventData) {
			dataEvents++
		}
		if event.Has(EventRecordEnd) {
			records++
		}
	}
	if err := m.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
	if dataEvents != 3 {
		t.Fatalf("data events = %d, want 3", dataEvents)
	}
	if records != 1 {
		t.Fatalf("record events = %d, want 1", records)
	}
}

func TestMachineCustomDelimiter(t *testing.T) {
	m := mustMachine(t, LexConfig{Delimiter: ';', Quote: '"'})
	input := "a;\"b;c\"\n"
	fields := 0
	for i := 0; i < len(input); i++ {
		event, err := m.Step(input[i])
		if err != nil {
			t.Fatalf("Step(%d) error = %v", i, err)
		}
		if event.Has(EventFieldEnd) {
			fields++
		}
	}
	if fields != 2 {
		t.Fatalf("field ends = %d, want 2", fields)
	}
}

func TestMachineRejectsMalformedInput(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "quote in unquoted field", input: "ab\"c"},
		{name: "byte after closing quote", input: "\"ab\"x"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := mustMachine(t, DefaultLexConfig())
			var gotErr error
			for i := 0; i < len(tt.input); i++ {
				if _, err := m.Step(tt.input[i]); err != nil {
					gotErr = err
					break
				}
			}
			if gotErr == nil {
				t.Fatal("expected syntax error")
			}
		})
	}
}

func TestMachineRejectsUnterminatedQuotedField(t *testing.T) {
	m := mustMachine(t, DefaultLexConfig())
	for _, b := range []byte("a,\"unterminated") {
		if _, err := m.Step(b); err != nil {
			t.Fatalf("Step() error = %v", err)
		}
	}
	if err := m.Finish(); err == nil {
		t.Fatal("Finish() expected unterminated quote error")
	}
}

func TestLexConfigValidation(t *testing.T) {
	bad := []LexConfig{
		{},
		{Delimiter: ',', Quote: ','},
		{Delimiter: '\n', Quote: '"'},
		{Delimiter: ',', Quote: '\r'},
	}
	for _, cfg := range bad {
		if _, err := NewMachine(cfg); err == nil {
			t.Fatalf("NewMachine(%+v) expected error", cfg)
		}
	}
}

func BenchmarkMachineStepUnquoted(b *testing.B) {
	input := []byte("alpha,beta,gamma,delta\n")
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m, _ := NewMachine(DefaultLexConfig())
		for _, ch := range input {
			_, _ = m.Step(ch)
		}
	}
}

func BenchmarkMachineStepQuoted(b *testing.B) {
	input := []byte("alpha,\"beta,with\nnewline\",\"escaped \"\" quote\"\n")
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m, _ := NewMachine(DefaultLexConfig())
		for _, ch := range input {
			_, _ = m.Step(ch)
		}
	}
}

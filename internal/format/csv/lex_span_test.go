package csv

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
)

func checkDataSpans(t *testing.T, input []byte, cfg LexConfig) {
	t.Helper()
	fast, err := NewMachine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	scalar := fast
	for i := 0; i < len(input); {
		n := fast.advanceData(input[i:])
		if n > 0 {
			for j := 0; j < n; j++ {
				event, err := scalar.Step(input[i+j])
				if err != nil || event != EventData {
					t.Fatalf("input=%q offset=%d span=%d event=%v err=%v", input, i+j, n, event, err)
				}
			}
			i += n
		} else {
			got, ge := fast.Step(input[i])
			want, we := scalar.Step(input[i])
			if (ge == nil) != (we == nil) || got != want {
				t.Fatalf("input=%q offset=%d events=%v/%v errors=%v/%v", input, i, got, want, ge, we)
			}
			if ge != nil {
				return
			}
			i++
		}
		if fast.state != scalar.state || fast.skipLF != scalar.skipLF {
			t.Fatalf("state mismatch at offset=%d input=%q", i, input)
		}
	}
	if (fast.Finish() == nil) != (scalar.Finish() == nil) {
		t.Fatalf("EOF mismatch input=%q", input)
	}
}

func TestCSVDataSpansMatchByteMachine(t *testing.T) {
	for _, cfg := range []LexConfig{DefaultLexConfig(), {Delimiter: '|', Quote: '\''}, {Delimiter: 0x81, Quote: 0x82}} {
		for _, input := range []string{"", "\r\n\r\n", "plain,long-unquoted-payload\r\n", "\"long, quoted\nvalue with \"\"escapes\"\"\",end\r\n", "\"unfinished", "bad\"quote", "\"x\"bad", strings.Repeat("abcdefgh", 40)} {
			checkDataSpans(t, []byte(input), cfg)
		}
		rng := rand.New(rand.NewSource(19))
		alphabet := []byte{'a', 'b', 0, 0x80, 0xff, cfg.Delimiter, cfg.Quote, '\r', '\n'}
		for i := 0; i < 500; i++ {
			input := make([]byte, rng.Intn(256))
			for j := range input {
				input[j] = alphabet[rng.Intn(len(alphabet))]
			}
			checkDataSpans(t, input, cfg)
		}
	}
}

func TestCSVSpanDecodeProjectionQuotesAndCRLF(t *testing.T) {
	m, scope, bm := testMemory(1 << 20)
	input := []byte("1|abcdefghijklmnopqrstuvwxyz|\"quoted\nwith \"\"escape\"\"\"\r\n2|unused|\"tail\"")
	chunk, consumed, err := DecodeChunk(input, []types.LogicalType{{ID: types.BigInt}, {ID: types.Varchar}, {ID: types.Varchar}}, DecodeOptions{Lex: LexConfig{Delimiter: '|', Quote: '"'}, Final: true, Projection: []int{0, 2}}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	if consumed != len(input) || chunk.Cardinality != 2 {
		t.Fatalf("consumed=%d rows=%d", consumed, chunk.Cardinality)
	}
	if got, valid := chunk.Columns[2].String(0); !valid || got != "quoted\nwith \"escape\"" {
		t.Fatalf("string=%q valid=%v", got, valid)
	}
	if chunk.Columns[1].IsValid(0) {
		t.Fatal("unread column materialized")
	}
	chunk.Release()
	if m.Used() != 0 {
		t.Fatalf("managed leak=%d", m.Used())
	}
}

func FuzzCSVDataSpans(f *testing.F) {
	f.Add([]byte("a,\"quoted\nvalue\"\r\n"))
	f.Add([]byte("\"a\"\"b\",x\n"))
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 1<<16 {
			t.Skip()
		}
		checkDataSpans(t, input, DefaultLexConfig())
	})
}

func BenchmarkCSVDataSpans(b *testing.B) {
	input := []byte(strings.Repeat("123456789,plain-long-unquoted-payload,\"long quoted, payload\"\r\n", 2048))
	for _, spans := range []bool{false, true} {
		name := "byte"
		if spans {
			name = "span"
		}
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(input)))
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				m, _ := NewMachine(DefaultLexConfig())
				for i := 0; i < len(input); i++ {
					if spans {
						if n := m.advanceData(input[i:]); n > 0 {
							i += n - 1
							continue
						}
					}
					if _, err := m.Step(input[i]); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

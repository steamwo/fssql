package csv

import (
	"errors"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

func testMemory(limit int64) (*memory.Manager, *memory.Scope, *memory.BufferManager) {
	mgr := memory.NewManager(limit)
	return mgr, mgr.NewScope(), memory.NewBufferManager(nil)
}

func TestDecodeChunkTypedVectors(t *testing.T) {
	mgr, scope, bm := testMemory(1 << 20)
	schema := []types.LogicalType{{ID: types.BigInt}, {ID: types.Double}, {ID: types.Boolean}, {ID: types.Varchar}}
	input := []byte("1,1.5,true,plain\n2,2.25,false,\"quoted, value\"\n3,3.5,true,\"line\nwith \"\"quote\"\"\"\n")
	chunk, consumed, err := DecodeChunk(input, schema, DecodeOptions{Final: true}, scope, bm)
	if err != nil {
		t.Fatalf("DecodeChunk() error = %v", err)
	}
	defer chunk.Release()
	if consumed != len(input) || chunk.Cardinality != 3 {
		t.Fatalf("consumed/cardinality = %d/%d, want %d/3", consumed, chunk.Cardinality, len(input))
	}
	if got, _ := chunk.Columns[0].Int64(1); got != 2 {
		t.Fatalf("bigint row2=%d", got)
	}
	if got, _ := chunk.Columns[1].Float64(2); got != 3.5 {
		t.Fatalf("double row3=%v", got)
	}
	if got, _ := chunk.Columns[2].Bool(1); got {
		t.Fatalf("bool row2=%v", got)
	}
	if got, _ := chunk.Columns[3].String(1); got != "quoted, value" {
		t.Fatalf("string row2=%q", got)
	}
	if got, _ := chunk.Columns[3].String(2); got != "line\nwith \"quote\"" {
		t.Fatalf("string row3=%q", got)
	}
	chunk.Release()
	if got := scope.Used(); got != 0 || mgr.Used() != 0 {
		t.Fatalf("managed bytes after release scope=%d manager=%d", got, mgr.Used())
	}
}

func TestDecodeChunkNullLiteral(t *testing.T) {
	_, scope, bm := testMemory(1 << 20)
	chunk, _, err := DecodeChunk([]byte("1,NULL\n2,text\n"), []types.LogicalType{{ID: types.BigInt}, {ID: types.Varchar}}, DecodeOptions{Final: true, NullLiteral: []byte("NULL")}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	defer chunk.Release()
	if chunk.Columns[1].IsValid(0) {
		t.Fatal("NULL literal should set validity false")
	}
	if got, ok := chunk.Columns[1].String(1); !ok || got != "text" {
		t.Fatalf("row2=%q ok=%v", got, ok)
	}
}

func TestDecodeChunkLeavesPartialRecordUnconsumed(t *testing.T) {
	_, scope, bm := testMemory(1 << 20)
	input := []byte("1,a\n2,partial")
	chunk, consumed, err := DecodeChunk(input, []types.LogicalType{{ID: types.BigInt}, {ID: types.Varchar}}, DecodeOptions{Final: false}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	defer chunk.Release()
	if chunk.Cardinality != 1 || consumed != len("1,a\n") {
		t.Fatalf("cardinality/consumed=%d/%d", chunk.Cardinality, consumed)
	}
}

func TestDecodeChunkNoTerminalNewline(t *testing.T) {
	_, scope, bm := testMemory(1 << 20)
	chunk, consumed, err := DecodeChunk([]byte("7,last"), []types.LogicalType{{ID: types.BigInt}, {ID: types.Varchar}}, DecodeOptions{Final: true}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	defer chunk.Release()
	if consumed != len("7,last") || chunk.Cardinality != 1 {
		t.Fatalf("consumed/cardinality=%d/%d", consumed, chunk.Cardinality)
	}
}

func TestDecodeChunkRejectsWidthMismatchAndMalformedCSV(t *testing.T) {
	for _, input := range []string{"1,2,3\n", "1\n", "1,\"unterminated"} {
		t.Run(input, func(t *testing.T) {
			_, scope, bm := testMemory(1 << 20)
			chunk, _, err := DecodeChunk([]byte(input), []types.LogicalType{{ID: types.BigInt}, {ID: types.Varchar}}, DecodeOptions{Final: true}, scope, bm)
			if chunk != nil {
				chunk.Release()
			}
			if err == nil {
				t.Fatal("expected decode error")
			}
			if got := scope.Used(); got != 0 {
				t.Fatalf("managed leak after error: %d", got)
			}
		})
	}
}

func TestDecodeChunkAllocationFailureHasNoLeak(t *testing.T) {
	mgr, scope, bm := testMemory(8)
	chunk, _, err := DecodeChunk([]byte("1,very-long-string\n"), []types.LogicalType{{ID: types.BigInt}, {ID: types.Varchar}}, DecodeOptions{Final: true}, scope, bm)
	if chunk != nil {
		chunk.Release()
	}
	if !errors.Is(err, memory.ErrLimitExceeded) {
		t.Fatalf("error=%v, want memory limit", err)
	}
	if scope.Used() != 0 || mgr.Used() != 0 {
		t.Fatalf("managed leak scope=%d manager=%d", scope.Used(), mgr.Used())
	}
}

func TestDecodeChunkCapsAtVectorSize(t *testing.T) {
	var b strings.Builder
	for i := 0; i < vector.DefaultVectorSize+2; i++ {
		b.WriteString("1,x\n")
	}
	_, scope, bm := testMemory(1 << 24)
	input := []byte(b.String())
	chunk, consumed, err := DecodeChunk(input, []types.LogicalType{{ID: types.BigInt}, {ID: types.Varchar}}, DecodeOptions{Final: true}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	defer chunk.Release()
	if chunk.Cardinality != vector.DefaultVectorSize {
		t.Fatalf("cardinality=%d", chunk.Cardinality)
	}
	if consumed >= len(input) {
		t.Fatalf("expected remaining input, consumed=%d len=%d", consumed, len(input))
	}
}

func BenchmarkDecodeChunk(b *testing.B) {
	var sb strings.Builder
	for i := 0; i < vector.DefaultVectorSize; i++ {
		sb.WriteString("123,1.25,true,hello-world\n")
	}
	input := []byte(sb.String())
	schema := []types.LogicalType{{ID: types.BigInt}, {ID: types.Double}, {ID: types.Boolean}, {ID: types.Varchar}}
	b.SetBytes(int64(len(input)))
	for i := 0; i < b.N; i++ {
		mgr, scope, bm := testMemory(1 << 24)
		chunk, _, err := DecodeChunk(input, schema, DecodeOptions{Final: true}, scope, bm)
		if err != nil {
			b.Fatal(err)
		}
		chunk.Release()
		if mgr.Used() != 0 {
			b.Fatalf("leak=%d", mgr.Used())
		}
	}
}

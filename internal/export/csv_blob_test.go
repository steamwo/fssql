package export

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

func TestCSVBlobQuoting(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts CSVWriteOptions
		want string
	}{
		{"delimiter", CSVWriteOptions{Delimiter: 'F'}, "\"00FF41\"\n"},
		{"quote", CSVWriteOptions{Quote: 'F'}, "F00FFFF41F\n"},
		{"null literal", CSVWriteOptions{NullString: "00FF41"}, "\"00FF41\"\n"},
		{"default", CSVWriteOptions{}, "00FF41\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := memory.NewManager(1 << 20)
			scope := manager.NewScope()
			buffers := memory.NewBufferManager(nil)
			v := newVector(t, scope, buffers, types.LogicalType{ID: types.Varchar, Binary: true}, 1)
			if err := v.SetBytes(0, []byte{0, 255, 65}); err != nil {
				t.Fatal(err)
			}
			chunk, err := vector.NewDataChunk(v)
			if err != nil {
				t.Fatal(err)
			}
			sink := &captureSink{}
			writer, err := NewCSVWriter([]Column{{Name: "blob", Type: v.Type}}, tc.opts, scope, buffers, sink)
			if err != nil {
				t.Fatal(err)
			}
			if err := writer.WriteChunk(context.Background(), chunk); err != nil {
				t.Fatal(err)
			}
			if _, err := writer.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			chunk.Release()
			if got := sink.buf.String(); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
			if scope.Used() != 0 {
				t.Fatalf("leaked %d bytes", scope.Used())
			}
		})
	}
}

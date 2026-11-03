package gosql

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestPhase4ScalarUDFErrorAbortsQuery(t *testing.T) {
	boom := errors.New("phase4 udf boom")
	fn := ScalarFunction{
		Name:          "phase4_fail",
		Arguments:     []Type{{ID: "VARCHAR"}},
		Result:        Type{ID: "VARCHAR"},
		NullHandling:  ScalarNullCall,
		Deterministic: true,
		Eval: func([]ScalarValue) (ScalarValue, error) { return ScalarValue{}, boom },
	}
	db, err := Open(Config{ScalarFunctions: []ScalarFunction{fn}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil { t.Fatal(err) }
	defer db.Close()
	_, err = db.QueryChunks(context.Background(), "SELECT phase4_fail('x')", func(*Chunk) error { return nil })
	if err == nil || !strings.Contains(err.Error(), boom.Error()) { t.Fatalf("query error=%v, want UDF failure", err) }
	if used := db.MemoryUsed(); used != 0 { t.Fatalf("managed memory leaked after UDF failure: %d", used) }
}

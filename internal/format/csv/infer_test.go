package csv

import (
	"errors"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/vector"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
)

func TestInferTemporalSchemaAndDecode(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []string
		want   types.LogicalType
	}{
		{"date", []string{"2026-01-02", "2024-02-29"}, types.LogicalType{ID: types.DateType}},
		{"timestamp", []string{"2026-01-02 03:04:05.123456", "2026-01-03T04:05:06"}, types.LogicalType{ID: types.TimestampType}},
		{"date_timestamp", []string{"2026-01-02", "2026-01-03 04:05:06"}, types.LogicalType{ID: types.TimestampType}},
		{"timestamp_date", []string{"2026-01-03 04:05:06", "2026-01-02"}, types.LogicalType{ID: types.TimestampType}},
		{"timezone", []string{"2026-01-02T03:04:05+08:00", "2026-01-03T04:05:06Z"}, types.TimestampTZType()},
		{"invalid_date", []string{"2026-01-02", "2026-02-30"}, types.LogicalType{ID: types.Varchar}},
		{"mixed_text", []string{"2026-01-02", "hello"}, types.LogicalType{ID: types.Varchar}},
		{"decimal_default", []string{"12.34", "0.01"}, types.LogicalType{ID: types.Double}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr, scope, bm := testMemory(1 << 20)
			input := []byte(tc.values[0] + "\nNULL\n" + tc.values[1] + "\n")
			got, err := InferSchema(input, InferenceOptions{Final: true, NullLiteral: []byte("NULL")}, scope, bm)
			if err != nil {
				t.Fatal(err)
			}
			if !got.Fields[0].Type.Same(tc.want) {
				t.Fatalf("type=%+v want=%+v", got.Fields[0].Type, tc.want)
			}
			v, err := vector.NewFlat(scope, bm, tc.want, 2)
			if err != nil {
				t.Fatal(err)
			}
			for row, raw := range tc.values {
				if err := decodeField(v, tc.want, row, []byte(raw), []byte("NULL")); err != nil {
					t.Fatal(err)
				}
			}
			if tc.name == "timezone" {
				x, _ := v.Int64(0)
				want := time.Date(2026, 1, 1, 19, 4, 5, 0, time.UTC).UnixMicro()
				if x != want {
					t.Fatalf("timezone=%d want=%d", x, want)
				}
			}
			v.Release()
			if scope.Used() != 0 || mgr.Used() != 0 {
				t.Fatalf("memory leak: %d/%d", scope.Used(), mgr.Used())
			}
		})
	}
}

func TestInferSchemaHeaderAndPromotion(t *testing.T) {
	mgr, scope, bm := testMemory(1 << 20)
	input := []byte("id,score,active,note\n1,1,true,alpha\n2,2.5,false,beta\n3,NULL,true,\"quoted,note\"\n")
	got, err := InferSchema(input, InferenceOptions{Header: true, Final: true, NullLiteral: []byte("NULL")}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	if got.SampleRows != 3 {
		t.Fatalf("sample rows=%d", got.SampleRows)
	}
	wantIDs := []types.LogicalTypeID{types.BigInt, types.Double, types.Boolean, types.Varchar}
	wantNames := []string{"id", "score", "active", "note"}
	for i := range wantIDs {
		if got.Fields[i].Name != wantNames[i] || got.Fields[i].Type.ID != wantIDs[i] {
			t.Fatalf("field %d=%+v", i, got.Fields[i])
		}
	}
	if scope.Used() != 0 || mgr.Used() != 0 {
		t.Fatalf("managed leak scope=%d manager=%d", scope.Used(), mgr.Used())
	}
}

func TestInferSchemaNoHeaderNamesAndNullOnlyVarchar(t *testing.T) {
	_, scope, bm := testMemory(1 << 20)
	got, err := InferSchema([]byte("1,NULL\n2,NULL\n"), InferenceOptions{Final: true, NullLiteral: []byte("NULL")}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	if got.Fields[0].Name != "column0" || got.Fields[1].Name != "column1" {
		t.Fatalf("names=%+v", got.Fields)
	}
	if got.Fields[0].Type.ID != types.BigInt || got.Fields[1].Type.ID != types.Varchar {
		t.Fatalf("types=%+v", got.Fields)
	}
}

func TestInferSchemaNormalizesHeaderNames(t *testing.T) {
	_, scope, bm := testMemory(1 << 20)
	got, err := InferSchema([]byte("name,name,,name_1\na,b,c,d\n"), InferenceOptions{Header: true, Final: true}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"name", "name_1", "column2", "name_1_1"}
	for i := range want {
		if got.Fields[i].Name != want[i] {
			t.Fatalf("name %d=%q want %q", i, got.Fields[i].Name, want[i])
		}
	}
}

func TestInferSchemaBoundedRows(t *testing.T) {
	_, scope, bm := testMemory(1 << 20)
	got, err := InferSchema([]byte("1\n2\ntext\n"), InferenceOptions{Final: true, MaxRows: 2}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	if got.SampleRows != 2 || got.Fields[0].Type.ID != types.BigInt {
		t.Fatalf("result=%+v", got)
	}
}

func TestInferSchemaIgnoresIncompleteTailWhenNotFinal(t *testing.T) {
	_, scope, bm := testMemory(1 << 20)
	got, err := InferSchema([]byte("1,a\n2,partial"), InferenceOptions{Final: false}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	if got.SampleRows != 1 || len(got.Fields) != 2 {
		t.Fatalf("result=%+v", got)
	}
}

func TestInferSchemaRejectsWidthMismatch(t *testing.T) {
	_, scope, bm := testMemory(1 << 20)
	_, err := InferSchema([]byte("1,a\n2\n"), InferenceOptions{Final: true}, scope, bm)
	if err == nil {
		t.Fatal("expected width mismatch")
	}
	if scope.Used() != 0 {
		t.Fatalf("managed leak=%d", scope.Used())
	}
}

func TestInferSchemaAllocationFailureHasNoLeak(t *testing.T) {
	mgr, scope, bm := testMemory(0)
	_, err := InferSchema([]byte("1,a\n"), InferenceOptions{Final: true}, scope, bm)
	if !errors.Is(err, memory.ErrLimitExceeded) {
		t.Fatalf("error=%v", err)
	}
	if scope.Used() != 0 || mgr.Used() != 0 {
		t.Fatalf("managed leak scope=%d manager=%d", scope.Used(), mgr.Used())
	}
}

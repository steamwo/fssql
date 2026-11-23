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

// TestInferSchemaQuotedNullSemantics 验证推断保留引号状态并与执行侧 NULL 配置一致。
func TestInferSchemaQuotedNullSemantics(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		nullLiteral []byte
		want        types.LogicalTypeID
	}{
		{name: "quoted_empty_number_crlf_eof", input: "1\r\n\"\"", nullLiteral: []byte(noQuotedNullPrefix), want: types.Varchar},
		{name: "quoted_empty_date", input: "2026-01-02\n\"\"\n", nullLiteral: []byte(noQuotedNullPrefix), want: types.Varchar},
		{name: "quoted_empty_text", input: "text\n\"\"\n", nullLiteral: []byte(noQuotedNullPrefix), want: types.Varchar},
		{name: "unquoted_empty_number", input: "1\n\n", nullLiteral: []byte(noQuotedNullPrefix), want: types.BigInt},
		{name: "null_literal", input: "1\nNULL\n", nullLiteral: []byte("NULL"), want: types.BigInt},
		{name: "quoted_null_literal_default", input: "1\n\"NULL\"\n", nullLiteral: []byte("NULL"), want: types.BigInt},
		{name: "quoted_number", input: "1\n\"2\"\n", nullLiteral: []byte(noQuotedNullPrefix), want: types.BigInt},
		{name: "quoted_empty_default", input: "1\n\"\"\n", nullLiteral: []byte(yesQuotedNullPrefix), want: types.BigInt},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, scope, bm := testMemory(1 << 20)
			got, err := InferSchema([]byte(tc.input), InferenceOptions{Final: true, NullLiteral: tc.nullLiteral}, scope, bm)
			if err != nil {
				t.Fatal(err)
			}
			if got.SampleRows != 2 || len(got.Fields) != 1 || got.Fields[0].Type.ID != tc.want {
				t.Fatalf("schema=%+v rows=%d want type=%d rows=2", got.Fields, got.SampleRows, tc.want)
			}
		})
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
	for _, tc := range []struct {
		name     string
		input    string
		options  InferenceOptions
		wantRows int
		wantType types.LogicalTypeID
	}{
		{name: "default types", input: "1,a\ntext,partial", wantRows: 1, wantType: types.BigInt},
		{
			name: "explicit candidates", input: "id,note\n42,a\n40000,partial",
			options:  InferenceOptions{Header: true, Candidates: []types.LogicalType{{ID: types.SmallInt}, {ID: types.Integer}}},
			wantRows: 1, wantType: types.SmallInt,
		},
		{name: "header only", input: "id,note\n42,partial", options: InferenceOptions{Header: true}, wantType: types.Varchar},
		{name: "final row at EOF", input: "1,a\ntext,last", options: InferenceOptions{Final: true}, wantRows: 2, wantType: types.Varchar},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, scope, bm := testMemory(1 << 20)
			got, err := InferSchema([]byte(tc.input), tc.options, scope, bm)
			if err != nil {
				t.Fatal(err)
			}
			if got.SampleRows != tc.wantRows || len(got.Fields) != 2 || got.Fields[0].Type.ID != tc.wantType {
				t.Fatalf("schema=%+v, want %d rows, 2 columns, first column type %v", got, tc.wantRows, tc.wantType)
			}
		})
	}
}

func TestInferSchemaRequiresCompleteFirstRecord(t *testing.T) {
	_, scope, bm := testMemory(1 << 20)
	for _, header := range []bool{false, true} {
		if got, err := InferSchema([]byte("1,partial"), InferenceOptions{Header: header}, scope, bm); err == nil {
			t.Fatalf("header=%t: inferred %v from an incomplete first record", header, got)
		}
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

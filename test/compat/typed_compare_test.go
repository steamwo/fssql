package compat_test

import (
	"math"
	"testing"
)

func TestTypedComparisonRejectsHiddenMismatches(t *testing.T) {
	s := func(value string) *string { return &value }
	base := func() typedResult {
		return typedResult{Names: []string{"v"}, Types: []string{"VARCHAR"}, Rows: [][]*string{{nil}, {s("NULL")}, {s("a")}, {s("a")}}}
	}
	for _, name := range []string{"name", "type", "null", "duplicates", "boundary", "order", "error"} {
		t.Run(name, func(t *testing.T) {
			a, b := base(), base()
			ordered := false
			switch name {
			case "name":
				b.Names[0] = "other"
			case "type":
				b.Types[0] = "BLOB"
			case "null":
				b.Rows[0][0] = s("NULL")
			case "duplicates":
				b.Rows = b.Rows[:3]
			case "boundary":
				b.Rows = [][]*string{{nil, s("NULL")}, {s("a"), s("a")}}
			case "order":
				b.Rows[0], b.Rows[1] = b.Rows[1], b.Rows[0]
				ordered = true
			case "error":
				b.Error = "failed"
			}
			if compareTyped(a, b, ordered, "") == nil {
				t.Fatal("mismatch accepted")
			}
		})
	}
	a, b := base(), base()
	b.Rows[0], b.Rows[1] = b.Rows[1], b.Rows[0]
	if err := compareTyped(a, b, false, ""); err != nil {
		t.Fatal(err)
	}
	for _, category := range []string{"unknown", "binder", "overflow"} {
		a := typedResult{Error: "failed", ErrorCategory: "conversion"}
		b := typedResult{Error: "failed", ErrorCategory: category}
		if compareTyped(a, b, false, "conversion") == nil {
			t.Fatal("different error categories accepted")
		}
	}
	a = typedResult{Error: "failed", ErrorCategory: "conversion"}
	if err := compareTyped(a, a, false, "conversion"); err != nil {
		t.Fatal(err)
	}
	a.ErrorCategory = "unknown"
	if compareTyped(a, a, false, "unknown") == nil {
		t.Fatal("unknown errors accepted")
	}
	for _, tc := range []struct {
		n    float64
		want string
	}{{math.NaN(), "nan"}, {math.Inf(1), "inf"}, {math.Inf(-1), "-inf"}, {math.Copysign(0, -1), "0"}} {
		got, err := typedCell(tc.n, "DOUBLE")
		if err != nil || got != tc.want {
			t.Fatalf("float=%q err=%v", got, err)
		}
	}
}

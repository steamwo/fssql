package function

import (
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
)

func TestBuiltinAggregateReturnTypes(t *testing.T) {
	c := NewBuiltinCatalog()
	cases := []struct {
		name string
		in   types.LogicalType
		want types.LogicalTypeID
	}{
		{"sum", types.LogicalType{ID: types.BigInt}, types.HugeInt},
		{"sum", types.LogicalType{ID: types.Double}, types.Double},
		{"avg", types.LogicalType{ID: types.Integer}, types.Double},
		{"min", types.LogicalType{ID: types.Varchar}, types.Varchar},
		{"max", types.LogicalType{ID: types.TimestampType}, types.TimestampType},
	}
	for _, tc := range cases {
		_, got, err := c.BindCall(tc.name, Aggregate, BindInput{Arguments: []ArgumentType{{Type: tc.in}}})
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got.ReturnType.ID != tc.want {
			t.Fatalf("%s got %v want %v", tc.name, got.ReturnType.ID, tc.want)
		}
	}
}

func TestSumDecimalWidening(t *testing.T) {
	c := NewBuiltinCatalog()
	d, _ := types.NewDecimal(12, 3)
	_, got, err := c.BindCall("SUM", Aggregate, BindInput{Arguments: []ArgumentType{{Type: d}}})
	if err != nil {
		t.Fatal(err)
	}
	if got.ReturnType.ID != types.Decimal || got.ReturnType.Decimal.Width != 38 || got.ReturnType.Decimal.Scale != 3 {
		t.Fatalf("got %+v", got.ReturnType)
	}
}

func TestCountStarAndSQLNull(t *testing.T) {
	c := NewBuiltinCatalog()
	_, got, err := c.BindCall("count", Aggregate, BindInput{Star: true})
	if err != nil || got.ReturnType.ID != types.BigInt {
		t.Fatalf("count*: %+v %v", got, err)
	}
	_, got, err = c.BindCall("count", Aggregate, BindInput{Arguments: []ArgumentType{{SQLNull: true}}})
	if err != nil || got.ReturnType.ID != types.BigInt {
		t.Fatalf("count null: %+v %v", got, err)
	}
	if _, _, err = c.BindCall("sum", Aggregate, BindInput{Arguments: []ArgumentType{{SQLNull: true}}}); err == nil {
		t.Fatal("expected SQLNULL inference error")
	}
}

// TestAggregateDistinctRejectedInStableV1 防止 COUNT(DISTINCT x) 被 Binder 丢失标志后静默按普通 aggregate 执行。
func TestAggregateDistinctRejectedInStableV1(t *testing.T) {
	c := NewBuiltinCatalog()
	if _, _, err := c.BindCall("count", Aggregate, BindInput{
		Arguments: []ArgumentType{{Type: types.LogicalType{ID: types.BigInt}}},
		Distinct:  true,
	}); err == nil {
		t.Fatal("expected aggregate DISTINCT rejection")
	}
}

func TestTableFunctionRegistrationDoesNotFakeBind(t *testing.T) {
	c := NewBuiltinCatalog()
	d, got, err := c.BindCall("read_parquet", Table, BindInput{Arguments: []ArgumentType{{Type: types.LogicalType{ID: types.Varchar}}}})
	if err != nil {
		t.Fatal(err)
	}
	if d.ID != ReadParquetID || d.Bind != nil || got.ReturnType.ID != types.Invalid {
		t.Fatalf("definition=%+v result=%+v", d, got)
	}
}

func TestCatalogRejectsDuplicateAndWrongKind(t *testing.T) {
	c := NewCatalog()
	d := Definition{ID: CountID, Name: "X", Kind: Aggregate, MinArgs: 1, MaxArgs: 1, Bind: bindCount}
	if err := c.Register(d); err != nil {
		t.Fatal(err)
	}
	if err := c.Register(d); err == nil {
		t.Fatal("expected duplicate error")
	}
	if _, _, err := c.BindCall("x", Scalar, BindInput{Arguments: []ArgumentType{{Type: types.LogicalType{ID: types.BigInt}}}}); err == nil {
		t.Fatal("expected kind error")
	}
}
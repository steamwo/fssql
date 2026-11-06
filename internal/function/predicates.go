package function

import (
	"fmt"
	"github.com/steamwo/fssql/internal/core/types"
)

const (
	InListID ID = 720 + iota
	BetweenID
	LikeID
	DistinctValueID
	MembershipMarkID
	ScalarCardinalityID
	NotDistinctID
)

func registerPredicates(c *Catalog) {
	for _, d := range []Definition{
		{ID: NotDistinctID, Name: "__fssql_not_distinct", Kind: Scalar, MinArgs: 2, MaxArgs: 2, Bind: bindPredicateComparable},
		{ID: InListID, Name: "__fssql_in", Kind: Scalar, MinArgs: 2, MaxArgs: -1, Bind: bindPredicateComparable},
		{ID: BetweenID, Name: "__fssql_between", Kind: Scalar, MinArgs: 3, MaxArgs: 3, Bind: bindPredicateComparable},
		{ID: LikeID, Name: "__fssql_like", Kind: Scalar, MinArgs: 2, MaxArgs: 3, Bind: func(in BindInput) (BindResult, error) {
			args := make([]types.LogicalType, len(in.Arguments))
			for i := range args {
				args[i] = types.LogicalType{ID: types.Varchar}
			}
			return BindResult{ReturnType: types.LogicalType{ID: types.Boolean}, Arguments: args}, nil
		}},
		{ID: DistinctValueID, Name: "__fssql_distinct_value", Kind: Scalar, MinArgs: 1, MaxArgs: -1},
		{ID: MembershipMarkID, Name: "__fssql_membership_mark", Kind: Scalar, MinArgs: 4, MaxArgs: 4},
		{ID: ScalarCardinalityID, Name: "__fssql_scalar_cardinality", Kind: Scalar, MinArgs: 2, MaxArgs: 2},
	} {
		if err := c.Register(d); err != nil {
			panic(err)
		}
	}
}

func bindPredicateComparable(in BindInput) (BindResult, error) {
	var t types.LogicalType
	for _, arg := range in.Arguments {
		if arg.SQLNull {
			continue
		}
		if t.ID == types.Invalid {
			t = arg.Type
			continue
		}
		if t.Same(arg.Type) {
			continue
		}
		if !t.IsNumeric() || !arg.Type.IsNumeric() {
			return BindResult{}, fmt.Errorf("predicate requires comparable argument types")
		}
		other := arg.Type
		if t.ID != types.Decimal && other.ID != types.Decimal && t.ID != types.Float && t.ID != types.Double && other.ID != types.Float && other.ID != types.Double && t.Unsigned != other.Unsigned {
			if t.ID == types.HugeInt || other.ID == types.HugeInt || (t.IsUnsignedInteger() && t.ID == types.BigInt) || (other.IsUnsignedInteger() && other.ID == types.BigInt) {
				t = types.LogicalType{ID: types.HugeInt}
			} else {
				t = types.LogicalType{ID: types.BigInt}
			}
		} else {
			var err error
			t, err = commonNumeric(t, other)
			if err != nil {
				return BindResult{}, err
			}
		}
	}
	if t.ID == types.Invalid {
		t = types.LogicalType{ID: types.Varchar}
	}
	args := make([]types.LogicalType, len(in.Arguments))
	for i := range args {
		args[i] = t
	}
	return BindResult{ReturnType: types.LogicalType{ID: types.Boolean}, Arguments: args}, nil
}

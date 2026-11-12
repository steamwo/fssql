package binder

import (
	"fmt"
	"strings"

	"github.com/steamwo/fssql/internal/core/types"
)

func alignUnionByName(left, right *BoundQuery) (*BoundQuery, *BoundQuery, error) {
	names := []RelationColumn{}
	indices := make([]map[string]int, 2)
	for side, q := range []*BoundQuery{left, right} {
		indices[side] = map[string]int{}
		for i, col := range q.Output {
			key := strings.ToLower(col.Name)
			if _, ok := indices[side][key]; ok {
				return nil, nil, fmt.Errorf("UNION BY NAME input has duplicate column %q", col.Name)
			}
			indices[side][key] = i
			if side == 0 {
				names = append(names, col)
			} else if _, ok := indices[0][key]; !ok {
				names = append(names, col)
			}
		}
	}
	wrap := func(q *BoundQuery, side int) *BoundQuery {
		// Give SQL NULL outputs a concrete physical type inside the completed
		// branch; retain their untyped-NULL status in the alignment projection.
		inner := *q
		inner.Output = cloneRelationColumns(q.Output)
		inner.Projection = append([]BoundSelectItem(nil), q.Projection...)
		for i, c := range inner.Output {
			if c.Type.ID == types.Invalid {
				inner.Output[i].Type = types.LogicalType{ID: types.Integer}
				inner.Projection[i] = BoundSelectItem{Expression: &BoundExpression{Kind: BoundNull, Type: inner.Output[i].Type, Nullable: true}, Output: inner.Output[i]}
			}
		}
		source := &BoundRelation{Kind: RelationCTE, ID: "set:byname", Columns: cloneRelationColumns(inner.Output), CTEQuery: &inner}
		out := &BoundQuery{Source: source}
		for _, name := range names {
			col := name
			expr := &BoundExpression{Kind: BoundNull, SQLNull: true, Nullable: true}
			col.Type = types.LogicalType{}
			col.Nullable = true
			if i, ok := indices[side][strings.ToLower(name.Name)]; ok {
				col.Type = q.Output[i].Type
				col.Nullable = q.Output[i].Nullable
				if col.Type.ID != types.Invalid {
					expr = &BoundExpression{Kind: BoundColumn, ColumnIndex: i, Type: col.Type, Nullable: col.Nullable}
				}
			}
			out.Output = append(out.Output, col)
			out.Projection = append(out.Projection, BoundSelectItem{Expression: expr, Output: col})
		}
		return out
	}
	return wrap(left, 0), wrap(right, 1), nil
}

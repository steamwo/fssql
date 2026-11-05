package expression

import (
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

// AND and OR dispatch before the loop; both children are still evaluated in
// their original order and SQL's three-valued logic is preserved.
func booleanBatch(kind Kind, left, right, out *vector.Vector, count int) bool {
	if (kind != And && kind != Or) || left.Type.ID != types.Boolean || right.Type.ID != types.Boolean || out.Type.ID != types.Boolean {
		return false
	}
	l, _ := left.BorrowFixedWidth()
	r, _ := right.BorrowFixedWidth()
	data, validity, _ := out.MutableFixedWidth()
	if kind == And {
		for row := 0; row < count; row++ {
			a, av := l.Value(row)
			b, bv := r.Value(row)
			if av && a[0] == 0 || bv && b[0] == 0 {
				data[row] = 0
			} else if !av || !bv {
				validity.SetValid(row, false)
			} else {
				data[row] = 1
			}
		}
	} else {
		for row := 0; row < count; row++ {
			a, av := l.Value(row)
			b, bv := r.Value(row)
			if av && a[0] != 0 || bv && b[0] != 0 {
				data[row] = 1
			} else if !av || !bv {
				validity.SetValid(row, false)
			} else {
				data[row] = 0
			}
		}
	}
	return true
}

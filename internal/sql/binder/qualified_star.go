package binder

import (
	"fmt"
	"sort"

	"github.com/steamwo/fssql/internal/sql/ast"
)

// qualifiedStarColumns 按关系原始顺序展开限定星号，并保留 USING 隐藏键的限定值
func qualifiedStarColumns(scope Scope, qualifier *ast.Identifier) ([]ColumnBinding, error) {
	if qualifier == nil {
		return scope.Columns, nil
	}
	var columns []ColumnBinding
	for _, bindings := range [][]ColumnBinding{scope.Columns, scope.Aliases} {
		for _, column := range bindings {
			if matchesIdentifier(column.Table, *qualifier) {
				columns = append(columns, column)
			}
		}
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("unknown relation qualifier %s", qualifier.Name)
	}
	sort.SliceStable(columns, func(i, j int) bool { return columns[i].TableColumnIndex < columns[j].TableColumnIndex })
	for i := 1; i < len(columns); i++ {
		if columns[i-1].TableColumnIndex == columns[i].TableColumnIndex {
			return nil, fmt.Errorf("relation qualifier %s is ambiguous", qualifier.Name)
		}
	}
	return columns, nil
}

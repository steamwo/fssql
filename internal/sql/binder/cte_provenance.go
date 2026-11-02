package binder

// cloneCTEQuery 复制已绑定 CTE query，并把显式 CTE 列别名写入 query-visible output metadata。
// 表达式/Source IR 仍按 immutable-by-convention 共享；这里只复制会因 alias 改写的 slice/metadata。
func cloneCTEQuery(query *BoundQuery, output []RelationColumn) *BoundQuery {
	if query == nil {
		return nil
	}
	out := *query
	out.Output = cloneRelationColumns(output)
	out.Projection = append([]BoundSelectItem(nil), query.Projection...)
	for i := range out.Projection {
		if i >= len(out.Output) {
			break
		}
		out.Projection[i].Output = out.Output[i]
		out.Projection[i].Output.Type = cloneLogicalType(out.Output[i].Type)
	}
	return &out
}

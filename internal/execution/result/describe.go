package result

import (
	"fmt"
	"strings"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/sql/binder"
)

const maxDescribeTypeTextBytes = 64 << 10
const maxDescribeTypeDepth = 64

// Column 描述 native result stream 的一列；DESCRIBE 结果通过该 schema 暴露稳定列名/类型。
type Column struct {
	Name     string            // Name 是 result-visible 列名。
	Type     types.LogicalType // Type 是 native DataChunk 列类型。
	Nullable bool              // Nullable 表示结果列本身是否允许 SQL NULL。
}

var describeOutputSchema = [...]Column{
	{Name: "column_name", Type: types.LogicalType{ID: types.Varchar}, Nullable: false},
	{Name: "column_type", Type: types.LogicalType{ID: types.Varchar}, Nullable: false},
	{Name: "null", Type: types.LogicalType{ID: types.Varchar}, Nullable: false},
	{Name: "key", Type: types.LogicalType{ID: types.Varchar}, Nullable: true},
	{Name: "default", Type: types.LogicalType{ID: types.Varchar}, Nullable: true},
	{Name: "extra", Type: types.LogicalType{ID: types.Varchar}, Nullable: true},
}

// DescribeSchema 返回 DuckDB-compatible DESCRIBE 六列 schema 的独立副本。
func DescribeSchema() []Column {
	out := make([]Column, len(describeOutputSchema))
	copy(out, describeOutputSchema[:])
	return out
}

// DescribeCursor 把 Binder 已解析 schema 流式物化为 native DataChunk；不会执行被 DESCRIBE 的查询。
type DescribeCursor struct {
	columns             []binder.RelationColumn // columns 是 Binder schema 的深拷贝。
	offset              int                     // offset 是下一批待物化列序号。
	preserveNullability bool                    // preserveNullability 仅 relation form 为 true；query form 按 DuckDB 输出 YES。
}

// NewDescribeCursor 创建 query-form DESCRIBE cursor；query nullability 按 DuckDB 行为统一显示 YES。
func NewDescribeCursor(stmt *binder.BoundDescribe) (*DescribeCursor, error) {
	return NewDescribeCursorWithNullability(stmt, false)
}

// NewDescribeCursorWithNullability 创建 DESCRIBE cursor；relation form 可保留 Catalog/Bind schema nullability。
func NewDescribeCursorWithNullability(stmt *binder.BoundDescribe, preserveNullability bool) (*DescribeCursor, error) {
	if stmt == nil || len(stmt.Columns) == 0 {
		return nil, fmt.Errorf("execution result: DESCRIBE requires non-empty bound schema")
	}
	columns := make([]binder.RelationColumn, len(stmt.Columns))
	for i, column := range stmt.Columns {
		if strings.TrimSpace(column.Name) == "" || column.Type.ID == types.Invalid {
			return nil, fmt.Errorf("execution result: invalid DESCRIBE column %d", i)
		}
		columns[i] = column
		columns[i].Type = cloneLogicalType(column.Type)
	}
	return &DescribeCursor{columns: columns, preserveNullability: preserveNullability}, nil
}

// Next 物化最多一个默认 DataChunk 的 DESCRIBE 行；done=true 时不再返回 chunk。
// Query form 因投影/表达式会丢失约束元数据，null 统一显示 YES；relation form 保留实际 column Nullable。
// 当前 v1 Catalog 没有 key/default metadata，因此 key/default/extra 输出 SQL NULL，避免用空字符串伪装未知 metadata。
func (c *DescribeCursor) Next(scope *memory.Scope, buffers *memory.BufferManager) (chunk *vector.DataChunk, done bool, err error) {
	if c == nil {
		return nil, false, fmt.Errorf("execution result: nil DESCRIBE cursor")
	}
	if scope == nil || buffers == nil {
		return nil, false, fmt.Errorf("execution result: DESCRIBE requires managed memory")
	}
	if c.offset >= len(c.columns) {
		return nil, true, nil
	}
	end := c.offset + vector.DefaultVectorSize
	if end > len(c.columns) {
		end = len(c.columns)
	}
	count := end - c.offset
	vectors := make([]*vector.Vector, len(describeOutputSchema))
	for i := range vectors {
		vectors[i], err = vector.NewFlat(scope, buffers, types.LogicalType{ID: types.Varchar}, count)
		if err != nil {
			releaseVectors(vectors)
			return nil, false, err
		}
	}
	for row := 0; row < count; row++ {
		column := c.columns[c.offset+row]
		typeText, typeErr := DuckDBTypeName(column.Type)
		if typeErr != nil {
			releaseVectors(vectors)
			return nil, false, fmt.Errorf("execution result: DESCRIBE column %q type: %w", column.Name, typeErr)
		}
		if err = vectors[0].SetString(row, column.Name); err != nil {
			releaseVectors(vectors)
			return nil, false, err
		}
		if err = vectors[1].SetString(row, typeText); err != nil {
			releaseVectors(vectors)
			return nil, false, err
		}
		nullableText := "YES"
		if c.preserveNullability && !column.Nullable {
			nullableText = "NO"
		}
		if err = vectors[2].SetString(row, nullableText); err != nil {
			releaseVectors(vectors)
			return nil, false, err
		}
		vectors[3].SetNull(row)
		vectors[4].SetNull(row)
		vectors[5].SetNull(row)
	}
	chunk, err = vector.NewDataChunk(vectors...)
	if err != nil {
		releaseVectors(vectors)
		return nil, false, err
	}
	c.offset = end
	return chunk, false, nil
}

// DuckDBTypeName 将核心 LogicalType 格式化为 Stable SQL compatibility 使用的 DuckDB-style 类型文本。
func DuckDBTypeName(t types.LogicalType) (string, error) {
	var b strings.Builder
	if err := appendTypeName(&b, t, 0); err != nil {
		return "", err
	}
	return b.String(), nil
}

// appendTypeName 递归格式化 nested type，并对控制面字符串大小和深度设置硬上限。
func appendTypeName(b *strings.Builder, t types.LogicalType, depth int) error {
	if depth > maxDescribeTypeDepth {
		return fmt.Errorf("nested type depth exceeds %d", maxDescribeTypeDepth)
	}
	write := func(s string) error {
		if len(s) > maxDescribeTypeTextBytes-b.Len() {
			return fmt.Errorf("type text exceeds %d bytes", maxDescribeTypeTextBytes)
		}
		b.WriteString(s)
		return nil
	}
	if t.IsUnsignedInteger() {
		names := map[types.LogicalTypeID]string{types.TinyInt: "UTINYINT", types.SmallInt: "USMALLINT", types.Integer: "UINTEGER", types.BigInt: "UBIGINT"}
		return write(names[t.ID])
	}
	if t.IsBlob() {
		return write("BLOB")
	}
	switch t.ID {
	case types.Boolean:
		return write("BOOLEAN")
	case types.TinyInt:
		return write("TINYINT")
	case types.SmallInt:
		return write("SMALLINT")
	case types.Integer:
		return write("INTEGER")
	case types.BigInt:
		return write("BIGINT")
	case types.HugeInt:
		return write("HUGEINT")
	case types.Float:
		return write("FLOAT")
	case types.Double:
		return write("DOUBLE")
	case types.Decimal:
		if t.Decimal.Width == 0 || t.Decimal.Width > 38 || t.Decimal.Scale > t.Decimal.Width {
			return fmt.Errorf("invalid DECIMAL(%d,%d)", t.Decimal.Width, t.Decimal.Scale)
		}
		return write(fmt.Sprintf("DECIMAL(%d,%d)", t.Decimal.Width, t.Decimal.Scale))
	case types.Varchar:
		return write("VARCHAR")
	case types.DateType:
		return write("DATE")
	case types.TimestampType:
		if t.IsTimestampTZ() {
			return write("TIMESTAMP WITH TIME ZONE")
		}
		if t.IsTimestampNS() {
			return write("TIMESTAMP_NS")
		}
		if t.Timestamp.Unit == types.TimestampMillis {
			return write("TIMESTAMP_MS")
		}
		return write("TIMESTAMP")
	case types.List:
		if t.Child == nil {
			return fmt.Errorf("LIST has no child type")
		}
		if err := appendTypeName(b, *t.Child, depth+1); err != nil {
			return err
		}
		return write("[]")
	case types.Struct:
		if len(t.Fields) == 0 {
			return write("STRUCT")
		}
		if err := write("STRUCT("); err != nil {
			return err
		}
		for i, field := range t.Fields {
			if strings.TrimSpace(field.Name) == "" {
				return fmt.Errorf("STRUCT field %d has empty name", i)
			}
			if i != 0 {
				if err := write(", "); err != nil {
					return err
				}
			}
			if err := write(formatStructFieldName(field.Name)); err != nil {
				return err
			}
			if err := write(" "); err != nil {
				return err
			}
			if err := appendTypeName(b, field.Type, depth+1); err != nil {
				return err
			}
		}
		return write(")")
	default:
		return fmt.Errorf("unsupported logical type id %d", t.ID)
	}
}

// formatStructFieldName 只在 DuckDB type text 需要时引用 STRUCT 字段名，并转义双引号。
func formatStructFieldName(name string) string {
	if simpleTypeIdentifier(name) && !typeKeyword(name) {
		return name
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func simpleTypeIdentifier(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if i == 0 {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_') {
				return false
			}
			continue
		}
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
			return false
		}
	}
	return true
}

func typeKeyword(name string) bool {
	switch strings.ToLower(name) {
	case "select", "from", "where", "group", "by", "having", "order", "limit", "offset", "join", "inner", "left", "right", "full", "cross", "semi", "anti", "null", "true", "false", "struct", "list", "date", "timestamp":
		return true
	default:
		return false
	}
}

func cloneLogicalType(t types.LogicalType) types.LogicalType {
	out := t
	if t.Child != nil {
		child := cloneLogicalType(*t.Child)
		out.Child = &child
	}
	if t.Fields != nil {
		out.Fields = make([]types.StructField, len(t.Fields))
		for i, field := range t.Fields {
			out.Fields[i] = types.StructField{Name: field.Name, Type: cloneLogicalType(field.Type)}
		}
	}
	return out
}

func releaseVectors(vectors []*vector.Vector) {
	for _, item := range vectors {
		if item != nil {
			item.Release()
		}
	}
}

package session

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/steamwo/fssql/internal/catalog"
	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	result "github.com/steamwo/fssql/internal/execution/result"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/binder"
)

const (
	internalInformationSchemaColumnsFunction = "__fssql_information_schema_columns"
	informationSchemaCatalogName             = "memory"
	maxInformationSchemaColumnRows           = 1 << 16
	maxInformationSchemaSnapshotBytes        = 32 << 20
)

// informationSchemaColumnRow 是 Prepare 阶段冻结的 information_schema.columns 单行元数据。
// 字段导出仅用于 package-internal JSON snapshot 编码，不构成 public API。
type informationSchemaColumnRow struct {
	TableSchema string `json:"table_schema"`
	TableName   string `json:"table_name"`
	ColumnName  string `json:"column_name"`
	Ordinal     int32  `json:"ordinal_position"`
	Nullable    bool   `json:"nullable"`
	TypeName    string `json:"data_type"`
	Precision   *int32 `json:"numeric_precision,omitempty"`
	Scale       *int32 `json:"numeric_scale,omitempty"`
	Datetime    *int32 `json:"datetime_precision,omitempty"`
}

// informationSchemaColumnsSnapshot 是 statement-scoped Catalog metadata 快照。
type informationSchemaColumnsSnapshot struct {
	fingerprint stats.DatasetFingerprint
	rows        []informationSchemaColumnRow
}

var informationSchemaColumnsSchema = [...]binder.RelationColumn{
	{Name: "table_catalog", Type: types.LogicalType{ID: types.Varchar}, Nullable: false},
	{Name: "table_schema", Type: types.LogicalType{ID: types.Varchar}, Nullable: false},
	{Name: "table_name", Type: types.LogicalType{ID: types.Varchar}, Nullable: false},
	{Name: "column_name", Type: types.LogicalType{ID: types.Varchar}, Nullable: false},
	{Name: "ordinal_position", Type: types.LogicalType{ID: types.Integer}, Nullable: false},
	{Name: "column_default", Type: types.LogicalType{ID: types.Varchar}, Nullable: true},
	{Name: "is_nullable", Type: types.LogicalType{ID: types.Varchar}, Nullable: false},
	{Name: "data_type", Type: types.LogicalType{ID: types.Varchar}, Nullable: false},
	{Name: "character_maximum_length", Type: types.LogicalType{ID: types.Integer}, Nullable: true},
	{Name: "character_octet_length", Type: types.LogicalType{ID: types.Integer}, Nullable: true},
	{Name: "numeric_precision", Type: types.LogicalType{ID: types.Integer}, Nullable: true},
	{Name: "numeric_scale", Type: types.LogicalType{ID: types.Integer}, Nullable: true},
	{Name: "datetime_precision", Type: types.LogicalType{ID: types.Integer}, Nullable: true},
	{Name: "is_generated", Type: types.LogicalType{ID: types.Varchar}, Nullable: false},
	{Name: "generation_expression", Type: types.LogicalType{ID: types.Varchar}, Nullable: true},
}

// isInformationSchemaColumnsName 只识别真正的两段 information_schema.columns 名称。
func isInformationSchemaColumnsName(name ast.QualifiedName) bool {
	if len(name.Parts) != 2 {
		return false
	}
	return informationSchemaIdentifierEqual(name.Parts[0], "information_schema") && informationSchemaIdentifierEqual(name.Parts[1], "columns")
}

func informationSchemaIdentifierEqual(identifier ast.Identifier, expected string) bool {
	if identifier.Quoted {
		return identifier.Name == expected
	}
	return strings.EqualFold(identifier.Name, expected)
}

// buildInformationSchemaColumnsSnapshot 从当前 Catalog transaction 构造 repeatable-read metadata snapshot。
// v1 Catalog 只有 main schema；information_schema 自身不是持久化 Catalog 对象，不递归列出自身。
func buildInformationSchemaColumnsSnapshot(ctx context.Context, tx catalog.Transaction) (binder.ResolvedRelation, informationSchemaColumnsSnapshot, error) {
	if tx == nil {
		return binder.ResolvedRelation{}, informationSchemaColumnsSnapshot{}, fmt.Errorf("session: information_schema.columns requires Catalog transaction")
	}
	entries, err := tx.List(ctx, "main")
	if err != nil {
		return binder.ResolvedRelation{}, informationSchemaColumnsSnapshot{}, err
	}
	rows := make([]informationSchemaColumnRow, 0)
	for _, entry := range entries {
		name, columns, err := informationSchemaEntryColumns(entry)
		if err != nil {
			return binder.ResolvedRelation{}, informationSchemaColumnsSnapshot{}, err
		}
		if len(rows) > maxInformationSchemaColumnRows-len(columns) {
			return binder.ResolvedRelation{}, informationSchemaColumnsSnapshot{}, fmt.Errorf("session: information_schema.columns row limit exceeded: %d", maxInformationSchemaColumnRows)
		}
		for ordinal, column := range columns {
			typeName, err := result.DuckDBTypeName(column.Type)
			if err != nil {
				return binder.ResolvedRelation{}, informationSchemaColumnsSnapshot{}, fmt.Errorf("session: information_schema.columns type %s.%s.%s: %w", name.Schema, name.Name, column.Name, err)
			}
			precision, scale, datetime := informationSchemaNumericMetadata(column.Type)
			rows = append(rows, informationSchemaColumnRow{TableSchema: name.Schema, TableName: name.Name, ColumnName: column.Name, Ordinal: int32(ordinal + 1), Nullable: column.Nullable, TypeName: typeName, Precision: precision, Scale: scale, Datetime: datetime})
		}
	}
	fingerprint := fingerprintInformationSchemaColumns(rows)
	id := "virtual:information_schema.columns:" + string(fingerprint)
	columns := make([]binder.RelationColumn, len(informationSchemaColumnsSchema))
	for i, column := range informationSchemaColumnsSchema {
		columns[i] = column
		columns[i].Type = cloneSourceType(column.Type)
	}
	return binder.ResolvedRelation{ID: id, Columns: columns, Fingerprint: fingerprint}, informationSchemaColumnsSnapshot{fingerprint: fingerprint, rows: rows}, nil
}

func informationSchemaEntryColumns(entry catalog.Entry) (catalog.ObjectName, []catalog.ColumnDefinition, error) {
	switch entry.Kind {
	case catalog.ObjectExternalTable:
		if entry.ExternalTable == nil {
			return catalog.ObjectName{}, nil, fmt.Errorf("session: information_schema.columns encountered invalid external-table entry")
		}
		return entry.ExternalTable.Name, entry.ExternalTable.Columns, nil
	case catalog.ObjectView:
		if entry.View == nil {
			return catalog.ObjectName{}, nil, fmt.Errorf("session: information_schema.columns encountered invalid view entry")
		}
		return entry.View.Name, entry.View.Columns, nil
	default:
		return catalog.ObjectName{}, nil, fmt.Errorf("session: information_schema.columns encountered unsupported Catalog kind %d", entry.Kind)
	}
}

// informationSchemaNumericMetadata 映射 DuckDB information_schema.columns 可由 LogicalType 精确确定的数值 metadata。
func informationSchemaNumericMetadata(t types.LogicalType) (precision, scale, datetime *int32) {
	value := func(v int32) *int32 { x := v; return &x }
	switch t.ID {
	case types.TinyInt:
		return value(8), value(0), nil
	case types.SmallInt:
		return value(16), value(0), nil
	case types.Integer:
		return value(32), value(0), nil
	case types.BigInt:
		return value(64), value(0), nil
	case types.HugeInt:
		return value(128), value(0), nil
	case types.Float:
		return value(24), nil, nil
	case types.Double:
		return value(53), nil, nil
	case types.Decimal:
		return value(int32(t.Decimal.Width)), value(int32(t.Decimal.Scale)), nil
	case types.DateType, types.TimestampType:
		return nil, nil, value(0)
	default:
		return nil, nil, nil
	}
}

func fingerprintInformationSchemaColumns(rows []informationSchemaColumnRow) stats.DatasetFingerprint {
	h := sha256.New()
	for _, row := range rows {
		_, _ = h.Write([]byte(row.TableSchema)); _, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(row.TableName)); _, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(row.ColumnName)); _, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(strconv.FormatInt(int64(row.Ordinal), 10))); _, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(row.TypeName))
		if row.Nullable { _, _ = h.Write([]byte{1}) } else { _, _ = h.Write([]byte{2}) }
	}
	return stats.DatasetFingerprint(hex.EncodeToString(h.Sum(nil)))
}

// encodeInformationSchemaSnapshot 将 bounded metadata snapshot 编码到内部 ScanSpec.Source。
func encodeInformationSchemaSnapshot(snapshot informationSchemaColumnsSnapshot) (string, error) {
	if snapshot.fingerprint == "" || len(snapshot.rows) > maxInformationSchemaColumnRows {
		return "", fmt.Errorf("session: invalid information_schema.columns snapshot")
	}
	raw, err := json.Marshal(snapshot.rows)
	if err != nil { return "", err }
	if len(raw) > maxInformationSchemaSnapshotBytes {
		return "", fmt.Errorf("session: information_schema.columns snapshot exceeds %d bytes", maxInformationSchemaSnapshotBytes)
	}
	return base64.RawStdEncoding.EncodeToString(raw), nil
}

func decodeInformationSchemaSnapshot(encoded string, expected stats.DatasetFingerprint) (informationSchemaColumnsSnapshot, error) {
	if expected == "" || encoded == "" {
		return informationSchemaColumnsSnapshot{}, fmt.Errorf("session: incomplete information_schema.columns runtime snapshot")
	}
	if len(encoded) > base64.RawStdEncoding.EncodedLen(maxInformationSchemaSnapshotBytes) {
		return informationSchemaColumnsSnapshot{}, fmt.Errorf("session: encoded information_schema.columns snapshot exceeds bound")
	}
	raw, err := base64.RawStdEncoding.DecodeString(encoded)
	if err != nil { return informationSchemaColumnsSnapshot{}, fmt.Errorf("session: decode information_schema.columns snapshot: %w", err) }
	if len(raw) > maxInformationSchemaSnapshotBytes {
		return informationSchemaColumnsSnapshot{}, fmt.Errorf("session: information_schema.columns snapshot exceeds bound")
	}
	var rows []informationSchemaColumnRow
	if err := json.Unmarshal(raw, &rows); err != nil {
		return informationSchemaColumnsSnapshot{}, fmt.Errorf("session: parse information_schema.columns snapshot: %w", err)
	}
	if len(rows) > maxInformationSchemaColumnRows {
		return informationSchemaColumnsSnapshot{}, fmt.Errorf("session: information_schema.columns row limit exceeded")
	}
	actual := fingerprintInformationSchemaColumns(rows)
	if actual != expected {
		return informationSchemaColumnsSnapshot{}, fmt.Errorf("session: information_schema.columns fingerprint changed between planning and execution")
	}
	return informationSchemaColumnsSnapshot{fingerprint: actual, rows: rows}, nil
}

// informationSchemaColumnsSource 将 frozen metadata snapshot 按默认 chunk 大小物化到 managed Vectors。
type informationSchemaColumnsSource struct {
	scope   *memory.Scope
	buffers *memory.BufferManager
	rows    []informationSchemaColumnRow
	offset  int
}

var _ pipeline.Source = (*informationSchemaColumnsSource)(nil)

func newInformationSchemaColumnsSource(scope *memory.Scope, buffers *memory.BufferManager, rows []informationSchemaColumnRow) (*informationSchemaColumnsSource, error) {
	if scope == nil || buffers == nil {
		return nil, fmt.Errorf("session: information_schema.columns runtime requires managed memory")
	}
	return &informationSchemaColumnsSource{scope: scope, buffers: buffers, rows: append([]informationSchemaColumnRow(nil), rows...)}, nil
}

// Next 只物化当前批次；所有 payload Vector/StringHeap 都由 Query MemoryScope 记账。
func (s *informationSchemaColumnsSource) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	if s == nil || s.scope == nil || s.buffers == nil { return nil, false, fmt.Errorf("session: information_schema.columns source is uninitialized") }
	if err := ctx.Err(); err != nil { return nil, false, err }
	if s.offset >= len(s.rows) { return nil, false, nil }
	end := s.offset + vector.DefaultVectorSize; if end > len(s.rows) { end = len(s.rows) }
	count := end - s.offset
	vectors := make([]*vector.Vector, len(informationSchemaColumnsSchema))
	for i, column := range informationSchemaColumnsSchema {
		item, err := vector.NewFlat(s.scope, s.buffers, column.Type, count)
		if err != nil { releaseInformationSchemaVectors(vectors); return nil, false, err }
		vectors[i] = item
	}
	for i := 0; i < count; i++ {
		row := s.rows[s.offset+i]
		if err := vectors[0].SetString(i, informationSchemaCatalogName); err != nil { releaseInformationSchemaVectors(vectors); return nil, false, err }
		if err := vectors[1].SetString(i, row.TableSchema); err != nil { releaseInformationSchemaVectors(vectors); return nil, false, err }
		if err := vectors[2].SetString(i, row.TableName); err != nil { releaseInformationSchemaVectors(vectors); return nil, false, err }
		if err := vectors[3].SetString(i, row.ColumnName); err != nil { releaseInformationSchemaVectors(vectors); return nil, false, err }
		vectors[4].SetInt32(i, row.Ordinal)
		vectors[5].SetNull(i)
		nullable := "NO"; if row.Nullable { nullable = "YES" }
		if err := vectors[6].SetString(i, nullable); err != nil { releaseInformationSchemaVectors(vectors); return nil, false, err }
		if err := vectors[7].SetString(i, row.TypeName); err != nil { releaseInformationSchemaVectors(vectors); return nil, false, err }
		vectors[8].SetNull(i); vectors[9].SetNull(i)
		setInformationSchemaOptionalInt32(vectors[10], i, row.Precision)
		setInformationSchemaOptionalInt32(vectors[11], i, row.Scale)
		setInformationSchemaOptionalInt32(vectors[12], i, row.Datetime)
		if err := vectors[13].SetString(i, "NEVER"); err != nil { releaseInformationSchemaVectors(vectors); return nil, false, err }
		vectors[14].SetNull(i)
	}
	chunk, err := vector.NewDataChunk(vectors...)
	if err != nil { releaseInformationSchemaVectors(vectors); return nil, false, err }
	s.offset = end
	return chunk, true, nil
}

func setInformationSchemaOptionalInt32(v *vector.Vector, row int, value *int32) {
	if value == nil { v.SetNull(row); return }
	v.SetInt32(row, *value)
}

func releaseInformationSchemaVectors(vectors []*vector.Vector) {
	for _, item := range vectors { if item != nil { item.Release() } }
}

// informationSchemaScanAdapter 是每 Query 预装的 internal virtual scan decoder。
func informationSchemaScanAdapter(scope *memory.Scope, buffers *memory.BufferManager) InternalScanOpenAdapter {
	return InternalScanOpenAdapter{
		Function: internalInformationSchemaColumnsFunction,
		Open: func(ctx context.Context, spec planner.ScanSpec) (pipeline.Source, error) {
			snapshot, err := decodeInformationSchemaSnapshot(spec.Source, spec.Fingerprint)
			if err != nil { return nil, err }
			return newInformationSchemaColumnsSource(scope, buffers, snapshot.rows)
		},
	}
}

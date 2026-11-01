package introspection

import (
	"fmt"
	"strings"

	coretypes "github.com/steamwo/fssql/internal/core/types"
)

type SchemaSource string

const (
	SchemaSourceExplicit       SchemaSource = "EXPLICIT"
	SchemaSourceCatalog        SchemaSource = "CATALOG"
	SchemaSourceFileMetadata   SchemaSource = "FILE_METADATA"
	SchemaSourceSampleInferred SchemaSource = "SAMPLE_INFERRED"
)

type Confidence string

const (
	ConfidenceExact   Confidence = "EXACT"
	ConfidenceHigh    Confidence = "HIGH"
	ConfidenceSampled Confidence = "SAMPLED"
	ConfidenceUnknown Confidence = "UNKNOWN"
)

type Column struct {
	Name     string
	Type     coretypes.LogicalType
	TypeName string
}

type SourceReport struct {
	columns            []Column
	SchemaSource       SchemaSource
	Confidence         Confidence
	MatchedFiles       int
	SchemaFiles        int
	DatasetFingerprint string
	UnionByName        bool
	FullValidation     bool
}

func NewSourceReport(fields []coretypes.StructField, source SchemaSource, confidence Confidence, matchedFiles, schemaFiles int, fingerprint string, unionByName, fullValidation bool) SourceReport {
	columns := make([]Column, len(fields))
	for i, field := range fields {
		t := cloneType(field.Type)
		columns[i] = Column{Name: field.Name, Type: t, TypeName: TypeName(t)}
	}
	return SourceReport{
		columns:            columns,
		SchemaSource:       source,
		Confidence:         confidence,
		MatchedFiles:       matchedFiles,
		SchemaFiles:        schemaFiles,
		DatasetFingerprint: fingerprint,
		UnionByName:        unionByName,
		FullValidation:     fullValidation,
	}
}

func (r SourceReport) Columns() []Column {
	out := make([]Column, len(r.columns))
	for i := range r.columns {
		out[i] = r.columns[i]
		out[i].Type = cloneType(r.columns[i].Type)
	}
	return out
}

func (r SourceReport) Fields() []coretypes.StructField {
	out := make([]coretypes.StructField, len(r.columns))
	for i := range r.columns {
		out[i] = coretypes.StructField{Name: r.columns[i].Name, Type: cloneType(r.columns[i].Type)}
	}
	return out
}

func TypeName(t coretypes.LogicalType) string {
	switch t.ID {
	case coretypes.Boolean:
		return "BOOLEAN"
	case coretypes.TinyInt:
		return "TINYINT"
	case coretypes.SmallInt:
		return "SMALLINT"
	case coretypes.Integer:
		return "INTEGER"
	case coretypes.BigInt:
		return "BIGINT"
	case coretypes.Float:
		return "FLOAT"
	case coretypes.Double:
		return "DOUBLE"
	case coretypes.Decimal:
		return fmt.Sprintf("DECIMAL(%d,%d)", t.Decimal.Width, t.Decimal.Scale)
	case coretypes.Varchar:
		return "VARCHAR"
	case coretypes.DateType:
		return "DATE"
	case coretypes.TimestampType:
		return "TIMESTAMP"
	case coretypes.List:
		if t.Child == nil {
			return "LIST<INVALID>"
		}
		return "LIST<" + TypeName(*t.Child) + ">"
	case coretypes.Struct:
		parts := make([]string, len(t.Fields))
		for i, field := range t.Fields {
			parts[i] = field.Name + " " + TypeName(field.Type)
		}
		return "STRUCT<" + strings.Join(parts, ", ") + ">"
	default:
		return "INVALID"
	}
}

func cloneType(t coretypes.LogicalType) coretypes.LogicalType {
	out := t
	if t.Child != nil {
		child := cloneType(*t.Child)
		out.Child = &child
	}
	if t.Fields != nil {
		out.Fields = make([]coretypes.StructField, len(t.Fields))
		for i, field := range t.Fields {
			out.Fields[i] = coretypes.StructField{Name: field.Name, Type: cloneType(field.Type)}
		}
	}
	return out
}

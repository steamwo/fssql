package parquet

import (
	"fmt"
	"strconv"

	coretypes "github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/format/scan"
	profilepkg "github.com/steamwo/fssql/internal/profile"
)

type ScanProfileInput struct {
	Binding          SourceBinding
	ProjectedColumns []string
	Filters          []scan.Filter
	RowGroupsTotal   int64
	FooterMetrics    []FooterMetrics
	RowGroupMetrics  []RowGroupScanMetrics
}

func BuildScanProfile(input ScanProfileInput) (profilepkg.ScanProfile, error) {
	p := profilepkg.ScanProfile{
		MatchedFiles:       input.Binding.MatchedFiles(),
		DatasetFingerprint: input.Binding.Fingerprint(),
		ProjectedColumns:   append([]string(nil), input.ProjectedColumns...),
		RowGroupsTotal:     input.RowGroupsTotal,
	}
	p.PushedFilters = make([]string, len(input.Filters))
	for i, filter := range input.Filters {
		p.PushedFilters[i] = formatProfileFilter(filter)
	}
	for _, metrics := range input.FooterMetrics {
		p.Requests += metrics.Requests
		p.PlannedBytes += metrics.PlannedBytes
		p.TransferredBytes += metrics.TransferredBytes
		p.UsefulBytes += metrics.UsefulBytes
	}
	for _, metrics := range input.RowGroupMetrics {
		p.Requests += metrics.Requests
		p.PlannedBytes += metrics.PlannedBytes
		p.TransferredBytes += metrics.TransferredBytes
		p.UsefulBytes += metrics.UsefulBytes
		p.RowsProduced += metrics.RowsProduced
		p.PagesRead += metrics.PagesRead
		if metrics.RowGroupsPruned > 0 {
			p.RowGroupsPruned += metrics.RowGroupsPruned
		} else {
			p.RowGroupsScanned++
		}
	}
	if err := p.Validate(); err != nil {
		return profilepkg.ScanProfile{}, err
	}
	return p, nil
}

func formatProfileFilter(filter scan.Filter) string {
	op := "?"
	switch filter.Op {
	case scan.Equal:
		op = "="
	case scan.NotEqual:
		op = "!="
	case scan.LessThan:
		op = "<"
	case scan.LessEqual:
		op = "<="
	case scan.GreaterThan:
		op = ">"
	case scan.GreaterEqual:
		op = ">="
	case scan.IsNull:
		return filter.Column + " IS NULL"
	case scan.IsNotNull:
		return filter.Column + " IS NOT NULL"
	}
	return filter.Column + " " + op + " " + formatProfileLiteral(filter.Value)
}

func formatProfileLiteral(lit scan.Literal) string {
	if lit.Null {
		return "NULL"
	}
	switch lit.Type.ID {
	case coretypes.Boolean:
		if lit.Bool {
			return "TRUE"
		}
		return "FALSE"
	case coretypes.TinyInt, coretypes.SmallInt, coretypes.Integer, coretypes.BigInt, coretypes.DateType, coretypes.TimestampType:
		return strconv.FormatInt(lit.Int64, 10)
	case coretypes.Float, coretypes.Double:
		return strconv.FormatFloat(lit.Float64, 'g', -1, 64)
	case coretypes.Varchar:
		return strconv.Quote(lit.String)
	default:
		return fmt.Sprintf("<type:%d>", lit.Type.ID)
	}
}

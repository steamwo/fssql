package profile

import (
	"errors"
	"fmt"
	"strings"
)

type ScanProfile struct {
	MatchedFiles       int
	DatasetFingerprint string
	ProjectedColumns   []string
	PushedFilters      []string
	RowGroupsTotal     int64
	RowGroupsPruned    int64
	RowGroupsScanned   int64
	Requests           int64
	PlannedBytes       int64
	TransferredBytes   int64
	UsefulBytes        int64
	RowsProduced       int64
	PagesRead          int64
}

func (p ScanProfile) Clone() ScanProfile {
	out := p
	out.ProjectedColumns = append([]string(nil), p.ProjectedColumns...)
	out.PushedFilters = append([]string(nil), p.PushedFilters...)
	return out
}

func (p ScanProfile) Validate() error {
	if p.MatchedFiles < 0 || p.RowGroupsTotal < 0 || p.RowGroupsPruned < 0 || p.RowGroupsScanned < 0 || p.Requests < 0 || p.PlannedBytes < 0 || p.TransferredBytes < 0 || p.UsefulBytes < 0 || p.RowsProduced < 0 || p.PagesRead < 0 {
		return errors.New("profile: scan metrics must be non-negative")
	}
	if p.RowGroupsPruned+p.RowGroupsScanned > p.RowGroupsTotal {
		return fmt.Errorf("profile: row group accounting exceeds total: pruned=%d scanned=%d total=%d", p.RowGroupsPruned, p.RowGroupsScanned, p.RowGroupsTotal)
	}
	if p.UsefulBytes > p.TransferredBytes {
		return fmt.Errorf("profile: useful bytes %d exceed transferred bytes %d", p.UsefulBytes, p.TransferredBytes)
	}
	return nil
}

func (p ScanProfile) Explain(analyze bool) string {
	var b strings.Builder
	b.WriteString("SCAN\n")
	fmt.Fprintf(&b, "  matched_files: %d\n", p.MatchedFiles)
	fmt.Fprintf(&b, "  projected_columns: [%s]\n", strings.Join(p.ProjectedColumns, ", "))
	fmt.Fprintf(&b, "  pushed_filters: [%s]\n", strings.Join(p.PushedFilters, ", "))
	fmt.Fprintf(&b, "  rowgroups: total=%d pruned=%d scanned=%d\n", p.RowGroupsTotal, p.RowGroupsPruned, p.RowGroupsScanned)
	fmt.Fprintf(&b, "  bytes: planned=%d transferred=%d useful=%d", p.PlannedBytes, p.TransferredBytes, p.UsefulBytes)
	if analyze {
		fmt.Fprintf(&b, "\n  runtime: requests=%d pages_read=%d rows_produced=%d", p.Requests, p.PagesRead, p.RowsProduced)
	}
	return b.String()
}

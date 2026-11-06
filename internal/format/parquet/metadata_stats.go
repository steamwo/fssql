package parquet

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/semantics"
)

// StatsAggregateKind selects the extremum answered from Parquet statistics.
type StatsAggregateKind uint8

const (
	StatsMin StatsAggregateKind = iota
	StatsMax
)

// StatsRequest describes one metadata-only MIN/MAX output column.
type StatsRequest struct {
	ColumnIndex int
	Kind        StatsAggregateKind
	OutputType  types.LogicalType
}

type statsPlan struct {
	path    []string
	leaf    *SchemaNode
	request StatsRequest
}

type statsOutput struct {
	valid bool
	value statValue
}

// MetadataStatsSource answers MIN/MAX from exact footer/row-group statistics
// without opening data pages. Unsupported statistics fall back to a normal scan
// before execution starts.
type MetadataStatsSource struct {
	mu      sync.Mutex
	binding SourceBinding
	outputs []statsOutput
	types   []types.LogicalType
	scope   *memory.Scope
	buffers *memory.BufferManager
	done    bool
}

// NewMetadataStatsSource validates all requests and computes the extremum from
// footer metadata. It returns accepted=false when exact statistics are missing
// so the caller can execute the ordinary aggregate instead.
func NewMetadataStatsSource(ctx context.Context, cfg RuntimeSourceConfig, requests []StatsRequest) (*MetadataStatsSource, bool, error) {
	if cfg.Binder == nil || cfg.Pattern == "" || cfg.ExpectedFingerprint == "" || len(requests) == 0 {
		return nil, false, nil
	}
	if len(cfg.Pushdown.Filters) != 0 || cfg.Pushdown.Limit != 0 {
		return nil, false, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	binding, err := cfg.Binder.Bind(ctx, cfg.Pattern, SourceBindOptions{MaxFooterBytes: cfg.MaxFooterBytes})
	if err != nil {
		return nil, false, err
	}
	if binding.Fingerprint() != cfg.ExpectedFingerprint {
		return nil, false, fmt.Errorf("parquet: source snapshot changed")
	}
	if len(binding.metadata) == 0 || binding.metadata[0].Footer.Schema == nil {
		return nil, false, nil
	}
	baseTree := binding.metadata[0].Footer.Schema
	plans, ok := resolveStatsRequests(baseTree, requests)
	if !ok {
		return nil, false, nil
	}
	outputs, nonNull, ok, err := collectStats(ctx, cfg, binding, baseTree, plans)
	if err != nil || !ok {
		return nil, false, err
	}
	if !nonNull {
		for i := range outputs {
			outputs[i].valid = false
		}
	}
	outputTypes := make([]types.LogicalType, len(requests))
	for i := range requests {
		outputTypes[i] = requests[i].OutputType
	}
	return &MetadataStatsSource{
		binding: binding, outputs: outputs, types: outputTypes,
		scope: cfg.Binder.scope, buffers: cfg.Binder.bm,
	}, true, nil
}

func resolveStatsRequests(tree *SchemaTree, requests []StatsRequest) ([]statsPlan, bool) {
	plans := make([]statsPlan, len(requests))
	for i, request := range requests {
		if request.ColumnIndex < 0 || request.ColumnIndex >= len(tree.Leaves) {
			return nil, false
		}
		leaf := tree.Leaves[request.ColumnIndex]
		if leaf == nil || len(leaf.Path) != 1 || !statsTypeSupported(leaf.Logical) {
			return nil, false
		}
		if !statsOutputMatches(leaf.Logical, request.OutputType) {
			return nil, false
		}
		plans[i] = statsPlan{path: append([]string(nil), leaf.Path...), leaf: leaf, request: request}
	}
	return plans, true
}

func statsTypeSupported(t types.LogicalType) bool {
	if t.IsUnsignedInteger() || t.ID == types.Decimal || t.ID == types.HugeInt {
		return false
	}
	switch t.ID {
	case types.Boolean, types.TinyInt, types.SmallInt, types.Integer, types.BigInt, types.DateType, types.TimestampType:
		return true
	default:
		return false
	}
}

func statsOutputMatches(leaf, output types.LogicalType) bool {
	if output.ID != leaf.ID {
		return false
	}
	if leaf.ID == types.TimestampType {
		return output.Timestamp.Unit == leaf.Timestamp.Unit && output.Timestamp.AdjustedToUTC == leaf.Timestamp.AdjustedToUTC
	}
	return true
}

func collectStats(ctx context.Context, cfg RuntimeSourceConfig, binding SourceBinding, baseTree *SchemaTree, plans []statsPlan) ([]statsOutput, bool, bool, error) {
	binder := cfg.Binder
	outputs := make([]statsOutput, len(plans))
	nonNull := false
	for _, file := range binding.ScanSet().Files() {
		if err := ctx.Err(); err != nil {
			return nil, false, false, err
		}
		driver, err := binder.registry.DriverFor(file.URI)
		if err != nil {
			return nil, false, false, err
		}
		footer, err := binder.cache.GetOrDiscover(ctx, driver, FooterSource{URI: file.URI, Size: file.Size, Identity: file.Identity}, binder.scope, binder.bm, cfg.MaxFooterBytes)
		if err != nil {
			return nil, false, false, err
		}
		if _, err := ResolveSchemas([]*SchemaTree{baseTree, footer.Schema}, false); err != nil {
			return nil, false, false, err
		}
		for i, plan := range plans {
			found, ok, err := collectFileStats(footer.Metadata, plan)
			if err != nil {
				return nil, false, false, err
			}
			if !ok {
				return nil, false, false, nil
			}
			if !found.valid {
				continue
			}
			nonNull = true
			if !outputs[i].valid {
				outputs[i] = found
				continue
			}
			order, ok := compareStats(plan.leaf.Logical, found.value, outputs[i].value)
			if !ok {
				return nil, false, false, nil
			}
			if (plan.request.Kind == StatsMin && order < 0) || (plan.request.Kind == StatsMax && order > 0) {
				outputs[i] = found
			}
		}
	}
	return outputs, nonNull, true, nil
}

// collectFileStats combines row-group statistics for one file. found.valid is
// false only when every non-empty row group is entirely NULL.
func collectFileStats(metadata FileMetaData, plan statsPlan) (statsOutput, bool, error) {
	out := statsOutput{}
	seen := false
	for _, rg := range metadata.RowGroups {
		if rg.NumRows < 0 {
			return statsOutput{}, false, fmt.Errorf("parquet: invalid row-group row count")
		}
		if rg.NumRows == 0 {
			continue
		}
		meta := findColumnMeta(rg, plan.path)
		if meta == nil {
			return statsOutput{}, false, nil
		}
		nullCount := int64(-1)
		if meta.Statistics != nil && meta.Statistics.NullCount != nil {
			nullCount = *meta.Statistics.NullCount
			if nullCount < 0 || nullCount > rg.NumRows {
				return statsOutput{}, false, nil
			}
		}
		if nullCount == rg.NumRows {
			continue
		}
		if meta.Statistics == nil {
			return statsOutput{}, false, nil
		}
		stats := meta.Statistics
		if (stats.IsMinValueExact != nil && !*stats.IsMinValueExact) || (stats.IsMaxValueExact != nil && !*stats.IsMaxValueExact) {
			return statsOutput{}, false, nil
		}
		minBytes, maxBytes := statsBounds(stats)
		if minBytes == nil || maxBytes == nil {
			return statsOutput{}, false, nil
		}
		minValue, minOK := decodeStatValue(plan.leaf, minBytes)
		maxValue, maxOK := decodeStatValue(plan.leaf, maxBytes)
		if !minOK || !maxOK {
			return statsOutput{}, false, nil
		}
		value := minValue
		if plan.request.Kind == StatsMax {
			value = maxValue
		}
		if !seen {
			out = statsOutput{valid: true, value: value}
			seen = true
			continue
		}
		order, ok := compareStats(plan.leaf.Logical, value, out.value)
		if !ok {
			return statsOutput{}, false, nil
		}
		if (plan.request.Kind == StatsMin && order < 0) || (plan.request.Kind == StatsMax && order > 0) {
			out.value = value
		}
	}
	return out, true, nil
}

func findColumnMeta(rg RowGroup, path []string) *ColumnMetaData {
	want := strings.Join(path, ".")
	for i := range rg.Columns {
		meta := rg.Columns[i].MetaData
		if meta != nil && strings.Join(meta.PathInSchema, ".") == want {
			return meta
		}
	}
	return nil
}

func compareStats(t types.LogicalType, a, b statValue) (int, bool) {
	if a.kind != b.kind {
		return 0, false
	}
	switch a.kind {
	case types.Boolean:
		if a.b == b.b {
			return 0, true
		}
		if !a.b {
			return -1, true
		}
		return 1, true
	case types.TinyInt, types.SmallInt, types.Integer, types.BigInt, types.DateType, types.TimestampType:
		return semantics.CompareInt64(a.i, b.i), true
	default:
		return 0, false
	}
}

func (s *MetadataStatsSource) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if s.done {
		return nil, false, nil
	}
	s.done = true
	columns := make([]*vector.Vector, len(s.outputs))
	for i, output := range s.outputs {
		column, err := vector.NewFlat(s.scope, s.buffers, s.types[i], 1)
		if err != nil {
			for _, column := range columns {
				if column != nil {
					column.Release()
				}
			}
			return nil, false, err
		}
		columns[i] = column
		if !output.valid {
			column.SetNull(0)
			continue
		}
		if err := writeStatsValue(column, s.types[i], output.value); err != nil {
			for _, column := range columns {
				if column != nil {
					column.Release()
				}
			}
			return nil, false, err
		}
	}
	chunk, err := vector.NewDataChunk(columns...)
	if err != nil {
		for _, column := range columns {
			if column != nil {
				column.Release()
			}
		}
		return nil, false, err
	}
	return chunk, true, nil
}

func writeStatsValue(v *vector.Vector, t types.LogicalType, value statValue) error {
	switch t.ID {
	case types.Boolean:
		v.SetBool(0, value.b)
	case types.TinyInt:
		v.SetInt8(0, int8(value.i))
	case types.SmallInt:
		v.SetInt16(0, int16(value.i))
	case types.Integer, types.DateType:
		v.SetInt32(0, int32(value.i))
	case types.BigInt, types.TimestampType:
		v.SetInt64(0, value.i)
	default:
		return fmt.Errorf("parquet: unsupported metadata aggregate type %d", t.ID)
	}
	return nil
}

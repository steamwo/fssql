package session

import (
	"fmt"

	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/query"
)

// QuerySourceExtensionFactory contributes query-scoped bind/runtime adapters
// frozen by the public composition root at DB Open.
type QuerySourceExtensionFactory func(*query.Context) ([]TableBindAdapter, []ScanOpenAdapter, error)

// WithAdapters returns a new immutable runtime registry with additional public
// table-function adapters. Existing factories remain unchanged and safe for
// statements already prepared against them.
func (f *RuntimeScanSourceFactory) WithAdapters(adapters []ScanOpenAdapter) (*RuntimeScanSourceFactory, error) {
	if f == nil || f.functions == nil {
		return nil, fmt.Errorf("session: runtime scan source factory is uninitialized")
	}
	if len(f.adapters)+len(adapters) > maxScanOpenAdapters {
		return nil, fmt.Errorf("session: too many scan open adapters: %d > %d", len(f.adapters)+len(adapters), maxScanOpenAdapters)
	}
	out := &RuntimeScanSourceFactory{
		relationStats: f.relationStats,
		functions: f.functions,
		adapters:  make(map[function.ID]ScanOpenFunc, len(f.adapters)+len(adapters)),
		internal:  make(map[string]ScanOpenFunc, len(f.internal)),
		counts:    make(map[function.ID]ScanOpenFunc, len(f.counts)),
		stats:     make(map[function.ID]AggregateScanOpenFunc, len(f.stats)),
	}
	for id, open := range f.adapters {
		out.adapters[id] = open
	}
	for name, open := range f.internal {
		out.internal[name] = open
	}
	for id, open := range f.counts {
		out.counts[id] = open
	}
	for id, open := range f.stats {
		out.stats[id] = open
	}
	for _, adapter := range adapters {
		if adapter.FunctionID == function.InvalidID || adapter.Open == nil {
			return nil, fmt.Errorf("session: invalid additional scan open adapter")
		}
		if _, exists := out.adapters[adapter.FunctionID]; exists {
			return nil, fmt.Errorf("session: duplicate scan open adapter for function id %d", adapter.FunctionID)
		}
		definition, ok := f.functions.LookupID(adapter.FunctionID)
		if !ok || definition.Kind != function.Table {
			return nil, fmt.Errorf("session: scan open adapter references unknown/non-table function id %d", adapter.FunctionID)
		}
		out.adapters[adapter.FunctionID] = adapter.Open
		if adapter.OpenCount != nil {
			out.counts[adapter.FunctionID] = adapter.OpenCount
		}
		if adapter.OpenStats != nil {
			out.stats[adapter.FunctionID] = adapter.OpenStats
		}
	}
	return out, nil
}

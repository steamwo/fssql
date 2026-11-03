package parquet

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

// MetadataCountSource reads only exact footer metadata from the frozen ScanSet.
// It owns no data buffers between Next calls and is safe for multiple workers.
type MetadataCountSource struct {
	mu      sync.Mutex
	cfg     RuntimeSourceConfig
	binding SourceBinding
	done    bool
}

func NewMetadataCountSource(ctx context.Context, cfg RuntimeSourceConfig) (*MetadataCountSource, error) {
	if cfg.Binder == nil || cfg.Pattern == "" || cfg.ExpectedFingerprint == "" {
		return nil, fmt.Errorf("parquet: metadata count requires binder and frozen source")
	}
	if len(cfg.Pushdown.Filters) != 0 || cfg.Pushdown.Limit != 0 {
		return nil, fmt.Errorf("parquet: metadata count requires an unfiltered, unlimited scan")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	binding, err := cfg.Binder.Bind(ctx, cfg.Pattern, SourceBindOptions{MaxFooterBytes: cfg.MaxFooterBytes})
	if err != nil {
		return nil, err
	}
	if binding.Fingerprint() != cfg.ExpectedFingerprint {
		return nil, fmt.Errorf("parquet: source snapshot changed")
	}
	return &MetadataCountSource{cfg: cfg, binding: binding}, nil
}

func (s *MetadataCountSource) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if s.done {
		return nil, false, nil
	}
	s.done = true
	b := s.cfg.Binder
	base := s.binding.metadata[0].Footer.Schema
	var total int64
	for _, file := range s.binding.ScanSet().Files() {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		driver, err := b.registry.DriverFor(file.URI)
		if err != nil {
			return nil, false, err
		}
		footer, err := b.cache.GetOrDiscover(ctx, driver, FooterSource{URI: file.URI, Size: file.Size, Identity: file.Identity}, b.scope, b.bm, s.cfg.MaxFooterBytes)
		if err != nil {
			return nil, false, err
		}
		if _, err := ResolveSchemas([]*SchemaTree{base, footer.Schema}, false); err != nil {
			return nil, false, err
		}
		n, err := exactMetadataRows(footer.Metadata)
		if err != nil {
			return nil, false, err
		}
		if n > math.MaxInt64-total {
			return nil, false, fmt.Errorf("parquet: COUNT(*) overflow")
		}
		total += n
	}
	v, err := vector.NewFlat(b.scope, b.bm, types.LogicalType{ID: types.BigInt}, 1)
	if err != nil {
		return nil, false, err
	}
	v.SetInt64(0, total)
	chunk, err := vector.NewDataChunk(v)
	if err != nil {
		v.Release()
		return nil, false, err
	}
	return chunk, true, nil
}

func exactMetadataRows(metadata FileMetaData) (int64, error) {
	var rows int64
	for _, rg := range metadata.RowGroups {
		if rg.NumRows < 0 || rg.NumRows > math.MaxInt64-rows {
			return 0, fmt.Errorf("parquet: invalid row-group row count")
		}
		rows += rg.NumRows
	}
	if metadata.NumRows < 0 || rows != metadata.NumRows {
		return 0, fmt.Errorf("parquet: inconsistent footer row count")
	}
	return rows, nil
}

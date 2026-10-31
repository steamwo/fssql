package parquet

import (
	"context"
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/fileset"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type SchemaValidationMode uint8

const (
	// SchemaValidationBounded is the ordinary bind/DESCRIBE mode. It binds from
	// the first immutable snapshot. This deliberately avoids implicit full-file
	// schema validation; later files still retain their immutable identities.
	SchemaValidationBounded SchemaValidationMode = iota
	// SchemaValidationFull explicitly validates every matched file schema.
	SchemaValidationFull
)

type SourceBindOptions struct {
	FileSet        fileset.PlanOptions
	UnionByName    bool
	ValidationMode SchemaValidationMode
	MaxFooterBytes int64
}

type BoundFileMetadata struct {
	File   fileset.FileSnapshot
	Footer Footer
}

type SourceBinding struct {
	scanSet        *fileset.ScanSet
	schema         ResolvedSchema
	metadata       []BoundFileMetadata
	validationMode SchemaValidationMode
	unionByName    bool
}

func (b SourceBinding) ScanSet() *fileset.ScanSet { return b.scanSet }
func (b SourceBinding) Schema() ResolvedSchema {
	out := b.schema
	out.Fields = append(out.Fields[:0:0], b.schema.Fields...)
	return out
}
func (b SourceBinding) Metadata() []BoundFileMetadata {
	return append([]BoundFileMetadata(nil), b.metadata...)
}
func (b SourceBinding) SchemaFiles() int                  { return len(b.metadata) }
func (b SourceBinding) ValidationMode() SchemaValidationMode { return b.validationMode }
func (b SourceBinding) UnionByName() bool                 { return b.unionByName }
func (b SourceBinding) MatchedFiles() int {
	if b.scanSet == nil {
		return 0
	}
	return b.scanSet.MatchedFiles()
}
func (b SourceBinding) Fingerprint() string {
	if b.scanSet == nil {
		return ""
	}
	return b.scanSet.Fingerprint()
}

type SourceBinder struct {
	registry *vfs.Registry
	planner  *fileset.Planner
	cache    *MetadataCache
	scope    *memory.Scope
	bm       *memory.BufferManager
}

func NewSourceBinder(registry *vfs.Registry, cache *MetadataCache, scope *memory.Scope, bm *memory.BufferManager) (*SourceBinder, error) {
	if registry == nil || cache == nil || scope == nil || bm == nil {
		return nil, errors.New("parquet: source binder requires registry, metadata cache, scope, and buffer manager")
	}
	return &SourceBinder{registry: registry, planner: fileset.NewPlanner(registry), cache: cache, scope: scope, bm: bm}, nil
}

// Bind is the single Parquet source binding path shared by query planning and
// source introspection. It always starts from FileSetPlanner, then obtains
// footer/schema metadata through MetadataCache, and finally calls ResolveSchemas.
func (b *SourceBinder) Bind(ctx context.Context, pattern string, opts SourceBindOptions) (SourceBinding, error) {
	if b == nil || b.registry == nil || b.planner == nil || b.cache == nil || b.scope == nil || b.bm == nil {
		return SourceBinding{}, errors.New("parquet: uninitialized source binder")
	}
	if opts.ValidationMode != SchemaValidationBounded && opts.ValidationMode != SchemaValidationFull {
		return SourceBinding{}, fmt.Errorf("parquet: invalid schema validation mode %d", opts.ValidationMode)
	}
	scanSet, err := b.planner.Plan(ctx, pattern, opts.FileSet)
	if err != nil {
		return SourceBinding{}, err
	}
	files := scanSet.Files()
	if len(files) == 0 {
		return SourceBinding{}, fileset.ErrNoMatches
	}

	// union_by_name must inspect all participating file schemas to produce the
	// actual union. FullValidation is otherwise explicit; ordinary strict bind
	// uses the first immutable snapshot only.
	schemaFileCount := 1
	if opts.UnionByName || opts.ValidationMode == SchemaValidationFull {
		schemaFileCount = len(files)
	}
	metadata := make([]BoundFileMetadata, 0, schemaFileCount)
	trees := make([]*SchemaTree, 0, schemaFileCount)
	for i := 0; i < schemaFileCount; i++ {
		file := files[i]
		driver, err := b.registry.DriverFor(file.URI)
		if err != nil {
			return SourceBinding{}, err
		}
		src := FooterSource{URI: file.URI, Size: file.Size, Identity: file.Identity}
		footer, err := b.cache.GetOrDiscover(ctx, driver, src, b.scope, b.bm, opts.MaxFooterBytes)
		if err != nil {
			return SourceBinding{}, fmt.Errorf("parquet: bind footer %s: %w", file.URI.String(), err)
		}
		if footer.Schema == nil || footer.Schema.Root == nil {
			return SourceBinding{}, fmt.Errorf("parquet: bind file %s has no schema", file.URI.String())
		}
		metadata = append(metadata, BoundFileMetadata{File: file, Footer: footer})
		trees = append(trees, footer.Schema)
	}
	resolved, err := ResolveSchemas(trees, opts.UnionByName)
	if err != nil {
		return SourceBinding{}, err
	}
	return SourceBinding{
		scanSet:        scanSet,
		schema:         resolved,
		metadata:       metadata,
		validationMode: opts.ValidationMode,
		unionByName:    opts.UnionByName,
	}, nil
}

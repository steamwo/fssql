package csv

import (
	"context"
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/storage/fileset"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

const (
	defaultInferenceSampleBytes int64 = 1 << 20
	defaultInferenceMaxBytes    int64 = 8 << 20
)

type SourceBindOptions struct {
	FileSet        fileset.PlanOptions
	Inference      InferenceOptions
	SampleBytes    int64
	MaxSampleBytes int64
}

type SourceBinding struct {
	scanSet    *fileset.ScanSet
	schema     InferredSchema
	inference  InferenceOptions
	schemaFile int
}

func (b SourceBinding) ScanSet() *fileset.ScanSet { return b.scanSet }
func (b SourceBinding) Schema() InferredSchema {
	out := b.schema
	out.Fields = append([]types.StructField(nil), b.schema.Fields...)
	return out
}
func (b SourceBinding) InferenceOptions() InferenceOptions { return b.inference }
func (b SourceBinding) MatchedFiles() int {
	if b.scanSet == nil { return 0 }
	return b.scanSet.MatchedFiles()
}
func (b SourceBinding) SchemaFiles() int { return b.schemaFile }
func (b SourceBinding) Fingerprint() string {
	if b.scanSet == nil { return "" }
	return b.scanSet.Fingerprint()
}
func (b SourceBinding) Types() []types.LogicalType {
	out := make([]types.LogicalType, len(b.schema.Fields))
	for i := range b.schema.Fields { out[i] = b.schema.Fields[i].Type }
	return out
}

type SourceBinder struct {
	registry *vfs.Registry
	planner  *fileset.Planner
	scope    *memory.Scope
	bm       *memory.BufferManager
}

func NewSourceBinder(registry *vfs.Registry, scope *memory.Scope, bm *memory.BufferManager) (*SourceBinder, error) {
	if registry == nil || scope == nil || bm == nil {
		return nil, errors.New("csv: source binder requires registry, memory scope, and buffer manager")
	}
	return &SourceBinder{registry: registry, planner: fileset.NewPlanner(registry), scope: scope, bm: bm}, nil
}

// Bind is the single CSV schema-resolution path shared by query execution and
// source introspection. It always freezes a FileSetPlanner ScanSet first and
// performs bounded inference against that immutable snapshot.
func (b *SourceBinder) Bind(ctx context.Context, pattern string, opts SourceBindOptions) (SourceBinding, error) {
	if b == nil || b.registry == nil || b.planner == nil || b.scope == nil || b.bm == nil {
		return SourceBinding{}, errors.New("csv: uninitialized source binder")
	}
	scanSet, err := b.planner.Plan(ctx, pattern, opts.FileSet)
	if err != nil { return SourceBinding{}, err }
	files := scanSet.Files()
	if len(files) == 0 { return SourceBinding{}, fileset.ErrNoMatches }
	file := files[0]
	if file.Size <= 0 { return SourceBinding{}, fmt.Errorf("csv: cannot infer schema from empty file %s", file.URI.String()) }
	driver, err := b.registry.DriverFor(file.URI)
	if err != nil { return SourceBinding{}, err }
	caps, err := b.registry.EffectiveCapabilities(ctx, file.URI)
	if err != nil { return SourceBinding{}, err }
	if !caps.Has(vfs.RandomRead) {
		return SourceBinding{}, fmt.Errorf("csv: schema inference requires RandomRead for %s", file.URI.String())
	}

	sample := opts.SampleBytes
	if sample <= 0 { sample = defaultInferenceSampleBytes }
	maxSample := opts.MaxSampleBytes
	if maxSample <= 0 { maxSample = defaultInferenceMaxBytes }
	if maxSample < sample { maxSample = sample }
	if sample > file.Size { sample = file.Size }
	if maxSample > file.Size { maxSample = file.Size }

	inferOpts := opts.Inference
	var lastErr error
	for {
		result, readErr := driver.ReadRange(ctx, vfs.RangeRequest{URI: file.URI, Offset: 0, Length: sample, Expected: &file.Identity, Tag: memory.TagReadAhead}, b.scope, b.bm)
		if readErr != nil { return SourceBinding{}, readErr }
		// 与 DuckDB 一致：文件起始的 UTF-8 BOM 不属于首列名/首列值。
		data, _ := trimCSVUTF8BOM(result.Buffer.Bytes())
		inferOpts.Final = sample == file.Size
		inferred, inferErr := InferSchema(data, inferOpts, b.scope, b.bm)
		result.Buffer.Release()
		if inferErr == nil {
			return SourceBinding{scanSet: scanSet, schema: inferred, inference: opts.Inference, schemaFile: 1}, nil
		}
		lastErr = inferErr
		if sample >= maxSample || sample >= file.Size { break }
		next := sample * 2
		if next > maxSample { next = maxSample }
		if next > file.Size { next = file.Size }
		if next == sample { break }
		sample = next
	}
	return SourceBinding{}, fmt.Errorf("csv: schema inference %s: %w", file.URI.String(), lastErr)
}

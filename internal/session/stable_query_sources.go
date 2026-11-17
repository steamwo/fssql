package session

import (
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/format/csv"
	jsonformat "github.com/steamwo/fssql/internal/format/json"
	"github.com/steamwo/fssql/internal/format/parquet"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/storage/objectview"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// StableQuerySourceFactory 是 production QuerySourceFactory：VFS/metadata cache 可跨 Query 复用，
// 任何持有 MemoryScope 的 format binder/runtime adapter 都按 QueryContext 重新创建。
type StableQuerySourceFactory struct {
	Registry       *vfs.Registry                // Registry 是统一 VFS capability/driver registry。
	ParquetCache   *parquet.MetadataCache       // ParquetCache 只缓存 immutable footer metadata，可跨 Query 共享。
	Buffers        *memory.BufferManager        // Buffers 是 engine 级 managed buffer allocator。
	Functions      *function.Catalog            // Functions 必须与 StableSQLEngine 使用同一个 catalog。
	CSVScan        csv.ScanOptions              // CSVScan 是 runtime scan policy；零值使用格式层既有默认。
	NDJSONScan     jsonformat.NDJSONScanOptions // NDJSONScan 是 runtime range/worker policy。
	JSONRangeBytes int64                        // JSONRangeBytes 控制普通 JSON 串行 range 窗口；<=0 使用 runtime 默认。
}

var _ QuerySourceFactory = (*StableQuerySourceFactory)(nil)

// NewStableQuerySourceFactory 创建 production source composition root。
func NewStableQuerySourceFactory(registry *vfs.Registry, parquetCache *parquet.MetadataCache, buffers *memory.BufferManager, functions *function.Catalog) (*StableQuerySourceFactory, error) {
	if registry == nil || parquetCache == nil || buffers == nil {
		return nil, fmt.Errorf("session: Stable query sources require VFS registry, Parquet metadata cache and BufferManager")
	}
	if functions == nil {
		functions = function.NewBuiltinCatalog()
	}
	return &StableQuerySourceFactory{Registry: registry, ParquetCache: parquetCache, Buffers: buffers, Functions: functions}, nil
}

// NewQuerySources 为一个 QueryContext 构造 Bind+runtime 唯一 source path。
// 同一批 binder 同时用于 schema Bind 与 runtime fingerprint re-bind，保证 AD-042/I-11 风格的一致性。
func (f *StableQuerySourceFactory) NewQuerySources(q *query.Context) ([]TableBindAdapter, *RuntimeScanSourceFactory, error) {
	if f == nil || f.Registry == nil || f.ParquetCache == nil || f.Buffers == nil || f.Functions == nil {
		return nil, nil, fmt.Errorf("session: Stable query source factory is uninitialized")
	}
	if q == nil || q.Memory == nil || q.Closed() {
		return nil, nil, fmt.Errorf("session: Stable query sources require live QueryContext")
	}
	views, err := objectview.New(q, f.Registry, f.Buffers)
	if err != nil {
		return nil, nil, err
	}
	registry := views.Registry()
	cache := parquet.NewQueryMetadataCache(f.ParquetCache)
	if err := q.AddCleanup(func() error { cache.Clear(); return nil }); err != nil {
		cache.Clear()
		return nil, nil, err
	}
	parquetBinder, err := parquet.NewSourceBinder(registry, cache, q.Memory, f.Buffers)
	if err != nil {
		return nil, nil, err
	}
	csvBinder, err := csv.NewSourceBinder(registry, q.Memory, f.Buffers)
	if err != nil {
		return nil, nil, err
	}
	jsonBinder, err := jsonformat.NewJSONBinder(registry, q.Memory, f.Buffers)
	if err != nil {
		return nil, nil, err
	}
	ndjsonBinder, err := jsonformat.NewNDJSONBinder(registry, q.Memory, f.Buffers)
	if err != nil {
		return nil, nil, err
	}
	resolver, err := NewStableTableFunctionResolver(registry, parquetBinder, csvBinder, jsonBinder, ndjsonBinder)
	if err != nil {
		return nil, nil, err
	}
	bindings, err := resolver.TableBindAdapters()
	if err != nil {
		return nil, nil, err
	}

	parquetScan, err := NewParquetScanOpenAdapter(parquetBinder)
	if err != nil {
		return nil, nil, err
	}
	csvScan, err := NewCSVScanOpenAdapter(csvBinder, f.CSVScan)
	if err != nil {
		return nil, nil, err
	}
	jsonScan, err := NewJSONScanOpenAdapter(jsonBinder, f.JSONRangeBytes)
	if err != nil {
		return nil, nil, err
	}
	ndjsonScan, err := NewNDJSONScanOpenAdapter(ndjsonBinder, f.NDJSONScan)
	if err != nil {
		return nil, nil, err
	}
	globScan, err := NewGlobScanOpenAdapter(registry, q.Memory, f.Buffers)
	if err != nil {
		return nil, nil, err
	}
	scans, err := NewRuntimeScanSourceFactory(f.Functions, []ScanOpenAdapter{parquetScan, csvScan, jsonScan, ndjsonScan, globScan})
	if err != nil {
		return nil, nil, err
	}
	// information_schema.columns 是 Planner-only virtual relation，不进入 FunctionCatalog。
	// adapter 每 Query 绑定当前 MemoryScope；真正 Catalog snapshot 已在 Prepare 阶段编码进 internal ScanSpec。
	scans, err = scans.WithInternalAdapters([]InternalScanOpenAdapter{informationSchemaScanAdapter(q.Memory, f.Buffers)})
	if err != nil {
		return nil, nil, err
	}
	return bindings, scans, nil
}

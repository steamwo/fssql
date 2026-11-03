// Package gosql exposes the Stable native Go API for the fssql engine.
package gosql

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/steamwo/fssql/internal/catalog"
	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
	exportpkg "github.com/steamwo/fssql/internal/export"
	"github.com/steamwo/fssql/internal/format/parquet"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/security"
	"github.com/steamwo/fssql/internal/session"
	s3remote "github.com/steamwo/fssql/internal/storage/remote/s3"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

const (
	defaultMemoryLimit = int64(256 << 20)
	defaultTempLimit = int64(1 << 30)
	defaultSpillMemoryBudget = int64(16 << 20)
	defaultSpillBlockBytes = 64 << 10
	defaultCatalogMaxEntries = 4096
	defaultMetadataCacheEntry = 256
)

type S3Config struct {
	Endpoint string
	Region string
	ForcePathStyle bool
	AllowInsecureHTTP bool
	AccessKey string
	SecretKey string
	SessionToken string
	CredentialScope string
	CredentialVersion string
	SecurityProfile string
	MaxRetries int
	RequestTimeout time.Duration
}

type Config struct {
	MemoryLimitBytes int64
	TempLimitBytes int64
	SpillMemoryBudgetBytes int64
	SpillBlockBytes int
	TempDir string
	ManifestDir string
	CatalogMaxEntries int
	ParquetMetadataEntries int
	Workers int
	BlockingQueue int
	S3 *S3Config
	LocalFiles *LocalFileConfig
	TableFunctions []TableFunction
	ScalarFunctions []ScalarFunction
}

type DB struct {
	memory *memory.Manager
	buffers *memory.BufferManager
	temporary *storagetemp.TemporaryStorageManager
	engine *session.StableStatementEngine
	closed atomic.Bool
}

func Open(config Config) (*DB, error) {
	if config.MemoryLimitBytes <= 0 { config.MemoryLimitBytes = defaultMemoryLimit }
	if config.TempLimitBytes <= 0 { config.TempLimitBytes = defaultTempLimit }
	if config.SpillMemoryBudgetBytes <= 0 { config.SpillMemoryBudgetBytes = defaultSpillMemoryBudget }
	if config.SpillMemoryBudgetBytes > config.MemoryLimitBytes { config.SpillMemoryBudgetBytes = config.MemoryLimitBytes }
	if config.SpillBlockBytes <= 0 { config.SpillBlockBytes = defaultSpillBlockBytes }
	if config.TempDir == "" { config.TempDir = filepath.Join(os.TempDir(), "fssql-spill") }
	if config.ManifestDir == "" { config.ManifestDir = filepath.Join(os.TempDir(), "fssql-copy-manifests") }
	if config.CatalogMaxEntries <= 0 { config.CatalogMaxEntries = defaultCatalogMaxEntries }
	if config.ParquetMetadataEntries <= 0 { config.ParquetMetadataEntries = defaultMetadataCacheEntry }

	manager := memory.NewManager(config.MemoryLimitBytes)
	buffers := memory.NewBufferManager(nil)
	cat, err := catalog.NewMemoryCatalog(config.CatalogMaxEntries)
	if err != nil { return nil, err }
	functions := function.NewPhase4Catalog()
	functions, registeredTables, err := registerTableFunctions(functions, config.TableFunctions)
	if err != nil { return nil, err }
	functions, scalars, err := registerScalarFunctions(functions, config.ScalarFunctions)
	if err != nil { return nil, err }

	drivers := make([]vfs.Driver, 0, 2)
	if config.LocalFiles != nil {
		driver, err := publicLocalDriver(*config.LocalFiles)
		if err != nil { return nil, err }
		drivers = append(drivers, driver)
	}
	if config.S3 != nil {
		driver, err := publicS3Driver(*config.S3)
		if err != nil { return nil, err }
		drivers = append(drivers, driver)
	}
	registry, err := vfs.NewRegistry(drivers...)
	if err != nil { return nil, err }
	metadata, err := parquet.NewMetadataCache(config.ParquetMetadataEntries)
	if err != nil { return nil, err }
	baseSources, err := session.NewStableQuerySourceFactory(registry, metadata, buffers, functions)
	if err != nil { return nil, err }
	spillBudget := config.SpillMemoryBudgetBytes
	spillBlock := config.SpillBlockBytes
	joinBudget := spillBudget
	if joinBudget < 4096 { joinBudget = 4096 }
	var sources session.QuerySourceFactory = baseSources
	if extensions := typedSourceExtensions(registeredTables, buffers, spillBudget); extensions != nil {
		sources = session.QuerySourceFactoryFunc(func(q *query.Context) ([]session.TableBindAdapter, *session.RuntimeScanSourceFactory, error) {
			bindings, scans, err := baseSources.NewQuerySources(q)
			if err != nil { return nil, nil, err }
			extraBindings, extraScans, err := extensions(q)
			if err != nil { return nil, nil, err }
			scans, err = scans.WithAdapters(extraScans)
			if err != nil { return nil, nil, err }
			return append(bindings, extraBindings...), scans, nil
		})
	}
	selects, err := session.NewStableSQLEngine(session.StableSQLEngineConfig{
		Catalog: cat,
		Functions: functions,
		Sources: sources,
		Buffers: buffers,
		Scalars: scalars,
		Distinct: session.ExternalDistinctRuntimeConfig{MemoryBudget: spillBudget, RadixBits: 4, SpillBlockBytes: spillBlock, Compression: storagetemp.CompressionNone},
		Sort: session.ExternalSortRuntimeConfig{MemoryBudget: spillBudget, SpillBlockBytes: spillBlock, Compression: storagetemp.CompressionNone},
		Aggregate: session.ExternalAggregateRuntimeConfig{MemoryBudget: spillBudget, RadixBits: 4, MaxDepth: 8, SpillBlockBytes: spillBlock, Compression: storagetemp.CompressionNone},
		Join: session.ExternalJoinRuntimeConfig{MemoryBudget: joinBudget, RadixBits: 4, MaxDepth: 8, MinReductionPercent: 10, SpillBlockBytes: spillBlock, Compression: storagetemp.CompressionNone},
		Workers: config.Workers,
		BlockingQueue: config.BlockingQueue,
	})
	if err != nil { return nil, err }

	secrets := security.NewSecretManager(nil)
	control, err := session.NewSQLControlAdapter(secrets, []string{"file", "s3", "sftp", "ftp", "webdav", "http", "https"})
	if err != nil { return nil, err }
	copyExecutor, err := session.NewExportCopyExecutor(session.ExportCopyExecutorConfig{
		Registry: registry,
		Buffers: buffers,
		ManifestStore: func(q *query.Context, jobID string) (exportpkg.ManifestStore, error) {
			return exportpkg.NewLocalManifestStore(filepath.Join(config.ManifestDir, jobID+".json"), 0, q.Memory, buffers)
		},
		MaxWriters: 1,
		PerWriterReservation: int64(spillBlock),
	})
	if err != nil { return nil, err }
	statements, err := session.NewStableStatementEngineWithCopy(selects, control, copyExecutor)
	if err != nil { return nil, err }
	quotas := storagetemp.NewTemporaryMemoryManager(storagetemp.Limits{Global: config.TempLimitBytes, DefaultResourceGroup: config.TempLimitBytes, DefaultQuery: config.TempLimitBytes})
	temporary, err := storagetemp.NewTemporaryStorageManager(config.TempDir, quotas)
	if err != nil { return nil, err }
	return &DB{memory: manager, buffers: buffers, temporary: temporary, engine: statements}, nil
}

func publicS3Driver(config S3Config) (vfs.Driver, error) {
	if config.CredentialScope == "" { config.CredentialScope = "gosql" }
	if config.SecurityProfile == "" { config.SecurityProfile = "gosql" }
	return s3remote.NewDriver(s3remote.Config{
		Endpoint: config.Endpoint,
		Region: config.Region,
		ForcePathStyle: config.ForcePathStyle,
		AllowInsecureHTTP: config.AllowInsecureHTTP,
		CredentialScope: config.CredentialScope,
		CredentialVersion: config.CredentialVersion,
		SecurityProfile: config.SecurityProfile,
		MaxRetries: config.MaxRetries,
		RequestTimeout: config.RequestTimeout,
	}, s3remote.Credentials{AccessKey: config.AccessKey, SecretKey: config.SecretKey, SessionToken: config.SessionToken}, nil)
}

func (db *DB) Close() error {
	if db == nil { return nil }
	db.closed.Store(true)
	return nil
}

func (db *DB) MemoryUsed() int64 {
	if db == nil || db.memory == nil { return 0 }
	return db.memory.Used()
}

type Column struct { Name string; Type Type; Nullable bool }

func (db *DB) QueryChunks(ctx context.Context, sql string, fn func(*Chunk) error) ([]Column, error) {
	if fn == nil { return nil, errors.New("gosql: nil chunk callback") }
	q, prepared, columns, err := db.prepare(ctx, sql)
	if err != nil { return nil, err }
	runErr := prepared.Run(q, &borrowedChunkSink{callback: fn})
	closeErr := q.Close()
	return columns, errors.Join(runErr, closeErr)
}

func (db *DB) Exec(ctx context.Context, sql string) error {
	q, prepared, _, err := db.prepare(ctx, sql)
	if err != nil { return err }
	runErr := prepared.Run(q, discardSink{})
	closeErr := q.Close()
	return errors.Join(runErr, closeErr)
}

func (db *DB) prepare(ctx context.Context, sql string) (*query.Context, session.PreparedStatement, []Column, error) {
	if db == nil || db.engine == nil || db.memory == nil || db.temporary == nil { return nil, nil, nil, errors.New("gosql: DB is uninitialized") }
	if db.closed.Load() { return nil, nil, nil, errors.New("gosql: DB is closed") }
	if err := ctx.Err(); err != nil { return nil, nil, nil, err }
	tempScope, err := db.temporary.NewQueryScope("default")
	if err != nil { return nil, nil, nil, err }
	q := query.NewWithTemp(ctx, db.memory, tempScope)
	prepared, err := db.engine.Prepare(ctx, q, sql)
	if err != nil {
		closeErr := q.Close()
		return nil, nil, nil, errors.Join(err, closeErr)
	}
	internalColumns := prepared.Output()
	columns := make([]Column, len(internalColumns))
	for i, column := range internalColumns { columns[i] = Column{Name: column.Name, Type: publicType(column.Type), Nullable: column.Nullable} }
	return q, prepared, columns, nil
}

type borrowedChunkSink struct { callback func(*Chunk) error }
func (s *borrowedChunkSink) Consume(_ context.Context, chunk *vector.DataChunk) error { return s.callback(&Chunk{chunk: chunk}) }
func (*borrowedChunkSink) Finalize(context.Context) error { return nil }

type discardSink struct{}
func (discardSink) Consume(context.Context, *vector.DataChunk) error { return nil }
func (discardSink) Finalize(context.Context) error { return nil }

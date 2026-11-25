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
	"github.com/steamwo/fssql/internal/sql/ast"
	s3remote "github.com/steamwo/fssql/internal/storage/remote/s3"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

const (
	defaultMemoryLimit        = int64(256 << 20)
	defaultTempLimit          = int64(1 << 30)
	defaultSpillMemoryBudget  = int64(16 << 20)
	defaultSpillBlockBytes    = 64 << 10
	defaultCatalogMaxEntries  = 4096
	defaultMetadataCacheEntry = 256
)

type S3Config struct {
	Endpoint          string
	Region            string
	ForcePathStyle    bool
	AllowInsecureHTTP bool
	AccessKey         string
	SecretKey         string
	SessionToken      string
	CredentialScope   string
	CredentialVersion string
	SecurityProfile   string
	MaxRetries        int
	RequestTimeout    time.Duration
}

type Config struct {
	MemoryLimitBytes       int64
	TempLimitBytes         int64
	SpillMemoryBudgetBytes int64
	SpillBlockBytes        int
	TempDir                string
	ManifestDir            string
	CatalogMaxEntries      int
	ParquetMetadataEntries int
	Workers                int
	BlockingQueue          int
	S3                     *S3Config
	HTTP                   []HTTPConfig
	LocalFiles             *LocalFileConfig
	FTP                    *FTPConfig
	SFTP                   *SFTPConfig
	WebDAV                 *WebDAVConfig
	TableFunctions         []TableFunction
	ScalarFunctions        []ScalarFunction
	// ParquetKeys resolves encrypted-export key bytes without placing keys in SQL or manifests.
	ParquetKeys ParquetKeyResolver
	// ParquetReadKeys resolves the raw 16- or 32-byte footer key for encrypted reads.
	ParquetReadKeys ParquetReadKeyResolver
	// SampleCompatibility enables DuckDB reservoir sampling. Empty preserves the
	// native stream; duckdb uses libstdc++, duckdb-msvc uses the Windows wheel.
	// Input order and chunk boundaries must match the reference engine.
	SampleCompatibility string
}

type DB struct {
	manifestDir string
	exportRegistry *vfs.Registry
	owner *DB
	memory            *memory.Manager
	buffers           *memory.BufferManager
	temporary         *storagetemp.TemporaryStorageManager
	engine            *session.StableStatementEngine
	closed            atomic.Bool
	materialized      *materializedRegistry
	spillMemoryBudget int64
	remoteResources   *remoteResources
}

func Open(config Config) (*DB, error) {
	return open(config, false)
}

func open(config Config, localReadOnly bool) (*DB, error) {
	if config.SampleCompatibility != "" && config.SampleCompatibility != "duckdb" && config.SampleCompatibility != "duckdb-msvc" {
		return nil, errors.New("gosql: unsupported SampleCompatibility (use duckdb or duckdb-msvc)")
	}
	if config.MemoryLimitBytes <= 0 {
		config.MemoryLimitBytes = defaultMemoryLimit
	}
	if config.TempLimitBytes <= 0 {
		config.TempLimitBytes = defaultTempLimit
	}
	if config.SpillMemoryBudgetBytes <= 0 {
		config.SpillMemoryBudgetBytes = defaultSpillMemoryBudget
	}
	if config.SpillMemoryBudgetBytes > config.MemoryLimitBytes {
		config.SpillMemoryBudgetBytes = config.MemoryLimitBytes
	}
	if config.SpillBlockBytes <= 0 {
		config.SpillBlockBytes = defaultSpillBlockBytes
	}
	if config.TempDir == "" {
		config.TempDir = filepath.Join(os.TempDir(), "fssql-spill")
	}
	if config.ManifestDir == "" {
		config.ManifestDir = filepath.Join(os.TempDir(), "fssql-copy-manifests")
	}
	if config.CatalogMaxEntries <= 0 {
		config.CatalogMaxEntries = defaultCatalogMaxEntries
	}
	if config.ParquetMetadataEntries <= 0 {
		config.ParquetMetadataEntries = defaultMetadataCacheEntry
	}

	manager := memory.NewManager(config.MemoryLimitBytes)
	buffers := memory.NewBufferManager(nil)
	baseCatalog, err := catalog.NewMemoryCatalog(config.CatalogMaxEntries)
	if err != nil {
		return nil, err
	}
	cat,err:=catalog.NewSessionCatalog(baseCatalog,config.CatalogMaxEntries)
	if err!=nil{return nil,err}
	functions := function.NewQueryCatalog()
	materialized := &materializedRegistry{items: make(map[string]*Materialized), limit: config.CatalogMaxEntries}
	functions, err = functions.With(function.Definition{ID: materializedFunctionID, Name: materializedFunctionName, Kind: function.Table, MinArgs: 1, MaxArgs: 1})
	if err != nil {
		return nil, err
	}
	functions, registeredTables, err := registerTableFunctions(functions, config.TableFunctions)
	if err != nil {
		return nil, err
	}
	functions, scalars, err := registerScalarFunctions(functions, config.ScalarFunctions)
	if err != nil {
		return nil, err
	}

	drivers := make([]vfs.Driver, 0, 2)
	if config.LocalFiles != nil {
		driver, err := publicLocalDriver(*config.LocalFiles, localReadOnly)
		if err != nil {
			return nil, err
		}
		drivers = append(drivers, driver)
	}
	if config.S3 != nil {
		driver, err := publicS3Driver(*config.S3)
		if err != nil {
			return nil, err
		}
		drivers = append(drivers, driver)
	}
	remotes, err := publicRemoteDrivers(config)
	if err != nil {
		return nil, err
	}
	opened := false
	defer func() {
		if !opened {
			_ = remotes.Close()
		}
	}()
	drivers = append(drivers, remotes.drivers...)
	registry, err := vfs.NewRegistry(drivers...)
	if err != nil {
		return nil, err
	}
	metadata, err := parquet.NewMetadataCache(config.ParquetMetadataEntries)
	if err != nil {
		return nil, err
	}
	baseSources, err := session.NewStableQuerySourceFactory(registry, metadata, buffers, functions)
	if err != nil {
		return nil, err
	}
	baseSources.ParquetReadKeys = parquet.ReadKeyResolver(config.ParquetReadKeys)
	spillBudget := config.SpillMemoryBudgetBytes
	spillBlock := config.SpillBlockBytes
	joinBudget := spillBudget
	if joinBudget < 4096 {
		joinBudget = 4096
	}
	var sources session.QuerySourceFactory = baseSources
	{
		extensions := typedSourceExtensions(registeredTables, buffers, spillBudget)
		sources = session.QuerySourceFactoryFunc(func(q *query.Context) ([]session.TableBindAdapter, *session.RuntimeScanSourceFactory, error) {
			bindings, scans, err := baseSources.NewQuerySources(q)
			if err != nil {
				return nil, nil, err
			}
			var extraBindings []session.TableBindAdapter
			var extraScans []session.ScanOpenAdapter
			if extensions != nil {
				extraBindings, extraScans, err = extensions(q)
				if err != nil {
					return nil, nil, err
				}
			}
			binding, scan := materialized.adapters(q)
			extraBindings = append(extraBindings, binding)
			extraScans = append(extraScans, scan)
			scans, err = scans.WithAdapters(extraScans)
			if err != nil {
				return nil, nil, err
			}
			return append(bindings, extraBindings...), scans, nil
		})
	}
	selects, err := session.NewStableSQLEngine(session.StableSQLEngineConfig{
		Catalog:       cat,
		Functions:     functions,
		Sources:       sources,
		Buffers:       buffers,
		Scalars:       scalars,
		Distinct:      session.ExternalDistinctRuntimeConfig{MemoryBudget: spillBudget, RadixBits: 4, SpillBlockBytes: spillBlock, Compression: storagetemp.CompressionNone},
		Sort:          session.ExternalSortRuntimeConfig{SampleCompatibility: config.SampleCompatibility, MemoryBudget: spillBudget, TopNMemoryBudget: config.MemoryLimitBytes, SpillBlockBytes: spillBlock, Compression: storagetemp.CompressionNone},
		Aggregate:     session.ExternalAggregateRuntimeConfig{MemoryBudget: spillBudget, RadixBits: 4, MaxDepth: 8, SpillBlockBytes: spillBlock, Compression: storagetemp.CompressionNone},
		Join:          session.ExternalJoinRuntimeConfig{MemoryBudget: joinBudget, RadixBits: 4, MaxDepth: 8, MinReductionPercent: 10, SpillBlockBytes: spillBlock, Compression: storagetemp.CompressionNone},
		Workers:       config.Workers,
		BlockingQueue: config.BlockingQueue,
	})
	if err != nil {
		return nil, err
	}

	secrets := security.NewSecretManager(nil)
	control, err := session.NewSQLControlAdapter(secrets, []string{"file", "s3", "sftp", "ftp", "webdav", "http", "https"})
	if err != nil {
		return nil, err
	}
	var parquetKeys session.ParquetKeyResolver
	if config.ParquetKeys != nil {
		parquetKeys = func(ctx context.Context, keyID, algorithm string) ([]byte, error) {
			return config.ParquetKeys(ctx, keyID, algorithm)
		}
	}
	copyExecutor, err := session.NewExportCopyExecutor(session.ExportCopyExecutorConfig{
		ManifestDir:config.ManifestDir,
		Registry:    registry,
		Buffers:     buffers,
		ParquetKeys: parquetKeys,
		ManifestStore: func(q *query.Context, jobID string) (exportpkg.ManifestStore, error) {
			return exportpkg.NewLocalManifestStore(filepath.Join(config.ManifestDir, jobID+".json"), 0, q.Memory, buffers)
		},
		MaxWriters:           1,
		PerWriterReservation: int64(spillBlock),
	})
	if err != nil {
		return nil, err
	}
	statements, err := session.NewStableStatementEngineWithCopy(selects, control, copyExecutor)
	if err != nil {
		return nil, err
	}
	quotas := storagetemp.NewTemporaryMemoryManager(storagetemp.Limits{Global: config.TempLimitBytes, DefaultResourceGroup: config.TempLimitBytes, DefaultQuery: config.TempLimitBytes})
	temporary, err := storagetemp.NewTemporaryStorageManager(config.TempDir, quotas)
	if err != nil {
		return nil, err
	}
	db := &DB{manifestDir:config.ManifestDir, exportRegistry:registry, memory: manager, buffers: buffers, temporary: temporary, engine: statements, materialized: materialized, spillMemoryBudget: spillBudget, remoteResources: remotes}
	statements.SetMaterializer(func(q *query.Context, prepared session.PreparedStatement) (catalog.ExternalTable, func() error, error) {
		var columns []Column
		var definitions []catalog.ColumnDefinition
		for _, c := range prepared.Output() {
			columns = append(columns, Column{Name: c.Name, Type: publicType(c.Type), Nullable: c.Nullable})
			definitions = append(definitions, catalog.ColumnDefinition{Name: c.Name, Type: c.Type, Nullable: c.Nullable})
		}
		m, err := db.materializePrepared(q.Context(), q, prepared, columns)
		if err != nil {
			return catalog.ExternalTable{}, nil, err
		}
		return catalog.ExternalTable{Columns: definitions, TableFunction: materializedFunctionName, Source: m.token}, m.Close, nil
	})
	opened = true
	return db, nil
}

func publicS3Driver(config S3Config) (vfs.Driver, error) {
	if config.CredentialScope == "" {
		config.CredentialScope = "gosql"
	}
	if config.SecurityProfile == "" {
		config.SecurityProfile = "gosql"
	}
	return s3remote.NewDriver(s3remote.Config{
		Endpoint:          config.Endpoint,
		Region:            config.Region,
		ForcePathStyle:    config.ForcePathStyle,
		AllowInsecureHTTP: config.AllowInsecureHTTP,
		CredentialScope:   config.CredentialScope,
		CredentialVersion: config.CredentialVersion,
		SecurityProfile:   config.SecurityProfile,
		MaxRetries:        config.MaxRetries,
		RequestTimeout:    config.RequestTimeout,
	}, s3remote.Credentials{AccessKey: config.AccessKey, SecretKey: config.SecretKey, SessionToken: config.SessionToken}, nil)
}

func (db *DB) Close() error {
	if db == nil {
		return nil
	}
	db.closed.Store(true)
 if db.owner!=nil{if db.engine==nil{return nil};return db.engine.CloseSession()}
 var sessionErr error;if db.engine!=nil{sessionErr=db.engine.CloseSession()}
	remoteErr := db.remoteResources.Close()
	if db.materialized != nil {
		return errors.Join(sessionErr, remoteErr, db.materialized.close())
	}
	return errors.Join(sessionErr, remoteErr)
}

func (db *DB) MemoryUsed() int64 {
	if db == nil || db.memory == nil {
		return 0
	}
	return db.memory.Used()
}

type Column struct {
	Name     string
	Type     Type
	Nullable bool
}

func (db *DB) QueryChunks(ctx context.Context, sql string, fn func(*Chunk) error) ([]Column, error) {
	return db.QueryChunksWithArgs(ctx, sql, fn)
}

// QueryChunksWithArgs binds SQL value parameters and streams chunks to fn.
func (db *DB) QueryChunksWithArgs(ctx context.Context, sql string, fn func(*Chunk) error, args ...any) ([]Column, error) {
	return db.queryChunks(ctx, sql, fn, args, nil)
}

func (db *DB) queryChunks(ctx context.Context, sql string, fn func(*Chunk) error, args []any, stats *QueryStats) ([]Column, error) {
	if fn == nil {
		return nil, errors.New("gosql: nil chunk callback")
	}
	var start time.Time
	var inputStats *vfs.InputStats
	if stats != nil {
		ctx, inputStats = vfs.WithInputStats(ctx)
		defer func() {
			stats.FilesMatched, stats.FilesRead, stats.ReadRequests, stats.BytesRead = inputStats.Snapshot()
		}()
		start = time.Now()
	}
	q, prepared, columns, err := db.prepareWithArgs(ctx, sql, args)
	if stats != nil {
		stats.PlanningTime = time.Since(start)
	}
	if err != nil {
		return nil, err
	}
	var rows, chunks atomic.Int64
	callback := fn
	if stats != nil {
		start = time.Now()
		callback = func(chunk *Chunk) error {
			rows.Add(int64(chunk.RowCount()))
			chunks.Add(1)
			return fn(chunk)
		}
	}
	runErr := prepared.Run(q, &borrowedChunkSink{callback: callback})
	if stats != nil {
		stats.ExecutionTime = time.Since(start)
		stats.Rows, stats.Chunks = rows.Load(), chunks.Load()
		stats.PeakManagedBytes, stats.PeakTempBytes = q.Memory.Peak(), q.Temp.Peak()
	}
	closeErr := q.Close()
	return columns, errors.Join(runErr, closeErr)
}

func (db *DB) Exec(ctx context.Context, sql string) error {
	q, prepared, _, err := db.prepare(ctx, sql)
	if err != nil {
		return err
	}
	runErr := prepared.Run(q, discardSink{})
	closeErr := q.Close()
	return errors.Join(runErr, closeErr)
}

func (db *DB) prepare(ctx context.Context, sql string) (*query.Context, session.PreparedStatement, []Column, error) {
	return db.prepareParsed(ctx, sql, nil)
}

func (db *DB) prepareParsed(ctx context.Context, sql string, parsed ast.Statement) (*query.Context, session.PreparedStatement, []Column, error) {
	if db == nil || db.engine == nil || db.memory == nil || db.temporary == nil {
		return nil, nil, nil, errors.New("gosql: DB is uninitialized")
	}
	if db.closed.Load() || (db.owner!=nil&&db.owner.closed.Load()) {
		return nil, nil, nil, errors.New("gosql: DB is closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	tempScope, err := db.temporary.NewQueryScope("default")
	if err != nil {
		return nil, nil, nil, err
	}
	q := query.NewWithTemp(ctx, db.memory, tempScope)
	var prepared session.PreparedStatement
	if parsed == nil {
		prepared, err = db.engine.Prepare(ctx, q, sql)
	} else {
		prepared, err = db.engine.PrepareParsed(ctx, q, sql, parsed)
	}
	if err != nil {
		closeErr := q.Close()
		return nil, nil, nil, errors.Join(err, closeErr)
	}
	internalColumns := prepared.Output()
	columns := make([]Column, len(internalColumns))
	for i, column := range internalColumns {
		columns[i] = Column{Name: column.Name, Type: publicType(column.Type), Nullable: column.Nullable}
	}
	return q, prepared, columns, nil
}

type borrowedChunkSink struct{ callback func(*Chunk) error }

func (s *borrowedChunkSink) Consume(_ context.Context, chunk *vector.DataChunk) error {
	return s.callback(&Chunk{chunk: chunk})
}
func (*borrowedChunkSink) Finalize(context.Context) error { return nil }

type discardSink struct{}

func (discardSink) Consume(context.Context, *vector.DataChunk) error { return nil }
func (discardSink) Finalize(context.Context) error                   { return nil }

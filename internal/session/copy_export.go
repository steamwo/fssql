package session

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	pathpkg "path"
	"strings"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	exportpkg "github.com/steamwo/fssql/internal/export"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/sql/binder"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type CopyManifestStoreFactory func(*query.Context, string) (exportpkg.ManifestStore, error)

type ParquetKeyResolver func(context.Context, string, string) ([]byte, error)

type ExportCopyExecutorConfig struct {
	Registry             *vfs.Registry
	Buffers              *memory.BufferManager
	ManifestStore        CopyManifestStoreFactory
	ParquetKeys          ParquetKeyResolver
	MaxWriters           int
	PerWriterReservation int64
}

type ExportCopyExecutor struct{ config ExportCopyExecutorConfig }

type CopyRequest struct {
	Destination         string
	Format              string
	Compression         string
	Header              bool
	Delimiter           byte
	Quote               byte
	EncryptionAlgorithm string
	KeyID               string
}

type CopyExportResult struct {
	Rows   int64
	Bytes  int64
	Schema []exportpkg.Column
}

func NewExportCopyExecutor(config ExportCopyExecutorConfig) (*ExportCopyExecutor, error) {
	if config.Registry == nil || config.Buffers == nil || config.ManifestStore == nil {
		return nil, errors.New("session: COPY export executor requires VFS registry, BufferManager and ManifestStore factory")
	}
	if config.MaxWriters <= 0 {
		config.MaxWriters = 1
	}
	if config.MaxWriters != 1 {
		return nil, fmt.Errorf("session: Stable COPY requires exactly one writer slot")
	}
	if config.PerWriterReservation <= 0 {
		config.PerWriterReservation = 64 << 10
	}
	return &ExportCopyExecutor{config: config}, nil
}

// Run keeps SQL COPY behavior unchanged while sharing the same Phase 6 execution path.
func (e *ExportCopyExecutor) Run(q *query.Context, bound *binder.BoundCopyTo, prepared *PreparedSQLQuery, sql string) error {
	if bound == nil {
		return errors.New("session: COPY bound statement is nil")
	}
	schema, err := copyExportSchema(prepared)
	if err != nil {
		return err
	}
	writerConfig, err := copyWriterConfig(bound.Options, schema)
	if err != nil {
		return err
	}
	_, err = e.runPrepared(q, prepared, bound.Destination, writerConfig, sql)
	return err
}

// RunPrepared is the typed API entrypoint used by gosql.DB.CopyTo.
func (e *ExportCopyExecutor) RunPrepared(q *query.Context, prepared *PreparedSQLQuery, request CopyRequest, queryText string) (CopyExportResult, error) {
	schema, err := copyExportSchema(prepared)
	if err != nil {
		return CopyExportResult{}, err
	}
	config, err := copyRequestWriterConfig(request, schema)
	if err != nil {
		return CopyExportResult{}, err
	}
	if request.EncryptionAlgorithm != "" || request.KeyID != "" {
		if config.Format != exportpkg.FormatParquet {
			return CopyExportResult{}, errors.New("session: encryption is only supported for Parquet COPY")
		}
		algorithm, err := exportpkg.NormalizeParquetEncryptionAlgorithm(request.EncryptionAlgorithm)
		if err != nil {
			return CopyExportResult{}, err
		}
		keyID := strings.TrimSpace(request.KeyID)
		if keyID == "" {
			return CopyExportResult{}, errors.New("session: encrypted Parquet COPY requires keyId")
		}
		if e.config.ParquetKeys == nil {
			return CopyExportResult{}, errors.New("session: encrypted Parquet COPY requires Config.ParquetKeys")
		}
		resolved, err := e.config.ParquetKeys(q.Context(), keyID, algorithm)
		if err != nil {
			return CopyExportResult{}, fmt.Errorf("session: resolve Parquet key %q: %w", keyID, err)
		}
		key := append([]byte(nil), resolved...)
		defer zeroKeyBytes(key)
		config.Parquet.Encryption = &exportpkg.ParquetEncryptionConfig{Algorithm: algorithm, KeyID: keyID, Key: key}
		if err := exportpkg.ValidateParquetEncryption(config.Parquet.Encryption); err != nil {
			return CopyExportResult{}, err
		}
	}
	return e.runPrepared(q, prepared, request.Destination, config, queryText)
}

func (e *ExportCopyExecutor) runPrepared(q *query.Context, prepared *PreparedSQLQuery, destination string, writerConfig exportpkg.FileWriterConfig, queryText string) (CopyExportResult, error) {
	if e == nil || e.config.Registry == nil || e.config.Buffers == nil || e.config.ManifestStore == nil {
		return CopyExportResult{}, errors.New("session: COPY export executor is uninitialized")
	}
	if q == nil || q.Memory == nil || q.Closed() || prepared == nil {
		return CopyExportResult{}, errors.New("session: COPY requires live query and prepared query")
	}
	if err := q.Context().Err(); err != nil {
		return CopyExportResult{}, err
	}
	snapshot, ok := q.SourceSnapshot()
	if !ok || strings.TrimSpace(snapshot.Fingerprint) == "" {
		return CopyExportResult{}, errors.New("session: COPY refuses to start without frozen Query SourceSnapshot")
	}
	// Validate the factory, including encryption key length and page codec, before any output writer is opened.
	pool, err := exportpkg.NewWriterPool(e.config.MaxWriters, e.config.PerWriterReservation, q.Memory)
	if err != nil {
		return CopyExportResult{}, err
	}
	factory, err := exportpkg.NewFileWriterFactory(writerConfig, e.config.Registry, q.Memory, e.config.Buffers, pool)
	if err != nil {
		return CopyExportResult{}, err
	}

	root, filePath, err := copyDestination(destination)
	if err != nil {
		return CopyExportResult{}, err
	}
	finalURI := root
	finalURI.Path = pathpkg.Join(root.Path, filePath)
	finalURI.Raw = finalURI.String()
	caps, err := e.config.Registry.EffectiveCapabilities(q.Context(), finalURI)
	if err != nil {
		return CopyExportResult{}, err
	}
	if !caps.Has(vfs.AtomicPublishWrite) {
		return CopyExportResult{}, fmt.Errorf("session: COPY destination %s does not provide AtomicPublishWrite", finalURI.Scheme)
	}

	queryFingerprint := fingerprintText(queryText)
	exportIdentity := string(writerConfig.Format) + "\x00" + writerConfig.Compression
	if writerConfig.Parquet.Encryption != nil {
		exportIdentity += "\x00" + writerConfig.Parquet.Encryption.Algorithm + "\x00" + writerConfig.Parquet.Encryption.KeyID
	}
	jobID := fingerprintText(queryFingerprint + "\x00" + snapshot.Fingerprint + "\x00" + finalURI.String() + "\x00" + exportIdentity)
	manifest, err := exportpkg.NewManifest(jobID, queryFingerprint, snapshot.Fingerprint, root.String(), string(writerConfig.Format), writerConfig.Compression, "atomic_publish", []string{filePath})
	if err != nil {
		return CopyExportResult{}, err
	}
	manifest.Codec = writerConfig.Compression
	if writerConfig.Parquet.Encryption != nil {
		manifest.EncryptionAlgorithm = writerConfig.Parquet.Encryption.Algorithm
		manifest.KeyID = writerConfig.Parquet.Encryption.KeyID
	}
	if err := manifest.Validate(); err != nil {
		return CopyExportResult{}, err
	}
	if err := manifest.TransitionExport(exportpkg.ExportStateWriting); err != nil {
		return CopyExportResult{}, err
	}
	store, err := e.config.ManifestStore(q, jobID)
	if err != nil {
		return CopyExportResult{}, err
	}
	if store == nil {
		return CopyExportResult{}, errors.New("session: COPY ManifestStore factory returned nil")
	}
	if err := store.Save(q.Context(), manifest); err != nil {
		return CopyExportResult{}, fmt.Errorf("session: persist COPY WRITING manifest: %w", err)
	}

	router, err := exportpkg.NewFileRouter(exportpkg.RoutingConfig{Mode: exportpkg.LayoutSingleFile, SingleFilePath: filePath}, writerConfig.Schema)
	if err != nil {
		return CopyExportResult{}, err
	}
	dataset, err := exportpkg.NewAtomicSingleFileDatasetWriter(manifest, router, factory)
	if err != nil {
		return CopyExportResult{}, err
	}
	commit := &exportpkg.SingleFileCommitter{Manifest: manifest, Store: store, Registry: e.config.Registry}
	copySink := &copyExportSink{dataset: dataset, manifest: manifest, store: store, commit: commit}
	if err := prepared.Run(q, copySink); err != nil {
		return CopyExportResult{}, err
	}
	if pool.Active() != 0 {
		return CopyExportResult{}, fmt.Errorf("session: COPY writer ownership leak: active=%d", pool.Active())
	}
	if len(manifest.FileStates) != 1 || manifest.FileStates[0].State != exportpkg.FileStateComplete {
		return CopyExportResult{}, errors.New("session: COPY completed without one COMPLETE output file")
	}
	return CopyExportResult{Rows: copySink.rows, Bytes: manifest.FileStates[0].Size, Schema: append([]exportpkg.Column(nil), writerConfig.Schema...)}, nil
}

func copyDestination(raw string) (vfs.URI, string, error) {
	u, err := vfs.ParseURI(strings.TrimSpace(raw))
	if err != nil {
		return vfs.URI{}, "", fmt.Errorf("session: COPY destination must resolve through VFS: %w", err)
	}
	filePath := pathpkg.Base(u.Path)
	if filePath == "" || filePath == "." || filePath == "/" {
		return vfs.URI{}, "", errors.New("session: COPY destination must name a single file object")
	}
	root := u
	root.Path = pathpkg.Dir(u.Path)
	if root.Path == "." {
		root.Path = "/"
	}
	root.Raw = root.String()
	return root, filePath, nil
}

func copyExportSchema(prepared *PreparedSQLQuery) ([]exportpkg.Column, error) {
	if prepared == nil {
		return nil, errors.New("session: COPY prepared query is nil")
	}
	columns := prepared.Output()
	if len(columns) == 0 {
		return nil, errors.New("session: COPY query has empty output schema")
	}
	out := make([]exportpkg.Column, len(columns))
	for i, column := range columns {
		out[i] = exportpkg.Column{Name: column.Name, Type: cloneStatementResultType(column.Type)}
	}
	return out, nil
}

func copyWriterConfig(options binder.BoundCopyOptions, schema []exportpkg.Column) (exportpkg.FileWriterConfig, error) {
	config := exportpkg.FileWriterConfig{Schema: schema, Compression: options.Compression}
	switch options.Format {
	case binder.CopyCSV:
		config.Format = exportpkg.FormatCSV
		config.CSV = exportpkg.CSVWriteOptions{Header: options.Header, Delimiter: options.Delimiter, Quote: options.Quote}
	case binder.CopyJSON:
		config.Format = exportpkg.FormatJSON
	case binder.CopyNDJSON:
		config.Format = exportpkg.FormatNDJSON
	case binder.CopyParquet:
		config.Format = exportpkg.FormatParquet
	default:
		return exportpkg.FileWriterConfig{}, fmt.Errorf("session: unsupported bound COPY format %q", options.Format)
	}
	return config, nil
}

func copyRequestWriterConfig(request CopyRequest, schema []exportpkg.Column) (exportpkg.FileWriterConfig, error) {
	format := strings.ToLower(strings.TrimSpace(request.Format))
	if format == "" {
		format = "csv"
	}
	compression := strings.ToLower(strings.TrimSpace(request.Compression))
	if compression == "" || compression == "none" {
		compression = "identity"
	}
	config := exportpkg.FileWriterConfig{Schema: schema, Compression: compression}
	switch format {
	case "csv":
		config.Format = exportpkg.FormatCSV
		delimiter, quote := request.Delimiter, request.Quote
		if delimiter == 0 {
			delimiter = ','
		}
		if quote == 0 {
			quote = '"'
		}
		if delimiter == quote {
			return exportpkg.FileWriterConfig{}, errors.New("session: CSV delimiter and quote must differ")
		}
		config.CSV = exportpkg.CSVWriteOptions{Header: request.Header, Delimiter: delimiter, Quote: quote}
	case "json":
		config.Format = exportpkg.FormatJSON
	case "ndjson":
		config.Format = exportpkg.FormatNDJSON
	case "parquet":
		config.Format = exportpkg.FormatParquet
	default:
		return exportpkg.FileWriterConfig{}, fmt.Errorf("session: unsupported COPY format %q", request.Format)
	}
	return config, nil
}

func fingerprintText(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

type copyExportSink struct {
	dataset  *exportpkg.DatasetWriter
	manifest *exportpkg.Manifest
	store    exportpkg.ManifestStore
	commit   *exportpkg.SingleFileCommitter
	rows     int64
	terminal bool
}

func (s *copyExportSink) Consume(ctx context.Context, chunk *vector.DataChunk) error {
	if s == nil || s.dataset == nil || s.terminal {
		return errors.New("session: COPY sink is closed")
	}
	if err := s.dataset.WriteChunk(ctx, chunk, ""); err != nil {
		return err
	}
	s.rows += int64(chunk.Cardinality)
	return nil
}

func (s *copyExportSink) Finalize(ctx context.Context) error {
	if s == nil || s.dataset == nil || s.manifest == nil || s.store == nil || s.commit == nil || s.terminal {
		return errors.New("session: COPY sink is closed")
	}
	if err := s.dataset.Close(ctx); err != nil {
		return err
	}
	if err := s.store.Save(ctx, s.manifest); err != nil {
		return fmt.Errorf("session: persist COPY complete-file manifest: %w", err)
	}
	if err := s.commit.Commit(ctx); err != nil {
		return err
	}
	s.terminal = true
	return nil
}

func (s *copyExportSink) Close() error {
	if s == nil || s.terminal {
		return nil
	}
	s.terminal = true
	var errs []error
	if s.dataset != nil {
		if err := s.dataset.Abort(context.Background()); err != nil {
			errs = append(errs, err)
		}
	}
	if s.manifest != nil && s.manifest.ExportState == exportpkg.ExportStateWriting {
		if err := s.manifest.TransitionExport(exportpkg.ExportStateFailed); err != nil {
			errs = append(errs, err)
		}
	}
	if s.store != nil && s.manifest != nil {
		if err := s.store.Save(context.Background(), s.manifest); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func zeroKeyBytes(value []byte) {
	for i := range value {
		value[i] = 0
	}
}

var _ pipeline.CloseableSink = (*copyExportSink)(nil)

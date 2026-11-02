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

// CopyManifestStoreFactory creates durable recovery state for one COPY job.
// The returned store is control/recovery state only; it is not a public output
// sidecar and must remain separate from query temporary spill storage.
type CopyManifestStoreFactory func(*query.Context, string) (exportpkg.ManifestStore, error)

// ExportCopyExecutorConfig binds Stable COPY to the existing Export/VFS stack.
type ExportCopyExecutorConfig struct {
	Registry             *vfs.Registry
	Buffers              *memory.BufferManager
	ManifestStore        CopyManifestStoreFactory
	MaxWriters           int
	PerWriterReservation int64
}

// ExportCopyExecutor is the production Stable COPY adapter. It does not parse,
// bind or plan SQL and it never opens a protocol driver outside VFS.
type ExportCopyExecutor struct {
	config ExportCopyExecutorConfig
}

func NewExportCopyExecutor(config ExportCopyExecutorConfig) (*ExportCopyExecutor, error) {
	if config.Registry == nil || config.Buffers == nil || config.ManifestStore == nil {
		return nil, errors.New("session: COPY export executor requires VFS registry, BufferManager and ManifestStore factory")
	}
	if config.MaxWriters <= 0 {
		config.MaxWriters = 1
	}
	// Stable SQL COPY is single-file in v1; accepting a larger pool would only
	// reserve unused concurrency and obscure the one-file ownership contract.
	if config.MaxWriters != 1 {
		return nil, fmt.Errorf("session: Stable COPY requires exactly one writer slot")
	}
	if config.PerWriterReservation <= 0 {
		config.PerWriterReservation = 64 << 10
	}
	return &ExportCopyExecutor{config: config}, nil
}

func (e *ExportCopyExecutor) Run(q *query.Context, bound *binder.BoundCopyTo, prepared *PreparedSQLQuery, sql string) error {
	if e == nil || e.config.Registry == nil || e.config.Buffers == nil || e.config.ManifestStore == nil {
		return errors.New("session: COPY export executor is uninitialized")
	}
	if q == nil || q.Memory == nil || q.Closed() || bound == nil || prepared == nil {
		return errors.New("session: COPY requires live query, bound statement and prepared query")
	}
	if err := q.Context().Err(); err != nil {
		return err
	}
	snapshot, ok := q.SourceSnapshot()
	if !ok || strings.TrimSpace(snapshot.Fingerprint) == "" {
		return errors.New("session: COPY refuses to start without frozen Query SourceSnapshot")
	}

	root, filePath, err := copyDestination(bound.Destination)
	if err != nil {
		return err
	}
	finalURI := root
	finalURI.Path = pathpkg.Join(root.Path, filePath)
	finalURI.Raw = finalURI.String()
	caps, err := e.config.Registry.EffectiveCapabilities(q.Context(), finalURI)
	if err != nil {
		return err
	}
	if !caps.Has(vfs.AtomicPublishWrite) {
		return fmt.Errorf("session: COPY destination %s does not provide AtomicPublishWrite", finalURI.Scheme)
	}

	schema, err := copyExportSchema(prepared)
	if err != nil {
		return err
	}
	writerConfig, err := copyWriterConfig(bound.Options, schema)
	if err != nil {
		return err
	}
	queryFingerprint := fingerprintText(sql)
	jobID := fingerprintText(queryFingerprint + "\x00" + snapshot.Fingerprint + "\x00" + finalURI.String())
	manifest, err := exportpkg.NewManifest(
		jobID,
		queryFingerprint,
		snapshot.Fingerprint,
		root.String(),
		string(writerConfig.Format),
		writerConfig.Compression,
		"atomic_publish",
		[]string{filePath},
	)
	if err != nil {
		return err
	}
	if err := manifest.TransitionExport(exportpkg.ExportStateWriting); err != nil {
		return err
	}
	store, err := e.config.ManifestStore(q, jobID)
	if err != nil {
		return err
	}
	if store == nil {
		return errors.New("session: COPY ManifestStore factory returned nil")
	}
	// Persist intent before opening any final-object writer. A crash before the
	// next durable snapshot therefore restarts by redoing the one incomplete file.
	if err := store.Save(q.Context(), manifest); err != nil {
		return fmt.Errorf("session: persist COPY WRITING manifest: %w", err)
	}

	pool, err := exportpkg.NewWriterPool(e.config.MaxWriters, e.config.PerWriterReservation, q.Memory)
	if err != nil {
		return err
	}
	factory, err := exportpkg.NewFileWriterFactory(writerConfig, e.config.Registry, q.Memory, e.config.Buffers, pool)
	if err != nil {
		return err
	}
	router, err := exportpkg.NewFileRouter(exportpkg.RoutingConfig{Mode: exportpkg.LayoutSingleFile, SingleFilePath: filePath}, schema)
	if err != nil {
		return err
	}
	dataset, err := exportpkg.NewAtomicSingleFileDatasetWriter(manifest, router, factory)
	if err != nil {
		return err
	}
	commit := &exportpkg.SingleFileCommitter{Manifest: manifest, Store: store, Registry: e.config.Registry}
	copySink := &copyExportSink{dataset: dataset, manifest: manifest, store: store, commit: commit}
	if err := prepared.Run(q, copySink); err != nil {
		return err
	}
	if pool.Active() != 0 {
		return fmt.Errorf("session: COPY writer ownership leak: active=%d", pool.Active())
	}
	return nil
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

func fingerprintText(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

// copyExportSink is the Push Pipeline sink for Stable COPY. It consumes native
// DataChunks directly and owns only the existing DatasetWriter/Manifest lifecycle.
type copyExportSink struct {
	dataset  *exportpkg.DatasetWriter
	manifest *exportpkg.Manifest
	store    exportpkg.ManifestStore
	commit   *exportpkg.SingleFileCommitter
	terminal bool
}

func (s *copyExportSink) Consume(ctx context.Context, chunk *vector.DataChunk) error {
	if s == nil || s.dataset == nil || s.terminal {
		return errors.New("session: COPY sink is closed")
	}
	return s.dataset.WriteChunk(ctx, chunk, "")
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

// Close is invoked by pipeline failure paths. It aborts any incomplete writer,
// persists FAILED where possible, and never deletes/replaces a previously
// complete final object itself; atomic VFS ownership remains below DatasetWriter.
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

var _ pipeline.CloseableSink = (*copyExportSink)(nil)

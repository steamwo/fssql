package session

import (
	"context"
	"errors"

	"github.com/steamwo/fssql/internal/core/vector"
	exportpkg "github.com/steamwo/fssql/internal/export"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func (e *ExportCopyExecutor) runStaged(q *query.Context, prepared *PreparedSQLQuery, dest vfs.URI, mode vfs.PublishMode, driver vfs.StagedPublishDriver, factory *exportpkg.FileWriterFactory, config exportpkg.FileWriterConfig, sql, source string) (CopyExportResult, error) {
	job, err := exportpkg.NewJobID()
	if err != nil {
		return CopyExportResult{}, err
	}
	intent, err := driver.PlanPublish(q.Context(), dest, string(job), mode)
	if err != nil {
		return CopyExportResult{}, err
	}
	m, err := exportpkg.NewPublishManifest(string(job), fingerprintText(sql), source, string(config.Format), config.Compression, intent)
	if err != nil {
		return CopyExportResult{}, err
	}
	m.Codec = config.Compression
	if config.Parquet.Encryption != nil {
		m.KeyID = config.Parquet.Encryption.KeyID
		m.EncryptionAlgorithm = config.Parquet.Encryption.Algorithm
	}
	coordinator := exportpkg.PublishCoordinator{ManifestDir: e.config.ManifestDir, Registry: e.config.Registry, Scope: q.Memory, Buffers: e.config.Buffers, Store: func(job string) (exportpkg.ManifestStore, error) { return e.config.ManifestStore(q, job) }}
	var rows int64
	result, err := coordinator.Run(q.Context(), m, func(ctx context.Context, base exportpkg.ByteSink) error {
		writer, err := factory.OpenStaged(ctx, base)
		if err != nil {
			return err
		}
		sink := &stagedCopySink{writer: writer}
		err = prepared.Run(q, sink)
		rows = sink.rows
		return errors.Join(err, sink.Close())
	})
	if err != nil {
		return CopyExportResult{}, err
	}
	return CopyExportResult{JobID: result.JobID, Rows: rows, Bytes: result.Bytes, Schema: append([]exportpkg.Column(nil), config.Schema...)}, nil
}

type stagedCopySink struct {
	writer *exportpkg.PooledWriter
	rows   int64
	closed bool
}

func (s *stagedCopySink) Consume(ctx context.Context, chunk *vector.DataChunk) error {
	if s.closed {
		return exportpkg.ErrWriterClosed
	}
	if err := s.writer.WriteChunk(ctx, chunk); err != nil {
		return err
	}
	s.rows += int64(chunk.Cardinality)
	return nil
}
func (s *stagedCopySink) Finalize(ctx context.Context) error {
	if s.closed {
		return exportpkg.ErrWriterClosed
	}
	s.closed = true
	_, err := s.writer.Close(ctx)
	return err
}
func (s *stagedCopySink) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.writer.Abort(context.Background())
}

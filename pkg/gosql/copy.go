package gosql

import (
	"context"
	"errors"

	"github.com/steamwo/fssql/internal/session"
)

// ParquetKeyResolver resolves one export key from request context and public key identity.
// Implementations should return exactly 16 bytes for AES-128-GCM or 32 bytes for AES-256-GCM.
type ParquetKeyResolver func(context.Context, string, string) ([]byte, error)

// ParquetReadKeyResolver resolves a footer key by the ID stored in the file.
// AES_GCM_V1 stores no key size, so the resolver selects the key by ID.
type ParquetReadKeyResolver func(ctx context.Context, keyID string) ([]byte, error)

type CopyFormat string

const (
	CopyFormatCSV     CopyFormat = "csv"
	CopyFormatJSON    CopyFormat = "json"
	CopyFormatNDJSON  CopyFormat = "ndjson"
	CopyFormatParquet CopyFormat = "parquet"
)

const (
	ParquetAES128GCM = "AES-128-GCM"
	ParquetAES256GCM = "AES-256-GCM"
)

// ParquetEncryptionOptions enables encrypted-footer Parquet Modular Encryption.
type ParquetEncryptionOptions struct {
	Algorithm string
	KeyID     string
}

// CopyOptions configures DB.CopyTo. Compression is whole-file for text formats,
// and the internal page codec (identity/snappy) for Parquet. Empty Compression
// defaults to snappy for Parquet and identity for text formats.
type CopyOptions struct {
	// PublishMode defaults to strict. Strict never downgrades automatically.
	PublishMode CopyPublishMode
	Format      CopyFormat
	Compression string
	// ParquetRowGroupRows defaults to 2048 when zero; only valid for Parquet.
	ParquetRowGroupRows int
	// ParquetPageRows defaults to min(512, row-group rows) when zero.
	// It must not exceed the effective row-group size; only valid for Parquet.
	ParquetPageRows int
	// ForceQuote quotes every non-NULL CSV field, including column names.
	ForceQuote bool
	// DuckDBCompatible uses DuckDB BLOB and UTC timestamp text in CSV output.
	// The default preserves the original native CSV representation.
	DuckDBCompatible bool
	// Header defaults to true for CSV, matching SQL COPY.
	Header     *bool
	Delimiter  byte
	Quote      byte
	Encryption *ParquetEncryptionOptions
}

// ExportResult is returned only after the final output object has been committed.
type ExportResult struct {
	JobID   string
	Rows    int64
	Bytes   int64
	Columns []Column
}

// CopyTo binds query once and sends its native chunks through the same Export/VFS
// atomic commit path used by SQL COPY. Failure returns a zero result.
func (db *DB) CopyTo(ctx context.Context, querySQL, destination string, options CopyOptions) (ExportResult, error) {
	q, prepared, columns, err := db.prepare(ctx, querySQL)
	if err != nil {
		return ExportResult{}, err
	}
	header := true
	if options.Header != nil {
		header = *options.Header
	}
	request := session.CopyRequest{
		PublishMode:         string(options.PublishMode),
		Destination:         destination,
		Format:              string(options.Format),
		Compression:         options.Compression,
		ParquetRowGroupRows: options.ParquetRowGroupRows,
		ParquetPageRows:     options.ParquetPageRows,
		Header:              header,
		Delimiter:           options.Delimiter,
		Quote:               options.Quote,
		ForceQuote:          options.ForceQuote,
		DuckDBCompatible:    options.DuckDBCompatible,
	}
	if options.Encryption != nil {
		request.EncryptionAlgorithm = options.Encryption.Algorithm
		request.KeyID = options.Encryption.KeyID
	}
	result, runErr := db.engine.CopyToPrepared(q, prepared, request, querySQL)
	closeErr := q.Close()
	if err := errors.Join(runErr, closeErr); err != nil {
		return ExportResult{}, err
	}
	return ExportResult{JobID: result.JobID, Rows: result.Rows, Bytes: result.Bytes, Columns: append([]Column(nil), columns...)}, nil
}

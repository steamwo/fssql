package parquet

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

var (
	ErrInvalidMagic  = errors.New("parquet: invalid magic")
	ErrInvalidFooter = errors.New("parquet: invalid footer")
)

const DefaultMaxFooterBytes int64 = 64 << 20

type FooterSource struct {
	URI      vfs.URI
	Size     int64
	Identity vfs.ObjectIdentity
}

type FooterMetrics struct {
	Requests         int64
	PlannedBytes     int64
	TransferredBytes int64
	UsefulBytes      int64
}

type Footer struct {
	Metadata       FileMetaData
	Schema         *SchemaTree
	MetadataOffset int64
	MetadataLength int64
	Metrics        FooterMetrics
}

func DiscoverFooter(ctx context.Context, driver vfs.Driver, src FooterSource, scope *memory.Scope, bm *memory.BufferManager, maxFooterBytes int64) (Footer, error) {
	var out Footer
	if driver == nil || scope == nil || bm == nil {
		return out, errors.New("parquet: footer discovery requires driver and managed memory")
	}
	if src.Size < 12 {
		return out, fmt.Errorf("%w: file size %d", ErrInvalidFooter, src.Size)
	}
	if maxFooterBytes <= 0 {
		maxFooterBytes = DefaultMaxFooterBytes
	}
	identity := src.Identity
	trailerReq := vfs.RangeRequest{URI: src.URI, Offset: src.Size - 8, Length: 8, Expected: &identity, Tag: memory.TagParquetRange}
	trailer, err := driver.ReadRange(ctx, trailerReq, scope, bm)
	if err != nil {
		return out, err
	}
	out.Metrics.Requests++
	out.Metrics.PlannedBytes += 8
	out.Metrics.TransferredBytes += 8
	out.Metrics.UsefulBytes += 8
	if trailer.Buffer == nil || trailer.Buffer.Len() != 8 {
		if trailer.Buffer != nil {
			trailer.Buffer.Release()
		}
		return out, fmt.Errorf("%w: trailer length", ErrInvalidFooter)
	}
	b := trailer.Buffer.Bytes()
	if string(b[4:8]) != "PAR1" {
		trailer.Buffer.Release()
		return out, ErrInvalidMagic
	}
	metaLen := int64(binary.LittleEndian.Uint32(b[:4]))
	trailer.Buffer.Release()
	if metaLen <= 0 || metaLen > maxFooterBytes || metaLen > src.Size-12 {
		return out, fmt.Errorf("%w: metadata length %d", ErrInvalidFooter, metaLen)
	}
	metaOffset := src.Size - 8 - metaLen
	if metaOffset < 4 {
		return out, fmt.Errorf("%w: metadata offset %d", ErrInvalidFooter, metaOffset)
	}
	metaReq := vfs.RangeRequest{URI: src.URI, Offset: metaOffset, Length: metaLen, Expected: &identity, Tag: memory.TagMetadata}
	meta, err := driver.ReadRange(ctx, metaReq, scope, bm)
	if err != nil {
		return out, err
	}
	out.Metrics.Requests++
	out.Metrics.PlannedBytes += metaLen
	out.Metrics.TransferredBytes += metaLen
	out.Metrics.UsefulBytes += metaLen
	if meta.Buffer == nil || int64(meta.Buffer.Len()) != metaLen {
		if meta.Buffer != nil {
			meta.Buffer.Release()
		}
		return out, fmt.Errorf("%w: metadata short read", ErrInvalidFooter)
	}
	decoded, err := DecodeFileMetaData(meta.Buffer.Bytes())
	meta.Buffer.Release()
	if err != nil {
		return out, err
	}
	tree, err := BuildSchemaTree(decoded.Schema)
	if err != nil {
		return out, err
	}
	out.Metadata = decoded
	out.Schema = tree
	out.MetadataOffset = metaOffset
	out.MetadataLength = metaLen
	return out, nil
}

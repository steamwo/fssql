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

// DiscoverFooter 复用可选文件句柄完成 trailer 与元数据读取；所有范围仍保持身份校验。
func DiscoverFooter(ctx context.Context, driver vfs.Driver, src FooterSource, scope *memory.Scope, bm *memory.BufferManager, maxFooterBytes int64) (Footer, error) {
	return discoverFooter(ctx, driver, src, scope, bm, maxFooterBytes, nil)
}

func discoverFooter(ctx context.Context, driver vfs.Driver, src FooterSource, scope *memory.Scope, bm *memory.BufferManager, maxFooterBytes int64, keys ReadKeyResolver) (Footer, error) {
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
	read := driver.ReadRange
	if opener, ok := driver.(vfs.RangeReaderOpener); ok {
		reader, err := opener.OpenRangeReader(ctx, src.URI, &identity)
		if err != nil {
			return out, err
		}
		defer reader.Close()
		read = reader.ReadRange
	}
	trailerReq := vfs.RangeRequest{URI: src.URI, Offset: src.Size - 8, Length: 8, Expected: &identity, Tag: memory.TagParquetRange}
	trailer, err := vfs.ReadInputRange(ctx, read, trailerReq, scope, bm)
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
	magic := string(b[4:8])
	metaLen := int64(binary.LittleEndian.Uint32(b[:4]))
	trailer.Buffer.Release()
	switch magic {
	case "PAR1":
	case "PARE":
		if keys == nil {
			return out, errEncryptedKeyRequired
		}
		head, err := vfs.ReadInputRange(ctx, read, vfs.RangeRequest{URI: src.URI, Offset: 0, Length: 4, Expected: &identity, Tag: memory.TagParquetRange}, scope, bm)
		if err != nil {
			return out, err
		}
		if head.Buffer == nil {
			return out, ErrInvalidMagic
		}
		valid := string(head.Buffer.Bytes()) == "PARE"
		head.Buffer.Release()
		if !valid {
			return out, ErrInvalidMagic
		}
		out.Metrics.Requests++
		out.Metrics.PlannedBytes += 4
		out.Metrics.TransferredBytes += 4
		out.Metrics.UsefulBytes += 4
	default:
		return out, ErrInvalidMagic
	}
	if metaLen <= 0 || metaLen > maxFooterBytes || metaLen > src.Size-12 {
		return out, fmt.Errorf("%w: metadata length %d", ErrInvalidFooter, metaLen)
	}
	metaOffset := src.Size - 8 - metaLen
	if metaOffset < 4 {
		return out, fmt.Errorf("%w: metadata offset %d", ErrInvalidFooter, metaOffset)
	}
	metaReq := vfs.RangeRequest{URI: src.URI, Offset: metaOffset, Length: metaLen, Expected: &identity, Tag: memory.TagMetadata}
	meta, err := vfs.ReadInputRange(ctx, read, metaReq, scope, bm)
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
	var decoded FileMetaData
	if magic == "PARE" {
		decoded, err = decodeEncryptedFooter(ctx, meta.Buffer.Bytes(), keys)
	} else {
		decoded, err = DecodeFileMetaData(meta.Buffer.Bytes())
	}
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

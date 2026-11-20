package parquet

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/steamwo/fssql/internal/core/memory"
)

func (c *ColumnCursor) readEncryptedPageHeader(ctx context.Context, remaining int64) (PageHeader, int, error) {
	dictionary := c.meta.DictionaryPageOffset != nil && c.offset == *c.meta.DictionaryPageOffset
	if !dictionary && c.pageOrdinal > math.MaxInt16 {
		return PageHeader{}, 0, errors.New("parquet: encrypted data page ordinal exceeds int16")
	}
	if remaining < encryptedOverhead {
		return PageHeader{}, 0, errors.New("parquet: truncated encrypted page header")
	}
	prefix, err := c.readRange(ctx, c.offset, 4, memory.TagParquetRange)
	if err != nil {
		return PageHeader{}, 0, err
	}
	length := int64(binary.LittleEndian.Uint32(prefix.Bytes()))
	prefix.Release()
	if length < encryptedNonceSize+encryptedTagSize || length > remaining-4 || length > maxPageHeaderProbe+encryptedNonceSize+encryptedTagSize {
		return PageHeader{}, 0, errors.New("parquet: invalid encrypted page header length")
	}
	buffer, err := c.readRange(ctx, c.offset+4, length, memory.TagParquetRange)
	if err != nil {
		return PageHeader{}, 0, err
	}
	defer buffer.Release()
	crypto := c.meta.crypto
	aad, size := crypto.pageAAD(dictionary, true, c.pageOrdinal)
	plain, err := crypto.file.openPayload(buffer.Bytes(), aad[:size])
	if err != nil {
		return PageHeader{}, 0, fmt.Errorf("parquet: decrypt page header: %w", err)
	}
	header, used, err := DecodePageHeader(plain)
	if err != nil {
		return PageHeader{}, 0, fmt.Errorf("parquet: decode encrypted page header: %w", err)
	}
	if used != len(plain) || header.CompressedPageSize < encryptedOverhead ||
		(dictionary && header.Type != PageDictionary) ||
		(!dictionary && header.Type != PageData && header.Type != PageDataV2) {
		return PageHeader{}, 0, errors.New("parquet: invalid encrypted page header")
	}
	return c.cachePageHeader(header, int(length)+4)
}

func (c *ColumnCursor) readDecodedPageBody(ctx context.Context, offset int64, header PageHeader) (*memory.Buffer, error) {
	length := int64(header.CompressedPageSize)
	crypto := c.meta.crypto
	if crypto == nil {
		return c.readPageBody(ctx, offset, length)
	}
	body, err := c.readRange(ctx, offset, length, memory.TagParquetCompressed)
	if err != nil {
		return nil, err
	}
	aad, size := crypto.pageAAD(header.Type == PageDictionary, false, c.pageOrdinal)
	if _, err := crypto.file.openFrame(body.Bytes(), aad[:size]); err != nil {
		body.Release()
		return nil, fmt.Errorf("parquet: decrypt page body: %w", err)
	}
	return body, nil
}

func (c *ColumnCursor) pagePayloadSize(header PageHeader) int {
	size := int(header.CompressedPageSize)
	if c.meta.crypto != nil {
		size -= encryptedOverhead
	}
	return size
}

func (c *ColumnCursor) pagePayload(body *memory.Buffer, header PageHeader) []byte {
	start := 0
	if c.meta.crypto != nil {
		start = 4 + encryptedNonceSize
	}
	return body.Bytes()[start : start+c.pagePayloadSize(header)]
}

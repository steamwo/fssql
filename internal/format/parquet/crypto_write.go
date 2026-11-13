package parquet

import "errors"

// EncodeFileCryptoMetaData encodes the plaintext FileCryptoMetaData prefix used by
// encrypted-footer Parquet files. The algorithm is AES_GCM_V1 and aad_file_unique
// carries the 8-byte per-file unique identifier.
func EncodeFileCryptoMetaData(fileUnique, keyMetadata []byte) ([]byte, error) {
	if len(fileUnique) != 8 {
		return nil, errors.New("parquet: encrypted footer file unique AAD must be 8 bytes")
	}
	w := &compactEncoder{}
	last := int16(0)
	last = w.fieldHeader(last, 1, ctStruct) // encryption_algorithm
	algLast := int16(0)
	algLast = w.fieldHeader(algLast, 1, ctStruct) // AES_GCM_V1
	aesLast := int16(0)
	aesLast = w.fieldHeader(aesLast, 2, ctBinary) // aad_file_unique
	w.binary(fileUnique)
	_ = aesLast
	w.stop() // AesGcmV1
	_ = algLast
	w.stop() // EncryptionAlgorithm union
	if len(keyMetadata) > 0 {
		last = w.fieldHeader(last, 2, ctBinary)
		w.binary(keyMetadata)
	}
	_ = last
	w.stop()
	return append([]byte(nil), w.buf...), nil
}

// EncodeEncryptedFileMetaData encodes FileMetaData for an encrypted-footer file.
// ColumnChunk.crypto_metadata is stamped ENCRYPTION_WITH_FOOTER_KEY while the
// ColumnMetaData itself remains inside the encrypted footer payload.
func EncodeEncryptedFileMetaData(meta FileMetaData) ([]byte, error) {
	if len(meta.Schema) == 0 {
		return nil, errors.New("parquet: metadata missing schema")
	}
	if meta.NumRows < 0 {
		return nil, errors.New("parquet: negative row count")
	}
	w := &compactEncoder{}
	if err := w.encryptedFileMeta(meta); err != nil {
		return nil, err
	}
	return append([]byte(nil), w.buf...), nil
}

func (w *compactEncoder) encryptedFileMeta(meta FileMetaData) error {
	last := int16(0)
	last = w.fieldHeader(last, 1, ctI32)
	w.i32(meta.Version)
	last = w.fieldHeader(last, 2, ctList)
	if err := w.listHeader(ctStruct, len(meta.Schema)); err != nil {
		return err
	}
	for i := range meta.Schema {
		if err := w.schemaElement(meta.Schema[i]); err != nil {
			return err
		}
	}
	last = w.fieldHeader(last, 3, ctI64)
	w.i64(meta.NumRows)
	if len(meta.RowGroups) > 0 {
		last = w.fieldHeader(last, 4, ctList)
		if err := w.listHeader(ctStruct, len(meta.RowGroups)); err != nil {
			return err
		}
		for i := range meta.RowGroups {
			if err := w.encryptedRowGroup(meta.RowGroups[i]); err != nil {
				return err
			}
		}
	}
	if len(meta.KeyValueMetadata) > 0 {
		last = w.fieldHeader(last, 5, ctList)
		if err := w.listHeader(ctStruct, len(meta.KeyValueMetadata)); err != nil {
			return err
		}
		for i := range meta.KeyValueMetadata {
			w.keyValue(meta.KeyValueMetadata[i])
		}
	}
	if meta.CreatedBy != "" {
		last = w.fieldHeader(last, 6, ctBinary)
		w.binary([]byte(meta.CreatedBy))
	}
	if len(meta.ColumnOrders) > 0 {
		last = w.fieldHeader(last, 7, ctList)
		if err := w.columnOrders(meta.ColumnOrders); err != nil {
			return err
		}
	}
	_ = last
	w.stop()
	return nil
}

func (w *compactEncoder) encryptedRowGroup(group RowGroup) error {
	if group.NumRows < 0 || group.TotalByteSize < 0 {
		return errors.New("parquet: invalid row group sizes")
	}
	last := int16(0)
	last = w.fieldHeader(last, 1, ctList)
	if err := w.listHeader(ctStruct, len(group.Columns)); err != nil {
		return err
	}
	for i := range group.Columns {
		if err := w.encryptedColumnChunk(group.Columns[i]); err != nil {
			return err
		}
	}
	last = w.fieldHeader(last, 2, ctI64)
	w.i64(group.TotalByteSize)
	last = w.fieldHeader(last, 3, ctI64)
	w.i64(group.NumRows)
	if group.FileOffset != nil {
		last = w.fieldHeader(last, 5, ctI64)
		w.i64(*group.FileOffset)
	}
	if group.TotalCompressedSize != nil {
		last = w.fieldHeader(last, 6, ctI64)
		w.i64(*group.TotalCompressedSize)
	}
	if group.Ordinal != nil {
		last = w.fieldHeader(last, 7, ctI16)
		w.i16(*group.Ordinal)
	}
	_ = last
	w.stop()
	return nil
}

func (w *compactEncoder) encryptedColumnChunk(chunk ColumnChunk) error {
	if chunk.MetaData == nil {
		return errors.New("parquet: column chunk missing metadata")
	}
	last := int16(0)
	if chunk.FilePath != "" {
		last = w.fieldHeader(last, 1, ctBinary)
		w.binary([]byte(chunk.FilePath))
	}
	last = w.fieldHeader(last, 2, ctI64)
	w.i64(chunk.FileOffset)
	last = w.fieldHeader(last, 3, ctStruct)
	if err := w.columnMeta(*chunk.MetaData); err != nil {
		return err
	}
	if chunk.OffsetIndexOffset != nil {
		last = w.fieldHeader(last, 4, ctI64)
		w.i64(*chunk.OffsetIndexOffset)
	}
	if chunk.OffsetIndexLength != nil {
		last = w.fieldHeader(last, 5, ctI32)
		w.i32(*chunk.OffsetIndexLength)
	}
	if chunk.ColumnIndexOffset != nil {
		last = w.fieldHeader(last, 6, ctI64)
		w.i64(*chunk.ColumnIndexOffset)
	}
	if chunk.ColumnIndexLength != nil {
		last = w.fieldHeader(last, 7, ctI32)
		w.i32(*chunk.ColumnIndexLength)
	}
	last = w.fieldHeader(last, 8, ctStruct) // crypto_metadata
	cryptoLast := int16(0)
	cryptoLast = w.fieldHeader(cryptoLast, 1, ctStruct) // ENCRYPTION_WITH_FOOTER_KEY
	_ = cryptoLast
	w.stop() // EncryptionWithFooterKey empty struct
	w.stop() // ColumnCryptoMetaData union
	_ = last
	w.stop()
	return nil
}

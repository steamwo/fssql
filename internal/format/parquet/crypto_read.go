package parquet

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
)

// ReadKeyResolver supplies the raw footer key. AES_GCM_V1 does not encode the
// key size, so the key ID must identify the right key.
type ReadKeyResolver func(context.Context, string) ([]byte, error)

var errEncryptedKeyRequired = errors.New("parquet: encrypted footer requires Config.ParquetReadKeys")

const (
	encryptedNonceSize = 12
	encryptedTagSize   = 16
	encryptedOverhead  = 4 + encryptedNonceSize + encryptedTagSize
)

type fileDecryption struct {
	gcm    cipher.AEAD
	unique [8]byte
}

type columnDecryption struct {
	file     *fileDecryption
	rowGroup uint16
	column   uint16
}

func decodeEncryptedFooter(ctx context.Context, data []byte, keys ReadKeyResolver) (FileMetaData, error) {
	r := newCompactReader(data)
	file, keyID, err := r.fileCryptoMetadata()
	if err != nil {
		return FileMetaData{}, fmt.Errorf("parquet: decode file crypto metadata: %w", err)
	}
	key, err := keys(ctx, keyID)
	if err != nil {
		return FileMetaData{}, fmt.Errorf("parquet: resolve footer key %q: %w", keyID, err)
	}
	if len(key) != 16 && len(key) != 32 {
		return FileMetaData{}, fmt.Errorf("parquet: footer key %q must be 16 or 32 bytes, got %d", keyID, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return FileMetaData{}, err
	}
	file.gcm, err = cipher.NewGCM(block)
	if err != nil {
		return FileMetaData{}, err
	}

	var aad [9]byte // The footer module ID is zero.
	copy(aad[:], file.unique[:])
	plain, err := file.openFrame(data[r.off:], aad[:])
	if err != nil {
		return FileMetaData{}, fmt.Errorf("parquet: decrypt footer: %w", err)
	}
	defer clear(plain)
	footerReader := newCompactReader(plain)
	meta, err := footerReader.fileMeta()
	if err != nil {
		return FileMetaData{}, err
	}
	if footerReader.remaining() != 0 {
		return FileMetaData{}, fmt.Errorf("%w: trailing encrypted footer data", ErrInvalidFooter)
	}
	for i, group := range meta.RowGroups {
		if i > math.MaxInt16 || (group.Ordinal != nil && int(*group.Ordinal) != i) {
			return FileMetaData{}, fmt.Errorf("%w: invalid encrypted row group ordinal %d", ErrInvalidFooter, i)
		}
		for j, chunk := range group.Columns {
			if j > math.MaxInt16 || !chunk.usesFooterKey || chunk.MetaData == nil || chunk.FilePath != "" {
				return FileMetaData{}, fmt.Errorf("parquet: unsupported encryption for row group %d column %d: expected an in-file column using the footer key", i, j)
			}
			chunk.MetaData.crypto = &columnDecryption{file: &file, rowGroup: uint16(i), column: uint16(j)}
		}
	}
	return meta, nil
}

func (r *compactReader) fileCryptoMetadata() (fileDecryption, string, error) {
	var file fileDecryption
	var haveAlgorithm bool
	var keyID string
	var last int16
	for {
		field, next, err := r.field(last)
		if err != nil {
			return file, "", err
		}
		last = next
		if field.typ == ctStop {
			break
		}
		switch {
		case field.id == 1 && field.typ == ctStruct:
			if err := r.readCryptoAlgorithm(&file); err != nil {
				return file, "", err
			}
			haveAlgorithm = true
		case field.id == 2 && field.typ == ctBinary:
			data, err := r.binary()
			if err != nil {
				return file, "", err
			}
			var metadata struct {
				Direct     bool   `json:"duckdbDirectKey"`
				FooterKey  string `json:"parquet.encryption.footer.key"`
				WrappedKey string `json:"wrappedDEK"`
			}
			if err := json.Unmarshal(data, &metadata); err != nil || !metadata.Direct || metadata.WrappedKey != "" {
				return file, "", errors.New("unsupported footer key metadata: expected a direct footer key")
			}
			keyID = metadata.FooterKey
		default:
			if err := r.skip(field.typ, 1); err != nil {
				return file, "", err
			}
		}
	}
	if !haveAlgorithm || keyID == "" {
		return file, "", errors.New("missing encryption algorithm or footer key ID")
	}
	return file, keyID, nil
}

func (r *compactReader) readCryptoAlgorithm(file *fileDecryption) error {
	algorithm, _, err := r.field(0)
	if err != nil {
		return err
	}
	if algorithm.id != 1 || algorithm.typ != ctStruct {
		return errors.New("unsupported encryption algorithm: expected AES_GCM_V1")
	}
	var haveUnique bool
	var last int16
	for {
		field, next, err := r.field(last)
		if err != nil {
			return err
		}
		last = next
		if field.typ == ctStop {
			break
		}
		switch {
		case field.id == 2 && field.typ == ctBinary:
			unique, err := r.binary()
			if err != nil {
				return err
			}
			if len(unique) != len(file.unique) {
				return errors.New("invalid file unique AAD length")
			}
			copy(file.unique[:], unique)
			haveUnique = true
		case field.id == 3 && field.typ == ctBoolFalse:
			// The optional supply_aad_prefix flag defaults to false.
		default:
			return errors.New("unsupported encryption AAD: expected only the file unique suffix")
		}
	}
	if !haveUnique {
		return errors.New("missing file unique AAD")
	}
	return r.expectCryptoStop()
}

func (r *compactReader) expectCryptoStop() error {
	b, err := r.byte()
	if err != nil {
		return err
	}
	if b != ctStop {
		return errors.New("parquet: invalid crypto metadata union")
	}
	return nil
}

func (f *fileDecryption) openFrame(frame, aad []byte) ([]byte, error) {
	if len(frame) < encryptedOverhead || int64(binary.LittleEndian.Uint32(frame)) != int64(len(frame)-4) {
		return nil, errors.New("invalid encrypted module length")
	}
	return f.openPayload(frame[4:], aad)
}

func (f *fileDecryption) openPayload(payload, aad []byte) ([]byte, error) {
	nonce := payload[:encryptedNonceSize]
	sealed := payload[encryptedNonceSize:]
	plain, err := f.gcm.Open(sealed[:0], nonce, sealed, aad)
	if err != nil {
		return nil, errors.New("encrypted module authentication failed")
	}
	return plain, nil
}

func (c *columnDecryption) pageAAD(dictionary, header bool, ordinal int) ([15]byte, int) {
	const (
		dataPage             = 2
		dictionaryPage       = 3
		dataPageHeader       = 4
		dictionaryPageHeader = 5
	)
	var aad [15]byte
	copy(aad[:], c.file.unique[:])
	switch {
	case dictionary && header:
		aad[8] = dictionaryPageHeader
	case dictionary:
		aad[8] = dictionaryPage
	case header:
		aad[8] = dataPageHeader
	default:
		aad[8] = dataPage
	}
	binary.LittleEndian.PutUint16(aad[9:11], c.rowGroup)
	binary.LittleEndian.PutUint16(aad[11:13], c.column)
	if dictionary {
		return aad, 13
	}
	binary.LittleEndian.PutUint16(aad[13:15], uint16(ordinal))
	return aad, 15
}

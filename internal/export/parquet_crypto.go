package export

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	ParquetAES128GCM = "AES-128-GCM"
	ParquetAES256GCM = "AES-256-GCM"

	parquetCryptoLengthBytes = 4
	parquetCryptoNonceBytes  = 12
	parquetCryptoTagBytes    = 16
	parquetUniqueFileIDBytes = 8

	parquetModuleFooter         = int8(0)
	parquetModuleDataPage       = int8(2)
	parquetModuleDataPageHeader = int8(4)
)

// ParquetEncryptionConfig is the per-export native Parquet Modular Encryption state.
// Key is deliberately not serializable and must never be copied into a manifest.
type ParquetEncryptionConfig struct {
	Algorithm   string
	KeyID       string
	Key         []byte
	KeyMetadata string
}

// NormalizeParquetEncryptionAlgorithm returns the canonical public algorithm name.
func NormalizeParquetEncryptionAlgorithm(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "aes-128-gcm", "aes_128_gcm", "aes128-gcm", "aes128gcm":
		return ParquetAES128GCM, nil
	case "aes-256-gcm", "aes_256_gcm", "aes256-gcm", "aes256gcm":
		return ParquetAES256GCM, nil
	default:
		return "", fmt.Errorf("export: unsupported Parquet encryption algorithm %q", value)
	}
}

// ValidateParquetEncryption performs every validation that can fail without opening an output object.
func ValidateParquetEncryption(config *ParquetEncryptionConfig) error {
	if config == nil {
		return nil
	}
	algorithm, err := NormalizeParquetEncryptionAlgorithm(config.Algorithm)
	if err != nil {
		return err
	}
	config.Algorithm = algorithm
	config.KeyID = strings.TrimSpace(config.KeyID)
	if config.KeyID == "" {
		return errors.New("export: Parquet encryption keyId is required")
	}
	want := 16
	if algorithm == ParquetAES256GCM {
		want = 32
	}
	if len(config.Key) != want {
		return fmt.Errorf("export: %s requires a %d-byte key, got %d", algorithm, want, len(config.Key))
	}
	if config.KeyMetadata == "" {
		metadata, err := directParquetKeyMetadata(config.KeyID)
		if err != nil {
			return err
		}
		config.KeyMetadata = metadata
	}
	return nil
}

type directParquetKeyMaterial struct {
	KeyMaterialType string `json:"keyMaterialType"`
	InternalStorage bool   `json:"internalStorage"`
	IsFooterKey     bool   `json:"isFooterKey"`
	KMSInstanceID   string `json:"kmsInstanceID"`
	KMSInstanceURL  string `json:"kmsInstanceURL"`
	MasterKeyID     string `json:"masterKeyID"`
	FooterKeyID     string `json:"parquet.encryption.footer.key"`
	WrappedDEK      string `json:"wrappedDEK"`
	DoubleWrapping  bool   `json:"doubleWrapping"`
	DuckDBDirectKey bool   `json:"duckdbDirectKey"`
}

func directParquetKeyMetadata(keyID string) (string, error) {
	encoded, err := json.Marshal(directParquetKeyMaterial{
		KeyMaterialType: "PKMT1",
		InternalStorage: true,
		IsFooterKey:     true,
		KMSInstanceID:   "DEFAULT",
		KMSInstanceURL:  "DEFAULT",
		MasterKeyID:     keyID,
		FooterKeyID:     keyID,
		WrappedDEK:      "",
		DoubleWrapping:  false,
		DuckDBDirectKey: true,
	})
	if err != nil {
		return "", fmt.Errorf("export: encode Parquet key metadata: %w", err)
	}
	return string(encoded), nil
}

func parquetEncryptedFrameSize(plain int) (int, error) {
	if plain < 0 || plain > int(^uint32(0))-parquetCryptoNonceBytes-parquetCryptoTagBytes {
		return 0, errors.New("export: Parquet encrypted module exceeds uint32 framing")
	}
	return parquetCryptoLengthBytes + parquetCryptoNonceBytes + plain + parquetCryptoTagBytes, nil
}

// sealParquetModule writes the exact DuckDB/Parquet Modular Encryption transport frame:
// uint32 little-endian (nonce+ciphertext+tag), nonce, ciphertext, tag.
func sealParquetModule(dst []byte, config *ParquetEncryptionConfig, fileAAD []byte, module int8, rowGroup, column, page int16, plain []byte) ([]byte, error) {
	if err := ValidateParquetEncryption(config); err != nil {
		return nil, err
	}
	if len(fileAAD) != parquetUniqueFileIDBytes {
		return nil, errors.New("export: Parquet encryption file AAD must be 8 bytes")
	}
	required, err := parquetEncryptedFrameSize(len(plain))
	if err != nil {
		return nil, err
	}
	if len(dst) < required {
		return nil, fmt.Errorf("export: Parquet encrypted module buffer %d < required %d", len(dst), required)
	}
	block, err := aes.NewCipher(config.Key)
	if err != nil {
		return nil, fmt.Errorf("export: initialize Parquet AES: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("export: initialize Parquet GCM: %w", err)
	}
	if gcm.NonceSize() != parquetCryptoNonceBytes || gcm.Overhead() != parquetCryptoTagBytes {
		return nil, errors.New("export: unexpected AES-GCM nonce/tag size")
	}
	payloadLength := parquetCryptoNonceBytes + len(plain) + parquetCryptoTagBytes
	binary.LittleEndian.PutUint32(dst[:4], uint32(payloadLength))
	nonce := dst[4 : 4+parquetCryptoNonceBytes]
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("export: generate Parquet encryption nonce: %w", err)
	}
	aad := parquetModuleAAD(fileAAD, module, rowGroup, column, page)
	sealed := gcm.Seal(dst[4+parquetCryptoNonceBytes:4+parquetCryptoNonceBytes], nonce, plain, aad)
	return dst[:4+parquetCryptoNonceBytes+len(sealed)], nil
}

func parquetModuleAAD(fileAAD []byte, module int8, rowGroup, column, page int16) []byte {
	out := make([]byte, 0, parquetUniqueFileIDBytes+7)
	out = append(out, fileAAD...)
	out = append(out, byte(module))
	if rowGroup < 0 {
		return out
	}
	var scratch [2]byte
	binary.LittleEndian.PutUint16(scratch[:], uint16(rowGroup))
	out = append(out, scratch[:]...)
	if column < 0 {
		return out
	}
	binary.LittleEndian.PutUint16(scratch[:], uint16(column))
	out = append(out, scratch[:]...)
	if page < 0 {
		return out
	}
	binary.LittleEndian.PutUint16(scratch[:], uint16(page))
	return append(out, scratch[:]...)
}

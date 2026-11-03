package export

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"strings"
	"testing"
)

func TestParquetEncryptionValidationAndFrame(t *testing.T) {
	config := &ParquetEncryptionConfig{Algorithm: "aes-128-gcm", KeyID: "master", Key: bytes.Repeat([]byte{7}, 16)}
	if err := ValidateParquetEncryption(config); err != nil {
		t.Fatal(err)
	}
	if config.Algorithm != ParquetAES128GCM || !strings.Contains(config.KeyMetadata, `"masterKeyID":"master"`) || !strings.Contains(config.KeyMetadata, `"duckdbDirectKey":true`) {
		t.Fatalf("unexpected normalized config: %+v", config)
	}
	plain := []byte("phase6")
	size, err := parquetEncryptedFrameSize(len(plain))
	if err != nil {
		t.Fatal(err)
	}
	frame, err := sealParquetModule(make([]byte, size), config, []byte("12345678"), parquetModuleDataPage, 2, 3, 4, plain)
	if err != nil {
		t.Fatal(err)
	}
	if got := int(binary.LittleEndian.Uint32(frame[:4])); got != len(frame)-4 {
		t.Fatalf("frame length=%d want=%d", got, len(frame)-4)
	}
	block, _ := aes.NewCipher(config.Key)
	gcm, _ := cipher.NewGCM(block)
	decoded, err := gcm.Open(nil, frame[4:16], frame[16:], parquetModuleAAD([]byte("12345678"), parquetModuleDataPage, 2, 3, 4))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, plain) {
		t.Fatalf("decoded=%q", decoded)
	}
}

func TestParquetEncryptionRejectsWrongKeyLength(t *testing.T) {
	config := &ParquetEncryptionConfig{Algorithm: ParquetAES256GCM, KeyID: "master", Key: make([]byte, 16)}
	if err := ValidateParquetEncryption(config); err == nil {
		t.Fatal("expected key length error")
	}
}

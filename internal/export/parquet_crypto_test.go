package export

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
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

func TestParquetEncryptionWriterClearsOwnedKey(t *testing.T) {
	for _, mode := range []string{"close", "abort", "failure"} {
		t.Run(mode, func(t *testing.T) {
			key := bytes.Repeat([]byte{7}, 16)
			scope := memory.NewManager(1 << 20).NewScope()
			defer scope.Close()
			w, err := NewParquetWriter([]Column{{Name: "id", Type: types.LogicalType{ID: types.BigInt}}}, ParquetWriteOptions{Encryption: &ParquetEncryptionConfig{Key: key, KeyID: "master", Algorithm: ParquetAES128GCM}}, scope, memory.NewBufferManager(nil), &captureSink{short: mode == "failure"})
			if err != nil {
				t.Fatal(err)
			}
			owned := w.opts.Encryption.Key
			if &owned[0] == &key[0] {
				t.Fatal("writer borrowed caller key")
			}
			if mode == "abort" {
				err = w.Abort(context.Background())
			} else {
				_, err = w.Close(context.Background())
			}
			if (err != nil) != (mode == "failure") {
				t.Fatalf("unexpected outcome: %v", err)
			}
			if !bytes.Equal(owned, make([]byte, 16)) || !bytes.Equal(key, bytes.Repeat([]byte{7}, 16)) || scope.Used() != 0 {
				t.Fatal("key ownership or memory cleanup failed")
			}
		})
	}
}

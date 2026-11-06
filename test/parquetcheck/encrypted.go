// Package parquetcheck independently inspects the encrypted writer's artifacts.
// It deliberately imports neither the writer nor its AAD/Thrift helpers.
package parquetcheck

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/golang/snappy"
)

type Report struct {
	KeyID                  string
	Codec                  int64
	Groups, Pages, Modules int
	Values                 [][]any
	FileAAD                []byte
}

// Encrypted checks the two-column BIGINT/VARCHAR test fixture, including NULLs,
// all encrypted modules, footer-key metadata, compression and tamper rejection.
// AAD follows Parquet Modular Encryption 4.4.2; ordinals are little-endian i16.
func Encrypted(t testing.TB, data, key []byte, keyID string, codec int64) Report {
	t.Helper()
	if len(data) < 12 || string(data[:4]) != "PARE" || string(data[len(data)-4:]) != "PARE" {
		t.Fatal("missing encrypted-footer envelope")
	}
	footerStart := len(data) - 8 - int(binary.LittleEndian.Uint32(data[len(data)-8:]))
	if footerStart < 4 {
		t.Fatal("invalid footer length")
	}
	r := compact{t: t, data: data[footerStart : len(data)-8]}
	crypto := r.object()
	if len(crypto) != 2 {
		t.Fatal("unexpected plaintext crypto metadata fields")
	}
	algorithm := object(t, crypto[1])
	if len(algorithm) != 1 {
		t.Fatal("unexpected algorithm union")
	}
	gcmMeta := object(t, algorithm[1])
	if len(gcmMeta) != 1 {
		t.Fatal("unexpected AES_GCM_V1 fields")
	}
	fileAAD := binaryValue(t, gcmMeta[2])
	if len(fileAAD) != 8 {
		t.Fatal("file unique AAD must contain 8 bytes")
	}
	var metadata map[string]any
	if err := json.Unmarshal(binaryValue(t, crypto[2]), &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata["masterKeyID"] != keyID || metadata["parquet.encryption.footer.key"] != keyID || metadata["duckdbDirectKey"] != true || metadata["wrappedDEK"] != "" {
		t.Fatal("unexpected key metadata")
	}
	if bytes.Contains(data[footerStart:footerStart+r.off], key) {
		t.Fatal("raw key in plaintext metadata")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	report := Report{KeyID: keyID, Codec: codec, FileAAD: append([]byte(nil), fileAAD...)}
	nonces := map[string]bool{}
	open := func(offset int, module byte, group, column, page int) ([]byte, int) {
		t.Helper()
		if offset < 4 || offset+4 > len(data)-8 {
			t.Fatal("module offset out of bounds")
		}
		n := 4 + int(binary.LittleEndian.Uint32(data[offset:]))
		if n < 32 || offset+n > len(data)-8 {
			t.Fatal("module length out of bounds")
		}
		frame := data[offset : offset+n]
		nonce := frame[4:16]
		if nonces[string(nonce)] {
			t.Fatal("reused module nonce")
		}
		nonces[string(nonce)] = true
		aad := append(append([]byte(nil), fileAAD...), module)
		if module != 0 {
			for _, ordinal := range []int{group, column, page} {
				aad = append(aad, byte(ordinal), byte(ordinal>>8))
			}
		}
		plain, err := gcm.Open(nil, nonce, frame[16:], aad)
		if err != nil {
			t.Fatalf("standard AAD rejected module=%d group=%d column=%d page=%d: %v", module, group, column, page, err)
		}
		// Authentication must cover every component of module identity.
		for i := range aad {
			wrong := append([]byte(nil), aad...)
			wrong[i] ^= 1
			if _, err := gcm.Open(nil, nonce, frame[16:], wrong); err == nil {
				t.Fatal("incorrect AAD authenticated")
			}
		}
		badKey := append([]byte(nil), key...)
		badKey[0] ^= 1
		badBlock, _ := aes.NewCipher(badKey)
		badGCM, _ := cipher.NewGCM(badBlock)
		if _, err := badGCM.Open(nil, nonce, frame[16:], aad); err == nil {
			t.Fatal("wrong key authenticated")
		}
		for _, index := range []int{4, 16, len(frame) - 1} {
			bad := append([]byte(nil), frame...)
			bad[index] ^= 1
			if _, err := gcm.Open(nil, bad[4:16], bad[16:], aad); err == nil {
				t.Fatal("tampered module authenticated")
			}
		}
		report.Modules++
		return plain, n
	}
	footer, footerSize := open(footerStart+r.off, 0, 0, 0, 0)
	if footerStart+r.off+footerSize != len(data)-8 {
		t.Fatal("footer envelope length mismatch")
	}
	f := compact{t: t, data: footer}
	meta := f.object()
	if f.off != len(footer) {
		t.Fatal("trailing footer bytes")
	}
	schema := list(t, meta[2])
	if len(schema) != 3 || integer(t, object(t, schema[1])[1]) != 2 || integer(t, object(t, schema[2])[1]) != 6 {
		t.Fatal("unexpected test schema")
	}
	groups, _ := meta[4].([]any)
	report.Groups = len(groups)
	var columns [2][]any
	nextOffset := 4
	for gi, group := range groups {
		g := object(t, group)
		chunks := list(t, g[1])
		if len(chunks) != 2 {
			t.Fatal("column count mismatch")
		}
		for ci, chunk := range chunks {
			c := object(t, chunk)
			crypto := object(t, c[8])
			if len(crypto) != 1 || len(object(t, crypto[1])) != 0 {
				t.Fatal("column must use footer key")
			}
			m := object(t, c[3])
			if integer(t, m[4]) != codec || integer(t, m[5]) != integer(t, g[3]) {
				t.Fatal("codec/value count mismatch")
			}
			offset := int(integer(t, m[9]))
			start := offset
			if offset != nextOffset {
				t.Fatal("unaccounted or overlapping page modules")
			}
			var count int64
			for pi := 0; count < integer(t, m[5]); pi++ {
				header, n := open(offset, 4, gi, ci, pi)
				offset += n
				hr := compact{t: t, data: header}
				h := hr.object()
				if hr.off != len(header) || integer(t, h[1]) != 0 {
					t.Fatal("invalid data page header")
				}
				dh := object(t, h[5])
				if integer(t, dh[2]) != 0 || integer(t, dh[3]) != 3 || integer(t, dh[4]) != 3 {
					t.Fatal("expected PLAIN/RLE data page")
				}
				body, n := open(offset, 2, gi, ci, pi)
				offset += n
				if n != int(integer(t, h[3])) {
					t.Fatal("encrypted compressed page size mismatch")
				}
				if codec == 1 {
					body, err = snappy.Decode(nil, body)
					if err != nil {
						t.Fatal(err)
					}
				}
				if len(body) != int(integer(t, h[2])) {
					t.Fatal("uncompressed page size mismatch")
				}
				values := pageValues(t, body, int(integer(t, dh[1])), ci)
				columns[ci] = append(columns[ci], values...)
				count += int64(len(values))
				report.Pages++
			}
			if count != integer(t, g[3]) || int64(offset-start) != integer(t, m[7]) {
				t.Fatal("column size/count mismatch")
			}
			nextOffset = offset
		}
	}
	if nextOffset != footerStart || len(columns[0]) != int(integer(t, meta[3])) || len(columns[0]) != len(columns[1]) {
		t.Fatal("file row/byte coverage mismatch")
	}
	for i := range columns[0] {
		report.Values = append(report.Values, []any{columns[0][i], columns[1][i]})
	}
	return report
}

func pageValues(t testing.TB, body []byte, count, col int) []any {
	t.Helper()
	if len(body) < 4 {
		t.Fatal("truncated definition levels")
	}
	n := int(binary.LittleEndian.Uint32(body))
	if n+4 > len(body) {
		t.Fatal("invalid definition levels length")
	}
	levels, values := body[4:4+n], body[4+n:]
	var valid []bool
	for len(levels) > 0 && len(valid) < count {
		h, used := binary.Uvarint(levels)
		if used <= 0 {
			t.Fatal("invalid RLE header")
		}
		levels = levels[used:]
		if h&1 == 0 {
			if len(levels) < 1 {
				t.Fatal("truncated RLE value")
			}
			for i := uint64(0); i < h>>1; i++ {
				valid = append(valid, levels[0] != 0)
			}
			levels = levels[1:]
		} else {
			n := int(h >> 1)
			if n > len(levels) {
				t.Fatal("truncated bit-packed levels")
			}
			for _, b := range levels[:n] {
				for j := 0; j < 8; j++ {
					valid = append(valid, b&(1<<j) != 0)
				}
			}
			levels = levels[n:]
		}
	}
	if len(valid) < count {
		t.Fatal("too few definition levels")
	}
	out := make([]any, count)
	for i := range out {
		if !valid[i] {
			continue
		}
		if col == 0 {
			if len(values) < 8 {
				t.Fatal("truncated INT64")
			}
			out[i] = int64(binary.LittleEndian.Uint64(values))
			values = values[8:]
		} else {
			if len(values) < 4 {
				t.Fatal("truncated BYTE_ARRAY")
			}
			n := int(binary.LittleEndian.Uint32(values))
			values = values[4:]
			if n > len(values) {
				t.Fatal("invalid BYTE_ARRAY length")
			}
			out[i] = string(values[:n])
			values = values[n:]
		}
	}
	if len(values) != 0 {
		t.Fatal("trailing PLAIN values")
	}
	return out
}

// A small independent Thrift compact decoder. Numeric keys are Parquet thrift
// field IDs; it shares no code with the production encoder or decoder.
type compact struct {
	t    testing.TB
	data []byte
	off  int
}

func (r *compact) take(n int) []byte {
	if n < 0 || n > len(r.data)-r.off {
		r.t.Fatal("truncated compact value")
	}
	b := r.data[r.off : r.off+n]
	r.off += n
	return b
}
func (r *compact) uint() uint64 {
	n, used := binary.Uvarint(r.data[r.off:])
	if used <= 0 {
		r.t.Fatal("invalid compact varint")
	}
	r.off += used
	return n
}
func (r *compact) signed() int64 { u := r.uint(); return int64(u>>1) ^ -int64(u&1) }
func (r *compact) object() map[int]any {
	out := map[int]any{}
	last := 0
	for {
		h := r.take(1)[0]
		if h == 0 {
			return out
		}
		id := last + int(h>>4)
		if h>>4 == 0 {
			id = int(r.signed())
		}
		if _, ok := out[id]; ok {
			r.t.Fatal("duplicate compact field")
		}
		out[id] = r.value(h & 15)
		last = id
	}
}
func (r *compact) value(kind byte) any {
	switch kind {
	case 1:
		return true
	case 2:
		return false
	case 3:
		return int64(int8(r.take(1)[0]))
	case 4, 5, 6:
		return r.signed()
	case 7:
		return r.take(8)
	case 8:
		return r.take(int(r.uint()))
	case 9, 10:
		h := r.take(1)[0]
		n := int(h >> 4)
		if n == 15 {
			n = int(r.uint())
		}
		out := make([]any, n)
		for i := range out {
			out[i] = r.value(h & 15)
		}
		return out
	case 12:
		return r.object()
	default:
		r.t.Fatal(fmt.Sprintf("unsupported compact type %d", kind))
		return nil
	}
}
func object(t testing.TB, v any) map[int]any {
	t.Helper()
	x, ok := v.(map[int]any)
	if !ok {
		t.Fatal("expected compact struct")
	}
	return x
}
func list(t testing.TB, v any) []any {
	t.Helper()
	x, ok := v.([]any)
	if !ok {
		t.Fatal("expected compact list")
	}
	return x
}
func integer(t testing.TB, v any) int64 {
	t.Helper()
	x, ok := v.(int64)
	if !ok {
		t.Fatal("expected compact integer")
	}
	return x
}
func binaryValue(t testing.TB, v any) []byte {
	t.Helper()
	x, ok := v.([]byte)
	if !ok {
		t.Fatal("expected compact binary")
	}
	return x
}

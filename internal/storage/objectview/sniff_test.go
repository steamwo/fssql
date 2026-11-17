package objectview

import "testing"

func FuzzCompressionMagic(f *testing.F) {
	for _, seed := range [][]byte{{}, {0x1f, 0x8b}, {0x28, 0xb5, 0x2f, 0xfd}, {0x50, 0x2a, 0x4d, 0x18}, []byte("plain")} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		kind := sniffCompression(data)
		if kind != "none" && kind != "gzip" && kind != "zstd" {
			t.Fatal("invalid compression kind")
		}
		if len(data) > 4 && kind != sniffCompression(data[:4]) {
			t.Fatal("sniff read beyond fixed prefix")
		}
	})
}

package objectview_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/objectview"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func gzipData(t *testing.T, data []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	w := gzip.NewWriter(&out)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}
func zstdData(data []byte) []byte {
	out := []byte{0x28, 0xb5, 0x2f, 0xfd, 0xa0}
	var size [4]byte
	binary.LittleEndian.PutUint32(size[:], uint32(len(data)))
	out = append(out, size[:]...)
	for {
		n := min(len(data), 128<<10)
		header := uint32(n) << 3
		if n == len(data) {
			header |= 1
		}
		out = append(out, byte(header), byte(header>>8), byte(header>>16))
		out = append(out, data[:n]...)
		data = data[n:]
		if len(data) == 0 {
			break
		}
	}
	return out
}
func skipFrame(data []byte) []byte {
	out := []byte{0x50, 0x2a, 0x4d, 0x18}
	var size [4]byte
	binary.LittleEndian.PutUint32(size[:], uint32(len(data)))
	out = append(out, size[:]...)
	return append(out, data...)
}

type protocolStream struct {
	*streamDriver
	scheme string
}

func (d *protocolStream) Scheme() string { return d.scheme }

func logicalBytes(t *testing.T, qctx context.Context, obj vfs.LogicalObject, scope *memory.Scope, bm *memory.BufferManager) []byte {
	t.Helper()
	if obj.Size == 0 {
		return nil
	}
	result, err := obj.Reader.ReadRange(qctx, vfs.RangeRequest{URI: obj.URI, Length: obj.Size, Expected: &obj.Identity}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Buffer.Release()
	return append([]byte(nil), result.Buffer.Bytes()...)
}

func TestCompressionViewsAcrossProtocols(t *testing.T) {
	plain := []byte("id\n1\n2\n")
	for _, scheme := range []string{"file", "s3", "ftp", "ftps", "sftp", "webdav", "webdavs", "http", "https"} {
		for _, kind := range []string{"gzip", "zstd"} {
			for _, mode := range []string{"auto", kind} {
				t.Run(fmt.Sprintf("%s/%s/%s", scheme, kind, mode), func(t *testing.T) {
					data := gzipData(t, plain)
					if kind == "zstd" {
						data = zstdData(plain)
					}
					d := &protocolStream{streamDriver: &streamDriver{data: string(data)}, scheme: scheme}
					q, r, m, bm := setup(t, d, 32<<20, 1<<20)
					obj, err := r.Resolve(q.Context(), uri(t, scheme+"://example.test/no-suffix"), vfs.ObjectViewSpec{FileCompression: mode})
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(logicalBytes(t, q.Context(), obj, q.Memory, bm), plain) || obj.Identity.SnapshotID == "" || obj.Identity.ETag != "" || m.Used() != 0 || q.Temp.OwnedFiles() != 1 || q.Temp.Used() != int64(len(plain)) {
						t.Fatal("compression view/ownership mismatch")
					}
				})
			}
		}
	}
}

func TestConcatenatedMembersFramesAndSkippablePrefixes(t *testing.T) {
	first, second := []byte("first\n"), []byte("second\n")
	gz := append(gzipData(t, first), gzipData(t, second)...)
	zs := append(zstdData(first), zstdData(second)...)
	prefix := append(skipFrame([]byte("ignored")), skipFrame(bytes.Repeat([]byte{1}, 150000))...)
	for name, data := range map[string][]byte{"gzip": gz, "zstd": zs, "skips": append(prefix, zs...)} {
		t.Run(name, func(t *testing.T) {
			q, r, _, bm := setup(t, &streamDriver{data: string(data)}, 32<<20, 1<<20)
			obj, err := r.Resolve(q.Context(), uri(t, "https://example.test/data"), vfs.ObjectViewSpec{FileCompression: "auto"})
			if err != nil {
				t.Fatal(err)
			}
			if string(logicalBytes(t, q.Context(), obj, q.Memory, bm)) != "first\nsecond\n" {
				t.Fatal("concatenation lost data")
			}
		})
	}
}

func TestCompressedViewFailureCleanup(t *testing.T) {
	plain := bytes.Repeat([]byte("payload\n"), 20000)
	gz := gzipData(t, plain)
	badGzip := append([]byte(nil), gz...)
	badGzip[len(badGzip)-8] ^= 1
	zs, _ := hex.DecodeString("28b52ffd240f790000312c616c7068610a322c626574610acb559942")
	badZstd := append([]byte(nil), zs...)
	badZstd[len(badZstd)-1] ^= 1
	for _, tc := range []struct {
		name, mode string
		data       []byte
		mem, temp  int64
		want       error
	}{
		{"gzip checksum", "auto", badGzip, 32 << 20, 1 << 20, gzip.ErrChecksum},
		{"gzip truncated", "gzip", gz[:len(gz)-3], 32 << 20, 1 << 20, io.ErrUnexpectedEOF},
		{"zstd checksum", "auto", badZstd, 32 << 20, 1 << 20, nil},
		{"zstd truncated", "zstd", zs[:len(zs)-2], 32 << 20, 1 << 20, io.ErrUnexpectedEOF},
		{"bad skip", "auto", []byte{0x50, 0x2a, 0x4d, 0x18, 0xff, 0xff, 0xff, 0xff}, 32 << 20, 1 << 20, io.ErrUnexpectedEOF},
		{"gzip mismatch", "gzip", []byte("plain"), 32 << 20, 1 << 20, objectview.ErrCompressionMismatch},
		{"zstd mismatch", "zstd", gz, 32 << 20, 1 << 20, objectview.ErrCompressionMismatch},
		{"memory", "auto", gz, 100000, 1 << 20, memory.ErrLimitExceeded},
		{"bomb quota", "auto", gz, 32 << 20, 100, storagetemp.ErrQuotaExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, r, m, _ := setup(t, &streamDriver{data: string(tc.data)}, tc.mem, tc.temp)
			obj, err := r.Resolve(q.Context(), uri(t, "https://example.test/data"), vfs.ObjectViewSpec{FileCompression: tc.mode})
			if err == nil || tc.want != nil && !errors.Is(err, tc.want) || obj.Identity.SnapshotID != "" {
				t.Fatalf("invalid result: %+v %v", obj, err)
			}
			if q.Temp.OwnedFiles() != 0 || q.Temp.Used() != 0 || m.Used() != 0 {
				t.Fatal("failed transform retained resources")
			}
		})
	}
}

func TestZstdWindowReservedBeforeAllocation(t *testing.T) {
	// Non-single-segment frame declares an 8 MiB window but emits one byte.
	data := []byte{0x28, 0xb5, 0x2f, 0xfd, 0, 0x68, 9, 0, 0, 'x'}
	for _, limit := range []int64{2 << 20, 16 << 20} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			q, r, m, bm := setup(t, &streamDriver{data: string(data)}, limit, 1<<20)
			obj, err := r.Resolve(q.Context(), uri(t, "https://example.test/data"), vfs.ObjectViewSpec{FileCompression: "zstd"})
			if limit == 2<<20 {
				if !errors.Is(err, memory.ErrLimitExceeded) {
					t.Fatal(err)
				}
				if m.Peak() >= limit {
					t.Fatal("window allocated before reservation")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if string(logicalBytes(t, q.Context(), obj, q.Memory, bm)) != "x" || m.Peak() < 9<<20 {
					t.Fatal("zstd window not accounted")
				}
			}
			if m.Used() != 0 {
				t.Fatal("decoder workspace retained")
			}
		})
	}
}

func TestCompressionViewSharingAndNoneIsolation(t *testing.T) {
	plain := []byte("plain data\n")
	data := gzipData(t, plain)
	d := &streamDriver{data: string(data)}
	q, r, _, bm := setup(t, d, 32<<20, 1<<20)
	u := uri(t, "https://example.test/data")
	objects := make([]vfs.LogicalObject, 10)
	errs := make([]error, 10)
	var wg sync.WaitGroup
	for i := range objects {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			objects[i], errs[i] = r.Resolve(q.Context(), u, vfs.ObjectViewSpec{FileCompression: "auto"})
		}(i)
	}
	wg.Wait()
	for i, obj := range objects {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		if obj.URI.ViewID != objects[0].URI.ViewID {
			t.Fatal("duplicate transform")
		}
	}
	raw, err := r.Resolve(q.Context(), u, vfs.ObjectViewSpec{FileCompression: "none"})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(logicalBytes(t, q.Context(), raw, q.Memory, bm), data) || raw.URI.ViewID == objects[0].URI.ViewID || raw.URI.ViewKey == objects[0].URI.ViewKey || d.opens.Load() != 2 || q.Temp.OwnedFiles() != 2 {
		t.Fatal("none and auto views shared")
	}
}

func TestAutoPlainRandomReadDoesNotMaterialize(t *testing.T) {
	base := &streamDriver{data: "plain input"}
	d := &randomDriver{Driver: base, data: base.data}
	q, r, m, bm := setup(t, d, 2<<20, 1<<20)
	obj, err := r.Resolve(q.Context(), uri(t, "https://example.test/data.gz"), vfs.ObjectViewSpec{FileCompression: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	if obj.Identity.SnapshotID != "" || q.Temp.OwnedFiles() != 0 || d.reads.Load() != 1 || m.Used() != 0 {
		t.Fatal("plain auto view downloaded/materialized")
	}
	if string(logicalBytes(t, q.Context(), obj, q.Memory, bm)) != base.data {
		t.Fatal("sniff consumed input")
	}
}

type cancelReader struct {
	reader io.Reader
	cancel context.CancelFunc
	reads  int
}

func (r *cancelReader) Read(p []byte) (int, error) {
	r.reads++
	if r.reads == 4 {
		r.cancel()
	}
	return r.reader.Read(p)
}

type cancelStream struct {
	*streamDriver
	cancel context.CancelFunc
}

func (d *cancelStream) OpenRead(context.Context, vfs.URI, *vfs.ObjectIdentity) (io.ReadCloser, error) {
	return io.NopCloser(&cancelReader{reader: strings.NewReader(d.data), cancel: d.cancel}), nil
}
func TestCancellationWhileSkippingZstdPrefix(t *testing.T) {
	d := &cancelStream{streamDriver: &streamDriver{data: string(append(skipFrame(make([]byte, 1<<20)), zstdData([]byte("data"))...))}}
	q, r, m, _ := setup(t, d, 32<<20, 1<<20)
	d.cancel = q.Cancel
	_, err := r.Resolve(q.Context(), uri(t, "https://example.test/data"), vfs.ObjectViewSpec{FileCompression: "auto"})
	if !errors.Is(err, context.Canceled) || m.Used() != 0 || q.Temp.Used() != 0 || q.Temp.OwnedFiles() != 0 {
		t.Fatalf("cancellation leaked: %v", err)
	}
}

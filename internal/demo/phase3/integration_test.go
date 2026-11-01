package phase3

import (
	"bytes"
	"compress/gzip"
	"context"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/format/compression"
	csvformat "github.com/steamwo/fssql/internal/format/csv"
	"github.com/steamwo/fssql/internal/storage/remote/sftp"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func gzipFixture(t *testing.T, src []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	w := gzip.NewWriter(&out)
	if _, err := w.Write(src); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestCompressedCSVLogicalResultEqualsUncompressed(t *testing.T) {
	plain := []byte("1,alpha\n2,beta\n")
	manager := memory.NewManager(4 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	schema := []types.LogicalType{{ID: types.BigInt}, {ID: types.Varchar}}

	want, _, err := csvformat.DecodeChunk(plain, schema, csvformat.DecodeOptions{Final: true}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := (compression.Gzip{}).Decode(gzipFixture(t, plain), scope, bm)
	if err != nil {
		want.Release()
		t.Fatal(err)
	}
	got, _, err := csvformat.DecodeChunk(decoded.Bytes(), schema, csvformat.DecodeOptions{Final: true}, scope, bm)
	decoded.Release()
	if err != nil {
		want.Release()
		t.Fatal(err)
	}
	for row := 0; row < want.Cardinality; row++ {
		wi, wok := want.Columns[0].Int64(row)
		gi, gok := got.Columns[0].Int64(row)
		ws, wsok := want.Columns[1].String(row)
		gs, gsok := got.Columns[1].String(row)
		if !wok || !gok || !wsok || !gsok || wi != gi || ws != gs {
			t.Fatalf("row %d mismatch", row)
		}
	}
	want.Release()
	got.Release()
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
}

type fixtureSFTPDialer struct{ data []byte }
type fixtureSFTPSession struct{ data []byte }

func (d fixtureSFTPDialer) Dial(ctx context.Context, cfg sftp.DialConfig) (sftp.DialResult, error) {
	if err := ctx.Err(); err != nil {
		return sftp.DialResult{}, err
	}
	return sftp.DialResult{Session: &fixtureSFTPSession{data: d.data}, HostKeySHA256: "SHA256:fixture"}, nil
}
func (s *fixtureSFTPSession) Stat(ctx context.Context, path string) (sftp.RemoteInfo, error) {
	if err := ctx.Err(); err != nil {
		return sftp.RemoteInfo{}, err
	}
	return sftp.RemoteInfo{Path: path, Size: int64(len(s.data)), Modified: time.Unix(1000, 0)}, nil
}
func (s *fixtureSFTPSession) List(context.Context, string, string, int) (sftp.RemotePage, error) {
	return sftp.RemotePage{Entries: []sftp.RemoteEntry{{Path: "data.csv", Size: int64(len(s.data)), Modified: time.Unix(1000, 0)}}}, nil
}
func (s *fixtureSFTPSession) ReadAt(ctx context.Context, path string, offset int64, dst []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if offset >= int64(len(s.data)) {
		return 0, nil
	}
	return copy(dst, s.data[offset:]), nil
}
func (s *fixtureSFTPSession) Close() error { return nil }

func TestSFTPVFSCSVQueryDemo(t *testing.T) {
	data := []byte("id,name\n1,alpha\n2,beta\n3,gamma\n")
	driver, err := sftp.NewDriver(sftp.Config{
		Endpoint: "sftp://host.example:22", CredentialScope: "fixture", SecurityProfile: "strict",
		HostKeySHA256: "SHA256:fixture", MaxSessions: 2, MaxRetries: 1,
	}, fixtureSFTPDialer{data: data})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewManager(8 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	binder, err := csvformat.NewSourceBinder(registry, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := binder.Bind(context.Background(), "sftp://host.example:22/data.csv", csvformat.SourceBindOptions{Inference: csvformat.InferenceOptions{Header: true, MaxRows: 100}})
	if err != nil {
		t.Fatal(err)
	}
	source, err := csvformat.NewSource(context.Background(), registry, binding, csvformat.ScanOptions{EnableParallel: true, TargetRangeBytes: 12}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for {
		chunk, ok, err := source.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		for row := 0; row < chunk.Cardinality; row++ {
			id, valid := chunk.Columns[0].Int64(row)
			if !valid {
				chunk.Release()
				t.Fatal("NULL id")
			}
			ids = append(ids, id)
		}
		chunk.Release()
	}
	if len(ids) != 3 || ids[0] != 1 || ids[1] != 2 || ids[2] != 3 {
		t.Fatalf("ids=%v", ids)
	}
	open, leased, _ := driver.PoolStats()
	if open > 2 || leased != 0 {
		t.Fatalf("pool open=%d leased=%d", open, leased)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
}

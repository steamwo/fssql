package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/format/compression"
	jsonformat "github.com/steamwo/fssql/internal/format/json"
	"github.com/steamwo/fssql/internal/storage/remote"
	"github.com/steamwo/fssql/internal/storage/remote/sftp"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type report struct {
	ManagedPeak      int64  `json:"managed_peak_bytes"`
	ManagedUsedAfter int64  `json:"managed_used_after_bytes"`
	GoHeapLive       uint64 `json:"go_heap_live_bytes"`
	GoTotalAllocated uint64 `json:"go_total_allocated_bytes"`
	GoHeapSys        uint64 `json:"go_heap_sys_bytes"`
	RSS              uint64 `json:"rss_bytes"`
	GCCycles         uint32 `json:"gc_cycles"`
	TempDiskPeak     int64  `json:"temp_disk_peak_bytes"`
	SFTPTotalAfter   int    `json:"sftp_total_sessions_after"`
	SFTPLeasedAfter  int    `json:"sftp_leased_sessions_after"`
	SFTPIdleAfter    int    `json:"sftp_idle_sessions_after"`
}

func rssBytes() uint64 {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(b))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * uint64(os.Getpagesize())
}

type fixtureSession struct{ data []byte }

func (s *fixtureSession) Stat(context.Context, string) (sftp.FileInfo, error) {
	return sftp.FileInfo{Size: int64(len(s.data)), ModUnix: 1234}, nil
}
func (s *fixtureSession) List(context.Context, string) ([]sftp.DirEntry, error) {
	return []sftp.DirEntry{{Name: "data.bin", Info: sftp.FileInfo{Size: int64(len(s.data)), ModUnix: 1234}}}, nil
}
func (s *fixtureSession) ReadAt(ctx context.Context, _ string, dst []byte, off int64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if off >= int64(len(s.data)) {
		return 0, io.EOF
	}
	n := copy(dst, s.data[off:])
	if n < len(dst) {
		return n, io.EOF
	}
	return n, nil
}
func (s *fixtureSession) Close() error { return nil }

func main() {
	mm := memory.NewManager(64 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)

	plain := bytes.Repeat([]byte("1234567890,abcdefghij\n"), 50000)
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	if _, err := zw.Write(plain); err != nil {
		panic(err)
	}
	if err := zw.Close(); err != nil {
		panic(err)
	}
	decoded, err := compression.Gzip().Decompress(context.Background(), gz.Bytes(), 8<<20, scope, bm)
	if err != nil {
		panic(err)
	}
	decoded.Release()

	records := make([][]byte, 2048)
	for i := range records {
		records[i] = []byte(`{"id":123,"name":"abcdefghijklmno"}`)
	}
	schema := []types.StructField{{Name: "id", Type: types.LogicalType{ID: types.BigInt}}, {Name: "name", Type: types.LogicalType{ID: types.Varchar}}}
	chunk, err := jsonformat.DecodeRecords(records, schema, jsonformat.ProjectedPathSet{}, scope, bm)
	if err != nil {
		panic(err)
	}
	chunk.Release()

	key := remote.ClientKey{Endpoint: "metrics.test:22", CredentialScope: "metrics", SecurityProfile: "strict"}
	mgr := sftp.NewManager(1, 2, sftp.SessionFactoryFunc(func(context.Context, remote.ClientKey) (sftp.Session, error) {
		return &fixtureSession{data: plain}, nil
	}))
	driver, err := sftp.NewDriver(key, mgr, 1)
	if err != nil {
		panic(err)
	}
	uri, err := vfs.ParseURI("sftp://metrics.test:22/data.bin")
	if err != nil {
		panic(err)
	}
	rr, err := driver.ReadRange(context.Background(), vfs.RangeRequest{URI: uri, Offset: 0, Length: 1 << 20}, scope, bm)
	if err != nil {
		panic(err)
	}
	rr.Buffer.Release()
	pool, err := mgr.Pool(key)
	if err != nil {
		panic(err)
	}
	_, leasedBeforeClose, _ := pool.Stats()
	if leasedBeforeClose != 0 {
		panic("SFTP lease remained before manager close")
	}
	if err := mgr.Close(); err != nil {
		panic(err)
	}
	total, leased, idle := pool.Stats()

	peak := mm.Peak()
	if scope.Used() != 0 {
		panic("managed memory leak")
	}
	if err := scope.Close(); err != nil {
		panic(err)
	}
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	out := report{ManagedPeak: peak, ManagedUsedAfter: mm.Used(), GoHeapLive: ms.HeapAlloc, GoTotalAllocated: ms.TotalAlloc, GoHeapSys: ms.HeapSys, RSS: rssBytes(), GCCycles: ms.NumGC, TempDiskPeak: 0, SFTPTotalAfter: total, SFTPLeasedAfter: leased, SFTPIdleAfter: idle}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		panic(err)
	}
}

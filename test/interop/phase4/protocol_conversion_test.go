package phase4

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/export"
	csvformat "github.com/steamwo/fssql/internal/format/csv"
	"github.com/steamwo/fssql/internal/storage/remote"
	"github.com/steamwo/fssql/internal/storage/remote/s3"
	"github.com/steamwo/fssql/internal/storage/remote/sftp"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// protocolS3Server 实现 interop 测试实际需要的 S3 Range Read 与 Multipart Write。
type protocolS3Server struct {
	mu sync.Mutex
	// objects 保存已提交对象，key 使用 bucket/object 形式。
	objects map[string][]byte
	// uploads 保存 uploadID -> partNumber -> payload。
	uploads map[string]map[int][]byte
	// uploadKeys 保存 uploadID 对应的目标对象。
	uploadKeys map[string]string
	// nextUpload 生成稳定测试 uploadID。
	nextUpload int
}

// handler 处理 S3 adapter 发出的真实 HTTP 请求。
func (s *protocolS3Server) handler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := strings.TrimPrefix(r.URL.Path, "/")
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodHead:
		payload, ok := s.objects[key]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		w.Header().Set("ETag", `"etag-`+strconv.Itoa(len(payload))+`"`)
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodGet && r.Header.Get("Range") != "":
		payload, ok := s.objects[key]
		if !ok {
			http.NotFound(w, r)
			return
		}
		var start, end int
		if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil || start < 0 || end < start || end >= len(payload) {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		body := payload[start : end+1]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("ETag", `"etag-`+strconv.Itoa(len(payload))+`"`)
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
	case r.Method == http.MethodPost && q.Has("uploads"):
		s.nextUpload++
		id := fmt.Sprintf("upload-%d", s.nextUpload)
		if s.uploads == nil {
			s.uploads = make(map[string]map[int][]byte)
			s.uploadKeys = make(map[string]string)
		}
		s.uploads[id] = make(map[int][]byte)
		s.uploadKeys[id] = key
		_, _ = fmt.Fprintf(w, `<InitiateMultipartUploadResult><UploadId>%s</UploadId></InitiateMultipartUploadResult>`, id)
	case r.Method == http.MethodPut && q.Get("uploadId") != "":
		id := q.Get("uploadId")
		part, err := strconv.Atoi(q.Get("partNumber"))
		if err != nil || s.uploads[id] == nil {
			http.Error(w, "bad upload", http.StatusBadRequest)
			return
		}
		payload, _ := io.ReadAll(r.Body)
		s.uploads[id][part] = append([]byte(nil), payload...)
		w.Header().Set("ETag", fmt.Sprintf(`"part-%d"`, part))
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodPost && q.Get("uploadId") != "":
		id := q.Get("uploadId")
		parts := s.uploads[id]
		if parts == nil {
			http.Error(w, "unknown upload", http.StatusBadRequest)
			return
		}
		indices := make([]int, 0, len(parts))
		for part := range parts {
			indices = append(indices, part)
		}
		sort.Ints(indices)
		var payload []byte
		for _, part := range indices {
			payload = append(payload, parts[part]...)
		}
		s.objects[s.uploadKeys[id]] = payload
		delete(s.uploads, id)
		delete(s.uploadKeys, id)
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodDelete && q.Get("uploadId") != "":
		id := q.Get("uploadId")
		delete(s.uploads, id)
		delete(s.uploadKeys, id)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "unexpected request", http.StatusBadRequest)
	}
}

// protocolSFTPStore 保存测试 SFTP endpoint 的共享对象状态。
type protocolSFTPStore struct {
	mu    sync.Mutex
	files map[string][]byte
}

// protocolSFTPSession 同时实现只读 Session 与 WritableSession。
type protocolSFTPSession struct {
	// store 在不同 pool lease 间共享。
	store *protocolSFTPStore
}

func (s *protocolSFTPSession) Stat(_ context.Context, path string) (sftp.FileInfo, error) {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	payload, ok := s.store.files[path]
	if !ok {
		return sftp.FileInfo{}, fmt.Errorf("not found: %s", path)
	}
	return sftp.FileInfo{Size: int64(len(payload)), ModUnix: 1234}, nil
}
func (*protocolSFTPSession) List(context.Context, string) ([]sftp.DirEntry, error) { return nil, nil }
func (s *protocolSFTPSession) ReadAt(_ context.Context, path string, dst []byte, off int64) (int, error) {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	payload, ok := s.store.files[path]
	if !ok {
		return 0, fmt.Errorf("not found: %s", path)
	}
	if off >= int64(len(payload)) {
		return 0, io.EOF
	}
	n := copy(dst, payload[off:])
	if n < len(dst) {
		return n, io.EOF
	}
	return n, nil
}
func (s *protocolSFTPSession) WriteAt(_ context.Context, path string, src []byte, off int64, truncate bool) (int, error) {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	if truncate {
		s.store.files[path] = nil
	}
	payload := s.store.files[path]
	need := int(off) + len(src)
	if need > len(payload) {
		next := make([]byte, need)
		copy(next, payload)
		payload = next
	}
	copy(payload[int(off):], src)
	s.store.files[path] = payload
	return len(src), nil
}
func (s *protocolSFTPSession) Remove(_ context.Context, path string) error {
	s.store.mu.Lock()
	delete(s.store.files, path)
	s.store.mu.Unlock()
	return nil
}
func (*protocolSFTPSession) Close() error { return nil }

// newProtocolSFTPDriver 创建 bounded endpoint-scoped SFTP driver 与可检查 lease 的 pool。
func newProtocolSFTPDriver(t *testing.T, store *protocolSFTPStore) (*sftp.Driver, *sftp.Manager, *sftp.Pool) {
	t.Helper()
	key := remote.ClientKey{Endpoint: "host:22", CredentialScope: "scope", CredentialVersion: "v1", SecurityProfile: "strict"}
	manager := sftp.NewManager(2, 2, sftp.SessionFactoryFunc(func(context.Context, remote.ClientKey) (sftp.Session, error) {
		return &protocolSFTPSession{store: store}, nil
	}))
	driver, err := sftp.NewDriver(key, manager, 2)
	if err != nil {
		_ = manager.Close()
		t.Fatal(err)
	}
	pool, err := manager.Pool(key)
	if err != nil {
		_ = manager.Close()
		t.Fatal(err)
	}
	return driver, manager, pool
}

// newProtocolS3Driver 创建实际经过 SigV4/HTTP adapter 的 S3 driver。
func newProtocolS3Driver(t *testing.T, state *protocolS3Server) (*s3.Driver, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(state.handler))
	driver, err := s3.NewDriver(s3.Config{
		Endpoint: server.URL, Region: "test-region", ForcePathStyle: true, AllowInsecureHTTP: true,
		CredentialScope: "scope", CredentialVersion: "v1", SecurityProfile: "strict", MaxRetries: 1,
	}, s3.Credentials{AccessKey: "access", SecretKey: "secret"}, nil)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	return driver, server
}

// protocolSchema 返回转换测试的固定 Own Vector schema。
func protocolSchema() ([]types.LogicalType, []export.Column) {
	typesOnly := []types.LogicalType{{ID: types.Integer}, {ID: types.Varchar}}
	return typesOnly, []export.Column{{Name: "id", Type: typesOnly[0]}, {Name: "name", Type: typesOnly[1]}}
}

// decodeRemoteCSV 使用真实 VFS driver 的 Stat/ReadRange 后直接解码到 Own DataChunk。
func decodeRemoteCSV(t *testing.T, driver vfs.Driver, uri vfs.URI, scope *memory.Scope, bm *memory.BufferManager) *vector.DataChunk {
	t.Helper()
	info, err := driver.Stat(context.Background(), uri)
	if err != nil {
		t.Fatal(err)
	}
	result, err := driver.ReadRange(context.Background(), vfs.RangeRequest{URI: uri, Offset: 0, Length: info.Size, Expected: &info.Identity, Tag: memory.TagReadAhead}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Buffer.Release()
	typesOnly, _ := protocolSchema()
	chunk, consumed, err := csvformat.DecodeChunk(result.Buffer.Bytes(), typesOnly, csvformat.DecodeOptions{Final: true}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	if consumed != int(info.Size) {
		chunk.Release()
		t.Fatalf("CSV consumed=%d size=%d", consumed, info.Size)
	}
	return chunk
}

// writeProtocolCSV 通过真实 FileWriterFactory 与 WriterPool 写一个 DataChunk。
func writeProtocolCSV(t *testing.T, registry *vfs.Registry, uri vfs.URI, chunk *vector.DataChunk, scope *memory.Scope, bm *memory.BufferManager) {
	t.Helper()
	_, columns := protocolSchema()
	pool, err := export.NewWriterPool(2, 4096, scope)
	if err != nil {
		t.Fatal(err)
	}
	factory, err := export.NewFileWriterFactory(export.FileWriterConfig{Schema: columns, Format: export.FormatCSV, Compression: "identity"}, registry, scope, bm, pool)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := factory.Open(context.Background(), uri, vfs.WriteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(context.Background(), chunk); err != nil {
		_ = writer.Abort(context.Background())
		t.Fatal(err)
	}
	if _, err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if pool.Active() != 0 {
		t.Fatalf("writer pool leak=%d", pool.Active())
	}
}

func TestS3ToSFTPProtocolConversion(t *testing.T) {
	const input = "1,alpha\n2,beta\n"
	s3State := &protocolS3Server{objects: map[string][]byte{"bucket/input/data.csv": []byte(input)}}
	s3Driver, s3Server := newProtocolS3Driver(t, s3State)
	defer s3Server.Close()
	sftpStore := &protocolSFTPStore{files: map[string][]byte{}}
	sftpDriver, sftpManager, sftpPool := newProtocolSFTPDriver(t, sftpStore)
	defer sftpManager.Close()

	scope := memory.NewManager(32 << 20).NewScope()
	bm := memory.NewBufferManager(nil)
	source, _ := vfs.ParseURI("s3://bucket/input/data.csv")
	chunk := decodeRemoteCSV(t, s3Driver, source, scope, bm)
	destination, _ := vfs.ParseURI("sftp://host:22/out/data.csv")
	registry, err := vfs.NewRegistry(s3Driver, sftpDriver)
	if err != nil {
		chunk.Release()
		t.Fatal(err)
	}
	writeProtocolCSV(t, registry, destination, chunk, scope, bm)
	chunk.Release()

	sftpStore.mu.Lock()
	got := string(sftpStore.files["/out/data.csv"])
	sftpStore.mu.Unlock()
	_, leased, _ := sftpPool.Stats()
	if got != input || leased != 0 || scope.Used() != 0 {
		t.Fatalf("S3->SFTP output=%q leased=%d memory=%d", got, leased, scope.Used())
	}
}

func TestSFTPToS3ProtocolConversion(t *testing.T) {
	const input = "3,gamma\n4,delta\n"
	sftpStore := &protocolSFTPStore{files: map[string][]byte{"/input/data.csv": []byte(input)}}
	sftpDriver, sftpManager, sftpPool := newProtocolSFTPDriver(t, sftpStore)
	defer sftpManager.Close()
	s3State := &protocolS3Server{objects: make(map[string][]byte)}
	s3Driver, s3Server := newProtocolS3Driver(t, s3State)
	defer s3Server.Close()

	scope := memory.NewManager(32 << 20).NewScope()
	bm := memory.NewBufferManager(nil)
	source, _ := vfs.ParseURI("sftp://host:22/input/data.csv")
	chunk := decodeRemoteCSV(t, sftpDriver, source, scope, bm)
	destination, _ := vfs.ParseURI("s3://bucket/out/data.csv")
	registry, err := vfs.NewRegistry(sftpDriver, s3Driver)
	if err != nil {
		chunk.Release()
		t.Fatal(err)
	}
	writeProtocolCSV(t, registry, destination, chunk, scope, bm)
	chunk.Release()

	s3State.mu.Lock()
	got := string(s3State.objects["bucket/out/data.csv"])
	remainingUploads := len(s3State.uploads)
	s3State.mu.Unlock()
	_, leased, _ := sftpPool.Stats()
	if got != input || leased != 0 || remainingUploads != 0 || scope.Used() != 0 {
		t.Fatalf("SFTP->S3 output=%q leased=%d multipart=%d memory=%d", got, leased, remainingUploads, scope.Used())
	}
}

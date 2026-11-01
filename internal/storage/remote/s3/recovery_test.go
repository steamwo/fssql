package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// recoveryUpload 表示测试服务端持有的一个未完成 multipart ownership。
type recoveryUpload struct {
	// Key 是目标对象 key。
	Key string
	// UploadID 是服务端 cleanup token。
	UploadID string
}

// recoveryServerState 实现 ListMultipartUploads/Create/UploadPart/Abort 的最小 S3 测试状态机。
type recoveryServerState struct {
	mu sync.Mutex
	// uploads 保存仍未完成的 multipart；被 Abort 后会删除。
	uploads []recoveryUpload
	// nextID 为 CreateMultipartUpload 生成稳定测试 ID。
	nextID int
	// listCalls/abortCalls 用于断言 pagination 与 cleanup 次数。
	listCalls  int
	abortCalls int
	// abortFailures 注入 retryable 5xx。
	abortFailures int
	// denyList 注入权限失败，验证 Resume 必须阻断。
	denyList bool
}

// handler 实现 recovery 测试所需的 S3 path-style 请求。
func (s *recoveryServerState) handler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodPost && q.Has("uploads"):
		s.nextID++
		id := fmt.Sprintf("upload-%04d", s.nextID)
		key := strings.TrimPrefix(r.URL.Path, "/bucket/")
		s.uploads = append(s.uploads, recoveryUpload{Key: key, UploadID: id})
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprintf(w, `<InitiateMultipartUploadResult><UploadId>%s</UploadId></InitiateMultipartUploadResult>`, id)
	case r.Method == http.MethodPut && q.Get("uploadId") != "":
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("ETag", `"part"`)
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodGet && q.Has("uploads"):
		s.listCalls++
		if s.denyList {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		s.writeMultipartList(w, q.Get("prefix"), q.Get("key-marker"), q.Get("upload-id-marker"), q.Get("max-uploads"))
	case r.Method == http.MethodDelete && q.Get("uploadId") != "":
		s.abortCalls++
		if s.abortFailures > 0 {
			s.abortFailures--
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		id := q.Get("uploadId")
		remaining := s.uploads[:0]
		for _, upload := range s.uploads {
			if upload.UploadID != id {
				remaining = append(remaining, upload)
			}
		}
		s.uploads = remaining
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "unexpected request", http.StatusBadRequest)
	}
}

// writeMultipartList 按 (key, uploadID) 排序并实现 S3 双 marker pagination。
func (s *recoveryServerState) writeMultipartList(w http.ResponseWriter, prefix, keyMarker, uploadMarker, rawLimit string) {
	limit, _ := strconv.Atoi(rawLimit)
	if limit <= 0 || limit > recoveryMaxUploads {
		limit = recoveryMaxUploads
	}
	items := append([]recoveryUpload(nil), s.uploads...)
	sort.Slice(items, func(i, j int) bool {
		if items[i].Key == items[j].Key {
			return items[i].UploadID < items[j].UploadID
		}
		return items[i].Key < items[j].Key
	})
	filtered := make([]recoveryUpload, 0, len(items))
	for _, item := range items {
		if !strings.HasPrefix(item.Key, prefix) {
			continue
		}
		if keyMarker != "" && (item.Key < keyMarker || (item.Key == keyMarker && item.UploadID <= uploadMarker)) {
			continue
		}
		filtered = append(filtered, item)
	}
	truncated := len(filtered) > limit
	if truncated {
		filtered = filtered[:limit]
	}
	_, _ = io.WriteString(w, "<ListMultipartUploadsResult>")
	for _, item := range filtered {
		_, _ = fmt.Fprintf(w, "<Upload><Key>%s</Key><UploadId>%s</UploadId></Upload>", item.Key, item.UploadID)
	}
	if truncated {
		last := filtered[len(filtered)-1]
		_, _ = fmt.Fprintf(w, "<IsTruncated>true</IsTruncated><NextKeyMarker>%s</NextKeyMarker><NextUploadIdMarker>%s</NextUploadIdMarker>", last.Key, last.UploadID)
	}
	_, _ = io.WriteString(w, "</ListMultipartUploadsResult>")
}

// newRecoveryTestDriver 创建带 no-op backoff 的 path-style S3 driver。
func newRecoveryTestDriver(t *testing.T, state *recoveryServerState) (*Driver, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(state.handler))
	driver, err := NewDriver(Config{
		Endpoint:          server.URL,
		Region:            "test-region",
		ForcePathStyle:    true,
		AllowInsecureHTTP: true,
		CredentialScope:   "scope",
		CredentialVersion: "v1",
		SecurityProfile:   "test",
		MaxRetries:        2,
	}, Credentials{AccessKey: "access", SecretKey: "secret"}, nil)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	driver.sleep = func(context.Context, time.Duration) error { return nil }
	return driver, server
}

func TestS3RecoveryCapabilityAndOrphanAfterProcessStateLoss(t *testing.T) {
	state := &recoveryServerState{}
	driver, server := newRecoveryTestDriver(t, state)
	defer server.Close()
	if !driver.StaticCapabilities().Has(vfs.IncompleteWriteRecovery) {
		t.Fatal("S3 must advertise incomplete write recovery")
	}

	mm := memory.NewManager(32 << 20)
	scope := mm.NewScope()
	object, _ := vfs.ParseURI("s3://bucket/staging/job-1/part-0001.parquet")
	handleI, err := driver.OpenWrite(context.Background(), object, vfs.WriteOptions{}, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	handle := handleI.(*s3WriteHandle)
	// 直接创建 multipart 后模拟硬退出：不调用 Abort，只释放本地进程 ownership。
	handle.mu.Lock()
	if err := handle.ensureMultipartLocked(context.Background()); err != nil {
		handle.mu.Unlock()
		t.Fatal(err)
	}
	handle.terminal = true
	handle.releaseLocked()
	handle.mu.Unlock()
	if scope.Used() != 0 {
		t.Fatalf("simulated process loss retained local memory=%d", scope.Used())
	}

	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	jobScope, _ := vfs.ParseURI("s3://bucket/staging/job-1/")
	report, err := registry.RecoverIncompleteWrites(context.Background(), jobScope, scope)
	if err != nil {
		t.Fatal(err)
	}
	if report.Discovered != 1 || report.Recovered != 1 {
		t.Fatalf("report=%+v", report)
	}
	state.mu.Lock()
	remaining := len(state.uploads)
	state.mu.Unlock()
	if remaining != 0 || scope.Used() != 0 {
		t.Fatalf("remaining=%d managed=%d", remaining, scope.Used())
	}
}

func TestS3RecoveryPaginationScopeIsolationAndIdempotence(t *testing.T) {
	state := &recoveryServerState{}
	for i := 0; i < 205; i++ {
		state.uploads = append(state.uploads, recoveryUpload{Key: fmt.Sprintf("staging/job-1/part-%03d", i), UploadID: fmt.Sprintf("u-%03d", i)})
	}
	// 相邻 job 前缀必须保留，job-1/ 不能误匹配 job-10/。
	state.uploads = append(state.uploads, recoveryUpload{Key: "staging/job-10/part-000", UploadID: "neighbor"})
	driver, server := newRecoveryTestDriver(t, state)
	defer server.Close()
	registry, _ := vfs.NewRegistry(driver)
	scope := memory.NewManager(4 << 20).NewScope()
	jobScope, _ := vfs.ParseURI("s3://bucket/staging/job-1/")
	report, err := registry.RecoverIncompleteWrites(context.Background(), jobScope, scope)
	if err != nil {
		t.Fatal(err)
	}
	if report.Discovered != 205 || report.Recovered != 205 {
		t.Fatalf("report=%+v", report)
	}
	state.mu.Lock()
	listCalls := state.listCalls
	remaining := append([]recoveryUpload(nil), state.uploads...)
	state.mu.Unlock()
	if listCalls < 3 {
		t.Fatalf("expected pagination, list calls=%d", listCalls)
	}
	if len(remaining) != 1 || remaining[0].UploadID != "neighbor" {
		t.Fatalf("scope isolation failed: %+v", remaining)
	}
	if scope.Used() != 0 {
		t.Fatalf("metadata leak=%d", scope.Used())
	}

	report, err = registry.RecoverIncompleteWrites(context.Background(), jobScope, scope)
	if err != nil || report.Discovered != 0 || report.Recovered != 0 {
		t.Fatalf("idempotent recovery report=%+v err=%v", report, err)
	}
}

func TestS3RecoveryRetryCredentialCancelAndMemoryGate(t *testing.T) {
	state := &recoveryServerState{
		uploads:       []recoveryUpload{{Key: "staging/job-1/part", UploadID: "u-1"}},
		abortFailures: 1,
	}
	driver, server := newRecoveryTestDriver(t, state)
	defer server.Close()
	registry, _ := vfs.NewRegistry(driver)
	jobScope, _ := vfs.ParseURI("s3://bucket/staging/job-1/")
	scope := memory.NewManager(4 << 20).NewScope()
	if report, err := registry.RecoverIncompleteWrites(context.Background(), jobScope, scope); err != nil || report.Recovered != 1 {
		t.Fatalf("retry recovery report=%+v err=%v", report, err)
	}
	state.mu.Lock()
	abortCalls := state.abortCalls
	state.mu.Unlock()
	if abortCalls < 2 {
		t.Fatalf("abort retry not exercised: calls=%d", abortCalls)
	}

	state = &recoveryServerState{uploads: []recoveryUpload{{Key: "staging/job-1/part", UploadID: "u-2"}}, denyList: true}
	driver2, server2 := newRecoveryTestDriver(t, state)
	defer server2.Close()
	registry2, _ := vfs.NewRegistry(driver2)
	if _, err := registry2.RecoverIncompleteWrites(context.Background(), jobScope, scope); !errors.Is(err, vfs.ErrCredential) {
		t.Fatalf("expected credential failure, got %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := registry.RecoverIncompleteWrites(ctx, jobScope, scope); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}

	limited := memory.NewManager(1024).NewScope()
	state3 := &recoveryServerState{uploads: []recoveryUpload{{Key: "staging/job-1/part", UploadID: "u-3"}}}
	driver3, server3 := newRecoveryTestDriver(t, state3)
	defer server3.Close()
	registry3, _ := vfs.NewRegistry(driver3)
	if _, err := registry3.RecoverIncompleteWrites(context.Background(), jobScope, limited); !errors.Is(err, memory.ErrLimitExceeded) {
		t.Fatalf("expected memory limit, got %v", err)
	}
	state3.mu.Lock()
	listCalls := state3.listCalls
	state3.mu.Unlock()
	if listCalls != 0 || limited.Used() != 0 {
		t.Fatalf("memory gate side effect list=%d used=%d", listCalls, limited.Used())
	}
}

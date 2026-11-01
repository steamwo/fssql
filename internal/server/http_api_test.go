package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
	querypkg "github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/resource"
)

var errHTTPTestEncode = errors.New("test HTTP chunk encode failure")

// httpTestChunkEncoder 是测试用 external DATA encoder，不进入 production wire contract。
type httpTestChunkEncoder struct {
	// mu 保护并发测试读取的 encoded/failAfter。
	mu sync.Mutex
	// encoded 保存已经成功观察到的 chunk 值。
	encoded []int64
	// failAfter 表示第 N 次 EncodeChunk 返回注入错误；0 表示不注入。
	failAfter int
}

// ContentType 返回测试私有媒体类型。
func (e *httpTestChunkEncoder) ContentType() string { return "application/x-fssql-test" }

// EncodeChunk 读取单列 bigint 测试 chunk，并写入简单行格式。
func (e *httpTestChunkEncoder) EncodeChunk(ctx context.Context, w io.Writer, chunk *vector.DataChunk) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if chunk == nil || len(chunk.Columns) != 1 || chunk.Cardinality != 1 {
		return errors.New("invalid test chunk")
	}
	value, ok := chunk.Columns[0].Int64(0)
	if !ok {
		return errors.New("NULL test value")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	next := len(e.encoded) + 1
	if e.failAfter > 0 && next >= e.failAfter {
		return errHTTPTestEncode
	}
	e.encoded = append(e.encoded, value)
	_, err := fmt.Fprintf(w, "%d\n", value)
	return err
}

// submitHTTPQuery 提交测试 Query 并解析返回的稳定 Query ID。
func submitHTTPQuery(t *testing.T, api *HTTPAPI, body string) QueryID {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/v1/queries", strings.NewReader(body))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("submit status=%d body=%s", response.Code, response.Body.String())
	}
	var payload httpSubmitResponse
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if payload.ID == "" {
		t.Fatal("empty query id")
	}
	return QueryID(payload.ID)
}

func TestHTTPAPIQueryResultsDrainAndReleaseManagedChunks(t *testing.T) {
	engine := fakeQueryEngine{plan: func(queryContext *querypkg.Context, sql string) (PreparedQuery, error) {
		if sql != "SELECT 1, 2" {
			return nil, fmt.Errorf("unexpected sql %q", sql)
		}
		return preparedQueryFunc(func(queryContext *querypkg.Context, cursor *ResultCursor) error {
			bm := memory.NewBufferManager(nil)
			for _, value := range []int64{1, 2} {
				chunk := testResultChunk(t, queryContext.Memory, bm, value)
				if err := cursor.Push(queryContext.Context(), chunk); err != nil {
					chunk.Release()
					return err
				}
			}
			return nil
		}), nil
	}}
	service, scheduler, manager := newQueryServiceHarness(t, engine, QueryServiceConfig{CursorCapacityChunks: 1})
	encoder := &httpTestChunkEncoder{}
	api, err := NewHTTPAPI(service, encoder, HTTPAPIConfig{})
	if err != nil {
		t.Fatal(err)
	}
	id := submitHTTPQuery(t, api, `{"sql":"SELECT 1, 2","resource_group":"default","class":"interactive","cpu_slots":1,"memory_bytes":65536}`)

	resultRequest := httptest.NewRequest(http.MethodGet, "/v1/queries/"+string(id)+"/results", nil)
	resultResponse := httptest.NewRecorder()
	api.ServeHTTP(resultResponse, resultRequest)
	if resultResponse.Code != http.StatusOK {
		t.Fatalf("results status=%d body=%s", resultResponse.Code, resultResponse.Body.String())
	}
	if got := resultResponse.Header().Get("Content-Type"); got != encoder.ContentType() {
		t.Fatalf("content-type=%q", got)
	}
	if got := resultResponse.Body.String(); got != "1\n2\n" {
		t.Fatalf("result body=%q", got)
	}
	waitQueryState(t, service, id, QuerySucceeded)
	if manager.Used() != 0 {
		t.Fatalf("managed bytes leaked: %d", manager.Used())
	}
	if scheduler.Snapshot().GlobalUsed != (resource.Usage{}) {
		t.Fatalf("admission lease leaked: %+v", scheduler.Snapshot().GlobalUsed)
	}

	statusRequest := httptest.NewRequest(http.MethodGet, "/v1/queries/"+string(id), nil)
	statusResponse := httptest.NewRecorder()
	api.ServeHTTP(statusResponse, statusRequest)
	if statusResponse.Code != http.StatusOK {
		t.Fatalf("status code=%d body=%s", statusResponse.Code, statusResponse.Body.String())
	}
	var status httpStatusResponse
	if err := json.NewDecoder(statusResponse.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	if status.State != "SUCCEEDED" || status.ID != string(id) {
		t.Fatalf("status=%+v", status)
	}

	forgetRequest := httptest.NewRequest(http.MethodDelete, "/v1/queries/"+string(id), nil)
	forgetResponse := httptest.NewRecorder()
	api.ServeHTTP(forgetResponse, forgetRequest)
	if forgetResponse.Code != http.StatusNoContent {
		t.Fatalf("forget status=%d body=%s", forgetResponse.Code, forgetResponse.Body.String())
	}
	notFoundResponse := httptest.NewRecorder()
	api.ServeHTTP(notFoundResponse, statusRequest.Clone(context.Background()))
	if notFoundResponse.Code != http.StatusNotFound {
		t.Fatalf("status after forget=%d", notFoundResponse.Code)
	}
}

func TestHTTPAPICancelDoesNotDependOnRequestLifetime(t *testing.T) {
	started := make(chan struct{})
	engine := fakeQueryEngine{plan: func(*querypkg.Context, string) (PreparedQuery, error) {
		return preparedQueryFunc(func(queryContext *querypkg.Context, _ *ResultCursor) error {
			close(started)
			<-queryContext.Context().Done()
			return queryContext.Context().Err()
		}), nil
	}}
	service, scheduler, manager := newQueryServiceHarness(t, engine, QueryServiceConfig{})
	api, err := NewHTTPAPI(service, &httpTestChunkEncoder{}, HTTPAPIConfig{})
	if err != nil {
		t.Fatal(err)
	}
	id := submitHTTPQuery(t, api, `{"sql":"SELECT wait","resource_group":"default","cpu_slots":1}`)
	<-started

	cancelRequest := httptest.NewRequest(http.MethodPost, "/v1/queries/"+string(id)+"/cancel", nil)
	cancelResponse := httptest.NewRecorder()
	api.ServeHTTP(cancelResponse, cancelRequest)
	if cancelResponse.Code != http.StatusAccepted {
		t.Fatalf("cancel status=%d body=%s", cancelResponse.Code, cancelResponse.Body.String())
	}
	waitQueryState(t, service, id, QueryCanceled)
	if manager.Used() != 0 {
		t.Fatalf("managed bytes leaked: %d", manager.Used())
	}
	if scheduler.Snapshot().GlobalUsed != (resource.Usage{}) {
		t.Fatalf("admission lease leaked: %+v", scheduler.Snapshot().GlobalUsed)
	}
}

func TestHTTPAPIEncoderFailureCancelsQueryAndReleasesBufferedChunks(t *testing.T) {
	secondPushStarted := make(chan struct{})
	engine := fakeQueryEngine{plan: func(*querypkg.Context, string) (PreparedQuery, error) {
		return preparedQueryFunc(func(queryContext *querypkg.Context, cursor *ResultCursor) error {
			bm := memory.NewBufferManager(nil)
			first := testResultChunk(t, queryContext.Memory, bm, 1)
			if err := cursor.Push(queryContext.Context(), first); err != nil {
				first.Release()
				return err
			}
			second := testResultChunk(t, queryContext.Memory, bm, 2)
			close(secondPushStarted)
			if err := cursor.Push(queryContext.Context(), second); err != nil {
				second.Release()
				return err
			}
			return nil
		}), nil
	}}
	service, scheduler, manager := newQueryServiceHarness(t, engine, QueryServiceConfig{CursorCapacityChunks: 1})
	api, err := NewHTTPAPI(service, &httpTestChunkEncoder{failAfter: 1}, HTTPAPIConfig{})
	if err != nil {
		t.Fatal(err)
	}
	id := submitHTTPQuery(t, api, `{"sql":"SELECT fail_encode","resource_group":"default","cpu_slots":1}`)
	<-secondPushStarted

	request := httptest.NewRequest(http.MethodGet, "/v1/queries/"+string(id)+"/results", nil)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("result status=%d", response.Code)
	}
	waitQueryState(t, service, id, QueryCanceled)
	if manager.Used() != 0 {
		t.Fatalf("managed result bytes leaked after encoder failure: %d", manager.Used())
	}
	if scheduler.Snapshot().GlobalUsed != (resource.Usage{}) {
		t.Fatalf("admission lease leaked after encoder failure: %+v", scheduler.Snapshot().GlobalUsed)
	}
}

func TestHTTPAPIRejectsUnboundedOrInvalidControlRequests(t *testing.T) {
	engine := fakeQueryEngine{plan: func(*querypkg.Context, string) (PreparedQuery, error) {
		return preparedQueryFunc(func(*querypkg.Context, *ResultCursor) error { return nil }), nil
	}}
	service, _, _ := newQueryServiceHarness(t, engine, QueryServiceConfig{})
	api, err := NewHTTPAPI(service, &httpTestChunkEncoder{}, HTTPAPIConfig{MaxControlBodyBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		method string
		path   string
		body   string
		want   int
	}{
		{name: "oversized", method: http.MethodPost, path: "/v1/queries", body: `{"sql":"` + strings.Repeat("x", 128) + `","resource_group":"default"}`, want: http.StatusRequestEntityTooLarge},
		{name: "unknown field", method: http.MethodPost, path: "/v1/queries", body: `{"sql":"SELECT 1","resource_group":"default","surprise":true}`, want: http.StatusBadRequest},
		{name: "invalid class", method: http.MethodPost, path: "/v1/queries", body: `{"sql":"SELECT 1","resource_group":"default","class":"urgent"}`, want: http.StatusBadRequest},
		{name: "trailing json", method: http.MethodPost, path: "/v1/queries", body: `{"sql":"SELECT 1","resource_group":"default"}{}`, want: http.StatusBadRequest},
		{name: "wrong method", method: http.MethodGet, path: "/v1/queries", want: http.StatusMethodNotAllowed},
		{name: "unknown path", method: http.MethodGet, path: "/v1/nope", want: http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			response := httptest.NewRecorder()
			api.ServeHTTP(response, request)
			if response.Code != tc.want {
				t.Fatalf("status=%d want=%d body=%s", response.Code, tc.want, response.Body.String())
			}
		})
	}
}

func TestHTTPAPIConstructorRequiresExplicitDataEncoder(t *testing.T) {
	if _, err := NewHTTPAPI(nil, &httpTestChunkEncoder{}, HTTPAPIConfig{}); err == nil {
		t.Fatal("expected nil service rejection")
	}
	engine := fakeQueryEngine{plan: func(*querypkg.Context, string) (PreparedQuery, error) { return nil, errors.New("unused") }}
	service, _, _ := newQueryServiceHarness(t, engine, QueryServiceConfig{})
	if _, err := NewHTTPAPI(service, nil, HTTPAPIConfig{}); err == nil {
		t.Fatal("expected nil encoder rejection")
	}
}

package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	coretypes "github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/introspection"
)

// recordingSourceInspector 记录 HTTP adapter 传入的 typed SourceSpec，并返回预设 Bind-backed report。
type recordingSourceInspector struct {
	// mu 保护 spec/called 的并发访问。
	mu sync.Mutex
	// spec 保存最近一次请求。
	spec introspection.SourceSpec
	// report 是测试返回的 schema 报告。
	report introspection.SourceReport
	// err 是测试注入的 inspector failure。
	err error
	// blockUntilCancel 要求 inspector 等待 request cancellation。
	blockUntilCancel bool
}

// InspectSource 实现 server 所需的统一 introspection.Inspector。
func (i *recordingSourceInspector) InspectSource(ctx context.Context, spec introspection.SourceSpec) (introspection.SourceReport, error) {
	i.mu.Lock()
	i.spec = spec
	block := i.blockUntilCancel
	report := i.report
	err := i.err
	i.mu.Unlock()
	if block {
		<-ctx.Done()
		return introspection.SourceReport{}, ctx.Err()
	}
	return report, err
}

func TestSourceInspectHTTPDelegatesTypedSpecAndProjectsReport(t *testing.T) {
	inspector := &recordingSourceInspector{report: introspection.NewSourceReport(
		[]coretypes.StructField{
			{Name: "id", Type: coretypes.LogicalType{ID: coretypes.BigInt}},
			{Name: "name", Type: coretypes.LogicalType{ID: coretypes.Varchar}},
		},
		introspection.SchemaSourceFileMetadata,
		introspection.ConfidenceExact,
		3,
		3,
		"dataset-fingerprint",
		true,
		true,
	)}
	handler, err := NewSourceInspectHTTPHandler(inspector, SourceInspectHTTPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/sources/inspect", strings.NewReader(`{"format":"PARQUET","pattern":"s3://bucket/data/*.parquet","union_by_name":true,"full_validation":true}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	inspector.mu.Lock()
	gotSpec := inspector.spec
	inspector.mu.Unlock()
	if gotSpec.Format != introspection.SourceFormatParquet || gotSpec.Pattern != "s3://bucket/data/*.parquet" || !gotSpec.UnionByName || !gotSpec.FullValidation {
		t.Fatalf("spec=%+v", gotSpec)
	}
	var payload sourceInspectHTTPResponse
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Columns) != 2 || payload.Columns[0].Name != "id" || payload.Columns[0].Type != "BIGINT" || payload.Columns[1].Type != "VARCHAR" {
		t.Fatalf("columns=%+v", payload.Columns)
	}
	if payload.SchemaSource != "FILE_METADATA" || payload.Confidence != "EXACT" || payload.MatchedFiles != 3 || payload.SchemaFiles != 3 || payload.DatasetFingerprint != "dataset-fingerprint" || !payload.UnionByName || !payload.FullValidation {
		t.Fatalf("payload=%+v", payload)
	}
}

func TestSourceInspectHTTPRejectsUnsupportedFormatWithoutFallback(t *testing.T) {
	registry := introspection.NewRegistry()
	handler, err := NewSourceInspectHTTPHandler(registry, SourceInspectHTTPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/sources/inspect", strings.NewReader(`{"format":"json","pattern":"s3://bucket/data.json"}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "unsupported source format") {
		t.Fatalf("body=%s", response.Body.String())
	}
}

func TestSourceInspectHTTPPropagatesRequestCancellation(t *testing.T) {
	inspector := &recordingSourceInspector{blockUntilCancel: true}
	handler, err := NewSourceInspectHTTPHandler(inspector, SourceInspectHTTPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodPost, "/v1/sources/inspect", strings.NewReader(`{"format":"csv","pattern":"s3://bucket/data.csv"}`)).WithContext(ctx)
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(response, request)
	}()
	time.Sleep(5 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("source inspect handler did not stop after request cancellation")
	}
	if response.Code != http.StatusRequestTimeout {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestSourceInspectHTTPRejectsInvalidAndOversizedControls(t *testing.T) {
	inspector := &recordingSourceInspector{report: introspection.SourceReport{}}
	handler, err := NewSourceInspectHTTPHandler(inspector, SourceInspectHTTPConfig{MaxControlBodyBytes: 64})
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
		{name: "oversized", method: http.MethodPost, path: "/v1/sources/inspect", body: `{"format":"csv","pattern":"` + strings.Repeat("x", 128) + `"}`, want: http.StatusRequestEntityTooLarge},
		{name: "unknown field", method: http.MethodPost, path: "/v1/sources/inspect", body: `{"format":"csv","pattern":"x","extra":true}`, want: http.StatusBadRequest},
		{name: "wrong method", method: http.MethodGet, path: "/v1/sources/inspect", want: http.StatusMethodNotAllowed},
		{name: "unknown path", method: http.MethodPost, path: "/v1/sources/other", want: http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != tc.want {
				t.Fatalf("status=%d want=%d body=%s", response.Code, tc.want, response.Body.String())
			}
		})
	}
}

func TestSourceInspectHTTPMapsBindFailureToUnprocessableEntity(t *testing.T) {
	inspector := &recordingSourceInspector{err: errors.New("bind failed")}
	handler, err := NewSourceInspectHTTPHandler(inspector, SourceInspectHTTPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/sources/inspect", strings.NewReader(`{"format":"csv","pattern":"s3://bucket/data.csv"}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestSourceInspectHTTPConstructorRequiresInspector(t *testing.T) {
	if _, err := NewSourceInspectHTTPHandler(nil, SourceInspectHTTPConfig{}); err == nil {
		t.Fatal("expected nil inspector rejection")
	}
}

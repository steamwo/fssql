package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/resource"
)

const defaultHTTPMaxControlBodyBytes int64 = 1 << 20

// HTTPChunkEncoder 定义 Server DATA payload 的外部交换边界。
// HTTP transport 只管理 Query/ResultCursor 生命周期，不在此处复制 Vector 类型语义。
type HTTPChunkEncoder interface {
	// ContentType 返回 DATA response 的媒体类型。
	ContentType() string
	// EncodeChunk 把一个 native DataChunk 编码到外部响应；调用期间不取得 chunk ownership。
	EncodeChunk(context.Context, io.Writer, *vector.DataChunk) error
}

// HTTPAPIConfig 固定 HTTP control-plane 的有界输入限制。
type HTTPAPIConfig struct {
	// MaxControlBodyBytes 限制 submit JSON body，防止 request control object 在 Go heap 无界增长。
	MaxControlBodyBytes int64
}

// HTTPAPI 是 QueryService 的 HTTP transport adapter；Query lifetime 仍由 QueryRegistry/QueryHandle 拥有。
type HTTPAPI struct {
	// service 是唯一 Query 生命周期入口。
	service *QueryService
	// encoder 负责 Server DATA payload 的 external interchange 编码。
	encoder HTTPChunkEncoder
	// config 保存已规范化的有界 transport 配置。
	config HTTPAPIConfig
}

// httpSubmitRequest 是外部 HTTP submit payload；不直接暴露内部 resource.QueryClass 数值。
type httpSubmitRequest struct {
	SQL           string `json:"sql"`
	ResourceGroup string `json:"resource_group"`
	Class         string `json:"class,omitempty"`
	CPUSlots      int64  `json:"cpu_slots,omitempty"`
	MemoryBytes   int64  `json:"memory_bytes,omitempty"`
	RemoteIOSlots int64  `json:"remote_io_slots,omitempty"`
	SpillIOSlots  int64  `json:"spill_io_slots,omitempty"`
}

// httpSubmitResponse 返回稳定 Query ID。
type httpSubmitResponse struct {
	ID string `json:"id"`
}

// httpStatusResponse 是不暴露内部可写对象的 QuerySnapshot wire view。
type httpStatusResponse struct {
	ID              string    `json:"id"`
	State           string    `json:"state"`
	CancelRequested bool      `json:"cancel_requested"`
	Error           string    `json:"error,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// httpErrorResponse 提供稳定的结构化 control-plane 错误。
type httpErrorResponse struct {
	Error string `json:"error"`
}

// NewHTTPAPI 创建 HTTP adapter；DATA encoder 必须显式提供，避免 transport 默默选择未冻结的结果格式。
func NewHTTPAPI(service *QueryService, encoder HTTPChunkEncoder, config HTTPAPIConfig) (*HTTPAPI, error) {
	if service == nil {
		return nil, errors.New("server: query service required for HTTP API")
	}
	if encoder == nil {
		return nil, errors.New("server: HTTP DATA chunk encoder required")
	}
	if strings.TrimSpace(encoder.ContentType()) == "" {
		return nil, errors.New("server: HTTP DATA content type required")
	}
	if config.MaxControlBodyBytes <= 0 {
		config.MaxControlBodyBytes = defaultHTTPMaxControlBodyBytes
	}
	return &HTTPAPI{service: service, encoder: encoder, config: config}, nil
}

// ServeHTTP 路由 Phase 5 Query HTTP API；未知 path/method 明确返回 404/405。
func (a *HTTPAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v1/queries" {
		if r.Method != http.MethodPost {
			writeHTTPError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		a.handleSubmit(w, r)
		return
	}

	id, suffix, ok := parseQueryPath(r.URL.Path)
	if !ok {
		writeHTTPError(w, http.StatusNotFound, "not found")
		return
	}
	switch suffix {
	case "":
		switch r.Method {
		case http.MethodGet:
			a.handleStatus(w, id)
		case http.MethodDelete:
			a.handleForget(w, id)
		default:
			writeHTTPError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	case "cancel":
		if r.Method != http.MethodPost {
			writeHTTPError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		a.handleCancel(w, id)
	case "results":
		if r.Method != http.MethodGet {
			writeHTTPError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		a.handleResults(w, r, id)
	default:
		writeHTTPError(w, http.StatusNotFound, "not found")
	}
}

// handleSubmit 解析 bounded JSON request，并只把 request context 用于 admission 等待。
func (a *HTTPAPI) handleSubmit(w http.ResponseWriter, r *http.Request) {
	reader := http.MaxBytesReader(w, r.Body, a.config.MaxControlBodyBytes)
	defer reader.Close()
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	var input httpSubmitRequest
	if err := decoder.Decode(&input); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			writeHTTPError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeHTTPError(w, http.StatusBadRequest, "invalid JSON request")
		return
	}
	if err := ensureJSONEOF(decoder); err != nil {
		writeHTTPError(w, http.StatusBadRequest, "invalid JSON request")
		return
	}
	class, err := parseHTTPQueryClass(input.Class)
	if err != nil {
		writeHTTPError(w, http.StatusBadRequest, err.Error())
		return
	}
	response, err := a.service.Submit(r.Context(), SubmitRequest{
		SQL: input.SQL, ResourceGroup: input.ResourceGroup, Class: class,
		CPUSlots: input.CPUSlots, MemoryBytes: input.MemoryBytes,
		RemoteIOSlots: input.RemoteIOSlots, SpillIOSlots: input.SpillIOSlots,
	})
	if err != nil {
		writeHTTPServiceError(w, err)
		return
	}
	writeHTTPJSON(w, http.StatusAccepted, httpSubmitResponse{ID: string(response.ID)})
}

// handleStatus 返回 QueryRegistry 快照，不把 HTTP request context 绑定到 Query lifetime。
func (a *HTTPAPI) handleStatus(w http.ResponseWriter, id QueryID) {
	snapshot, err := a.service.Status(id)
	if err != nil {
		writeHTTPServiceError(w, err)
		return
	}
	out := httpStatusResponse{
		ID: string(snapshot.ID), State: snapshot.State.String(), CancelRequested: snapshot.CancelRequested,
		CreatedAt: snapshot.CreatedAt, UpdatedAt: snapshot.UpdatedAt,
	}
	if snapshot.Error != nil {
		out.Error = snapshot.Error.Error()
	}
	writeHTTPJSON(w, http.StatusOK, out)
}

// handleCancel 请求 Query cancellation；真正 CANCELED 终态仍由 QueryHandle.Finalize 在资源归零后发布。
func (a *HTTPAPI) handleCancel(w http.ResponseWriter, id QueryID) {
	if err := a.service.Cancel(id); err != nil {
		writeHTTPServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// handleForget 只释放 terminal Query 的 registry retention ownership。
func (a *HTTPAPI) handleForget(w http.ResponseWriter, id QueryID) {
	if err := a.service.Forget(id); err != nil {
		writeHTTPServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleResults 独占 ResultCursor consumer ownership，逐 chunk 编码并立即 Release。
// 若客户端断开或 encoder 失败，必须取消 Query，避免 producer 永久阻塞或 Managed Buffer 泄漏。
func (a *HTTPAPI) handleResults(w http.ResponseWriter, r *http.Request, id QueryID) {
	cursor, err := a.service.ClaimResults(id)
	if err != nil {
		writeHTTPServiceError(w, err)
		return
	}
	completed := false
	defer func() {
		if !completed {
			_ = a.service.Cancel(id)
		}
	}()

	w.Header().Set("Content-Type", a.encoder.ContentType())
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	for {
		chunk, nextErr := cursor.Next(r.Context())
		if errors.Is(nextErr, io.EOF) {
			completed = true
			return
		}
		if nextErr != nil {
			return
		}
		encodeErr := a.encoder.EncodeChunk(r.Context(), w, chunk)
		chunk.Release()
		if encodeErr != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
	}
}

// parseQueryPath 解析 `/v1/queries/{id}` 及其一级 action suffix。
func parseQueryPath(path string) (QueryID, string, bool) {
	const prefix = "/v1/queries/"
	if !strings.HasPrefix(path, prefix) {
		return "", "", false
	}
	rest := strings.Trim(strings.TrimPrefix(path, prefix), "/")
	if rest == "" {
		return "", "", false
	}
	parts := strings.Split(rest, "/")
	if len(parts) > 2 || parts[0] == "" {
		return "", "", false
	}
	suffix := ""
	if len(parts) == 2 {
		suffix = parts[1]
	}
	return QueryID(parts[0]), suffix, true
}

// parseHTTPQueryClass 把稳定文本值翻译为内部 resource.QueryClass。
func parseHTTPQueryClass(raw string) (resource.QueryClass, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "interactive":
		return resource.QueryClassInteractive, nil
	case "batch":
		return resource.QueryClassBatch, nil
	default:
		return 0, fmt.Errorf("invalid query class %q", raw)
	}
}

// ensureJSONEOF 拒绝一个 request body 中拼接多个 JSON value。
func ensureJSONEOF(decoder *json.Decoder) error {
	var extra json.RawMessage
	if err := decoder.Decode(&extra); errors.Is(err, io.EOF) {
		return nil
	} else if err != nil {
		return err
	}
	return errors.New("server: trailing JSON value")
}

// writeHTTPServiceError 把内部 control-plane 错误映射为稳定 HTTP status，不改变 Query terminal 语义。
func writeHTTPServiceError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, ErrQueryNotFound):
		status = http.StatusNotFound
	case errors.Is(err, ErrQueryServiceCapacity):
		status = http.StatusTooManyRequests
	case errors.Is(err, ErrInvalidSubmitRequest), errors.Is(err, resource.ErrInvalidRequest), errors.Is(err, resource.ErrUnknownGroup), errors.Is(err, resource.ErrRequestTooLarge):
		status = http.StatusBadRequest
	case errors.Is(err, ErrQueryNotTerminal), errors.Is(err, ErrQueryResultsClaimed):
		status = http.StatusConflict
	case errors.Is(err, resource.ErrSchedulerClosed):
		status = http.StatusServiceUnavailable
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		status = http.StatusRequestTimeout
	}
	writeHTTPError(w, status, err.Error())
}

// writeHTTPError 写入有界的小型 JSON control response。
func writeHTTPError(w http.ResponseWriter, status int, message string) {
	writeHTTPJSON(w, status, httpErrorResponse{Error: message})
}

// writeHTTPJSON 统一 control-plane JSON response headers。
func writeHTTPJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

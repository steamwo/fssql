package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/steamwo/fssql/internal/introspection"
)

// SourceInspectHTTPConfig 固定 source inspect control request 的有界输入限制。
type SourceInspectHTTPConfig struct {
	// MaxControlBodyBytes 限制 JSON control body，防止 introspection request 在 Go heap 无界增长。
	MaxControlBodyBytes int64
}

// SourceInspectHTTPHandler 实现 `POST /v1/sources/inspect`，schema 解析完全委托给 Bind-backed Inspector。
type SourceInspectHTTPHandler struct {
	// inspector 必须复用 Query Bind/TableFunction.Bind；handler 本身不做 schema inference。
	inspector introspection.Inspector
	// config 保存规范化后的 bounded request 配置。
	config SourceInspectHTTPConfig
}

// sourceInspectHTTPRequest 是稳定的 source inspect control payload。
type sourceInspectHTTPRequest struct {
	// Format 选择已经注册真实 Bind 路径的 source format。
	Format string `json:"format"`
	// Pattern 是交给 FileSetPlanner/Bind 的 URI 或 glob。
	Pattern string `json:"pattern"`
	// UnionByName 显式请求格式支持的 union-by-name schema bind。
	UnionByName bool `json:"union_by_name,omitempty"`
	// FullValidation 显式请求 Experimental 全量 schema validation；默认 false。
	FullValidation bool `json:"full_validation,omitempty"`
}

// sourceInspectHTTPColumn 是 SourceReport.Column 的外部只读投影。
type sourceInspectHTTPColumn struct {
	// Name 是 Bind 解析出的列名。
	Name string `json:"name"`
	// Type 是 Bind 解析出的 canonical logical type 名称。
	Type string `json:"type"`
}

// sourceInspectHTTPResponse 是 SourceReport 的 HTTP wire projection。
type sourceInspectHTTPResponse struct {
	// Columns 与实际 Query Bind schema 一一对应。
	Columns []sourceInspectHTTPColumn `json:"columns"`
	// SchemaSource 标记 schema 来源，例如 FILE_METADATA/SAMPLE_INFERRED。
	SchemaSource string `json:"schema_source"`
	// Confidence 标记 schema resolution 的置信等级。
	Confidence string `json:"confidence"`
	// MatchedFiles 是 immutable ScanSet 中匹配文件数量。
	MatchedFiles int `json:"matched_files"`
	// SchemaFiles 是本次 Bind 实际读取 schema 的文件数量。
	SchemaFiles int `json:"schema_files"`
	// DatasetFingerprint 标识 FileSetPlanner 冻结的数据集快照。
	DatasetFingerprint string `json:"dataset_fingerprint"`
	// UnionByName 表示实际 Bind 是否执行 union-by-name。
	UnionByName bool `json:"union_by_name"`
	// FullValidation 表示实际 Bind 是否执行显式全量 schema validation。
	FullValidation bool `json:"full_validation"`
}

// NewSourceInspectHTTPHandler 创建 source inspect endpoint；Inspector 不能为空，避免 server 退化为独立 inference。
func NewSourceInspectHTTPHandler(inspector introspection.Inspector, config SourceInspectHTTPConfig) (*SourceInspectHTTPHandler, error) {
	if inspector == nil {
		return nil, errors.New("server: source inspector required")
	}
	if config.MaxControlBodyBytes <= 0 {
		config.MaxControlBodyBytes = defaultHTTPMaxControlBodyBytes
	}
	return &SourceInspectHTTPHandler{inspector: inspector, config: config}, nil
}

// ServeHTTP 只服务稳定 source inspect 路由；其他路由由 Query HTTP API 或上层 mux 处理。
func (h *SourceInspectHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/sources/inspect" {
		writeHTTPError(w, http.StatusNotFound, "not found")
		return
	}
	if r.Method != http.MethodPost {
		writeHTTPError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	h.handleInspect(w, r)
}

// handleInspect 解析 bounded typed SourceSpec，并直接调用共享 Source Introspection service。
func (h *SourceInspectHTTPHandler) handleInspect(w http.ResponseWriter, r *http.Request) {
	reader := http.MaxBytesReader(w, r.Body, h.config.MaxControlBodyBytes)
	defer reader.Close()
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	var input sourceInspectHTTPRequest
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
	spec := introspection.SourceSpec{
		Format:         introspection.SourceFormat(strings.ToLower(strings.TrimSpace(input.Format))),
		Pattern:        input.Pattern,
		UnionByName:    input.UnionByName,
		FullValidation: input.FullValidation,
	}
	report, err := h.inspector.InspectSource(r.Context(), spec)
	if err != nil {
		writeSourceInspectError(w, err)
		return
	}
	writeHTTPJSON(w, http.StatusOK, sourceInspectResponse(report))
}

// sourceInspectResponse 把不可变 SourceReport 投影为 JSON DTO，不重新解释或推断 schema。
func sourceInspectResponse(report introspection.SourceReport) sourceInspectHTTPResponse {
	columns := report.Columns()
	outColumns := make([]sourceInspectHTTPColumn, len(columns))
	for i, column := range columns {
		outColumns[i] = sourceInspectHTTPColumn{Name: column.Name, Type: column.TypeName}
	}
	return sourceInspectHTTPResponse{
		Columns:            outColumns,
		SchemaSource:       string(report.SchemaSource),
		Confidence:         string(report.Confidence),
		MatchedFiles:       report.MatchedFiles,
		SchemaFiles:        report.SchemaFiles,
		DatasetFingerprint: report.DatasetFingerprint,
		UnionByName:        report.UnionByName,
		FullValidation:     report.FullValidation,
	}
}

// writeSourceInspectError 映射 SourceSpec/Bind/cancellation 错误，不把 unsupported format 静默降级到另一套 inference。
func writeSourceInspectError(w http.ResponseWriter, err error) {
	status := http.StatusUnprocessableEntity
	switch {
	case errors.Is(err, introspection.ErrInvalidSourceSpec), errors.Is(err, introspection.ErrUnsupportedSourceFormat):
		status = http.StatusBadRequest
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		status = http.StatusRequestTimeout
	}
	writeHTTPError(w, status, err.Error())
}

package server

import (
	"bytes"
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
	querypkg "github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/resource"
)

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
func (e *httpTestChunkEncoder) EncodeChunk(ctx context.Context, w io.Writer, chunk interface{ /* compile-time shape intentionally rejected below */ }) error {
	return errors.New("unreachable")
}

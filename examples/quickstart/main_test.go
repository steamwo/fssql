package main

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

// TestRun 验证外部调用方式的实际结果与 README 中的示例输出一致。
func TestRun(t *testing.T) {
	var output bytes.Buffer
	if err := run(context.Background(), &output); err != nil {
		t.Fatal(err)
	}
	const want = "native: 42\ndatabase/sql: 42\n"
	if got := output.String(); got != want {
		t.Fatalf("输出 = %q，期望 %q", got, want)
	}
}

// TestRunCanceled 验证调用方取消后示例返回取消错误且不输出查询结果。
func TestRunCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var output bytes.Buffer
	if err := run(ctx, &output); !errors.Is(err, context.Canceled) {
		t.Fatalf("错误 = %v，期望 context.Canceled", err)
	}
	if output.Len() != 0 {
		t.Fatalf("取消后仍有输出：%q", output.String())
	}
}

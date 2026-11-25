package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

// TestRun 验证完整示例可以在无外部依赖的情况下跑通，并抽查每类功能的输出。
func TestRun(t *testing.T) {
	var output bytes.Buffer
	if err := run(context.Background(), &output); err != nil {
		t.Fatal(err)
	}
	text := output.String()

	// 每类功能至少一个稳定标记，防止示例被裁剪成只覆盖单一接口。
	for _, marker := range []string{
		"=== 1/8 原生 Chunk 查询与全部类型访问器 ===",
		"i64",
		"=== 2/8 database/sql 行式接口与参数绑定 ===",
		"BeginTx 返回预期错误",
		"=== 3/8 SQL 查询能力 ===",
		"reservoir_rows",
		"=== 4/8 Go 表函数、标量 UDF、HMAC 与物化快照 ===",
		"两次重复扫描分别返回",
		"=== 5/8 本地文件、读取函数与结构化导出 ===",
		"目录外部表 2026-01-01 rows=2 total=30\n",
		"目录外部表 2026-01-02 rows=3 total=60\n",
		"目录外部表拒绝不兼容分片",
		"加密 Parquet 导出",
		"=== 6/8 Catalog DDL、DESCRIBE 与元数据 ===",
		"information_schema.columns",
		"=== 7/8 资源配置、受管内存与 S3 ===",
		"SampleCompatibility=duckdb",
		"=== 8/8 错误边界与上下文取消 ===",
		"全部示例执行完成。",
	} {
		if !strings.Contains(text, marker) {
			t.Fatalf("输出缺少标记 %q\n--- 输出 ---\n%s", marker, text)
		}
	}

	// HMAC 输出必须与标准库计算结果一致。
	mac := hmac.New(sha256.New, []byte("quickstart-demo-hmac-key"))
	_, _ = mac.Write([]byte("alice@example.com"))
	wantHMAC := hex.EncodeToString(mac.Sum(nil))
	if !strings.Contains(text, wantHMAC) {
		t.Fatalf("输出缺少 HMAC 结果 %q", wantHMAC)
	}

	// 示例中的“预期失败”断言不应被触发。
	if strings.Contains(text, "预期失败但成功执行") {
		t.Fatalf("出现意外的成功执行：\n%s", text)
	}
}

// TestRunCanceled 验证取消后示例返回 context.Canceled，且不写入任何结果。
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

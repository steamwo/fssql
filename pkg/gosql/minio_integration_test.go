package gosql_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/steamwo/fssql/pkg/gosql"
)

// minIOIntegrationConfig 仅在显式配置 MinIO 时启用真实服务测试，并限制为专用测试 bucket。
func minIOIntegrationConfig(t *testing.T) (gosql.Config, string) {
	t.Helper()
	endpoint := os.Getenv("FSSQL_MINIO_ENDPOINT")
	if endpoint == "" {
		t.Skip("设置 FSSQL_MINIO_ENDPOINT 等环境变量以启用真实 MinIO 集成测试")
	}
	bucket := os.Getenv("FSSQL_MINIO_BUCKET")
	if !strings.HasPrefix(bucket, "fssql-agent-test-") || strings.ContainsAny(bucket, "'/\\") {
		t.Fatal("FSSQL_MINIO_BUCKET 必须是 fssql-agent-test- 前缀的专用测试 bucket")
	}
	access, secret := os.Getenv("FSSQL_MINIO_ACCESS_KEY"), os.Getenv("FSSQL_MINIO_SECRET_KEY")
	if access == "" || secret == "" {
		t.Fatal("真实 MinIO 测试缺少访问凭据")
	}
	return gosql.Config{
		MemoryLimitBytes: 32 << 20,
		TempDir:          t.TempDir(),
		ManifestDir:      t.TempDir(),
		S3: &gosql.S3Config{
			Endpoint: endpoint, Region: "us-east-1", ForcePathStyle: true,
			AllowInsecureHTTP: true, AccessKey: access, SecretKey: secret,
			RequestTimeout: 10 * time.Second,
		},
	}, bucket
}

// TestMinIORead 验证公开 API 通过真实 S3 Range 请求读取 CSV/Parquet，并释放受管内存。
func TestMinIORead(t *testing.T) {
	for _, format := range []string{"csv", "parquet"} {
		t.Run(format, func(t *testing.T) {
			config, bucket := minIOIntegrationConfig(t)
			db, err := gosql.Open(config)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			row := 0
			labels := []string{"alpha", "beta", "gamma"}
			sql := fmt.Sprintf("SELECT id, label, amount FROM read_%s('s3://%s/input.%s') ORDER BY id", format, bucket, format)
			if format == "csv" {
				// 公共 CSV 装配的默认推断不启用 Header，按生成列名读取无表头样本。
				sql = fmt.Sprintf("SELECT column0 AS id, column1 AS label, column2 AS amount FROM read_csv('s3://%s/input.csv') ORDER BY id", bucket)
			}
			columns, err := db.QueryChunks(ctx, sql, func(chunk *gosql.Chunk) error {
				for index := 0; index < chunk.RowCount(); index++ {
					if row >= len(labels) {
						return fmt.Errorf("返回了额外的行")
					}
					id, valid, err := chunk.Int64(index, 0)
					if err != nil || !valid || id != int64(row+1) {
						return fmt.Errorf("id=%d valid=%v err=%v", id, valid, err)
					}
					label, valid, err := chunk.String(index, 1)
					if err != nil || !valid || label != labels[row] {
						return fmt.Errorf("label=%q valid=%v err=%v", label, valid, err)
					}
					amount, valid, err := chunk.Int64(index, 2)
					if err != nil || !valid || amount != int64((row+1)*10) {
						return fmt.Errorf("amount=%d valid=%v err=%v", amount, valid, err)
					}
					row++
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if row != 3 || len(columns) != 3 {
				t.Fatalf("rows=%d columns=%d", row, len(columns))
			}
			if used := db.MemoryUsed(); used != 0 {
				t.Fatalf("managed memory leak=%d", used)
			}
		})
	}
}

// TestMinIOAggregate 验证 database/sql 通过真实 MinIO 扫描执行聚合并保留整数精度。
func TestMinIOAggregate(t *testing.T) {
	config, bucket := minIOIntegrationConfig(t)
	db, err := gosql.OpenSQLDB(config)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var count int64
	var sum string
	sql := fmt.Sprintf("SELECT COUNT(*), SUM(amount) FROM read_parquet('s3://%s/input.parquet')", bucket)
	if err := db.QueryRowContext(ctx, sql).Scan(&count, &sum); err != nil {
		t.Fatal(err)
	}
	if count != 3 || sum != "60" {
		t.Fatalf("count=%d sum=%q", count, sum)
	}
}

// TestMinIOMissingObject 验证真实 404 返回失败且查询资源被释放。
func TestMinIOMissingObject(t *testing.T) {
	config, bucket := minIOIntegrationConfig(t)
	db, err := gosql.Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = db.QueryChunks(ctx, fmt.Sprintf("SELECT * FROM read_parquet('s3://%s/missing.parquet')", bucket), func(*gosql.Chunk) error {
		return fmt.Errorf("缺失对象不应返回结果")
	})
	if err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("预期真实 404，得到 %v", err)
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("managed memory leak=%d", used)
	}
}

// TestMinIOCopyRoundTrip 验证单文件 COPY 经真实 MinIO 发布后可由查询读取，持久化失败必须显式报错。
func TestMinIOCopyRoundTrip(t *testing.T) {
	config, bucket := minIOIntegrationConfig(t)
	db, err := gosql.Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	t.Cleanup(func() {
		if used := db.MemoryUsed(); used != 0 {
			t.Errorf("managed memory leak=%d", used)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	uri := fmt.Sprintf("s3://%s/output-%d.parquet", bucket, time.Now().UnixNano())
	if err := db.Exec(ctx, fmt.Sprintf("COPY (SELECT 42 AS answer) TO '%s' (FORMAT parquet)", uri)); err != nil {
		t.Fatal(err)
	}
	var rows int
	_, err = db.QueryChunks(ctx, fmt.Sprintf("SELECT answer FROM read_parquet('%s')", uri), func(chunk *gosql.Chunk) error {
		for row := 0; row < chunk.RowCount(); row++ {
			answer, valid, err := chunk.Int64(row, 0)
			if err != nil || !valid || answer != 42 {
				return fmt.Errorf("answer=%d valid=%v err=%v", answer, valid, err)
			}
			rows++
		}
		return nil
	})
	if err != nil || rows != 1 {
		t.Fatalf("rows=%d err=%v", rows, err)
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("managed memory leak=%d", used)
	}
}

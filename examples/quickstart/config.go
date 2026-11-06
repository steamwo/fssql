package main

import (
	"context"
	"io"
	"path/filepath"
	"time"

	"github.com/steamwo/fssql/pkg/gosql"
)

// demoConfig 演示资源配置、受管内存、S3 配置结构与采样兼容模式。
//
// Config 的零值会选择默认值；这里显式写出关键字段，便于对照文档：
//   - MemoryLimitBytes 是引擎受管内存上限，不是进程 RSS；
//   - TempLimitBytes 限制临时存储总量；
//   - SpillMemoryBudgetBytes 是落盘策略阈值，不是独立内存额度；
//   - Workers/BlockingQueue 控制并行度与队列深度；
//   - TempDir/ManifestDir 只配置 spill 与导出恢复状态，不启用本地数据读取；
//   - LocalFiles/S3 才是数据源开关，未配置的 scheme 会显式失败；
//   - ParquetKeys 是加密导出的密钥回调，不保存密钥字节。
func demoConfig(ctx context.Context, w io.Writer, env *environment) error {
	if err := writef(w, `-- 当前显式配置
   MemoryLimitBytes       = 64 MiB
   TempLimitBytes         = 256 MiB
   SpillMemoryBudgetBytes = 4 MiB
   SpillBlockBytes        = 64 KiB
   Workers                = 2
   BlockingQueue          = 2
   CatalogMaxEntries      = 64
   ParquetMetadataEntries = 16
   LocalFiles.RootDir     = %s
`, env.root); err != nil {
		return err
	}

	// 受管内存在所有查询/快照关闭后应回到 0。它不是 Go heap 或 RSS。
	if err := writef(w, "-- DB.MemoryUsed()（查询全部结束后）= %d 字节\n", env.db.MemoryUsed()); err != nil {
		return err
	}

	// S3 示例会真正 Open 一次以验证配置注册，但不会发起网络请求：
	// NewDriver 只校验 endpoint/region/凭证并构造 HTTP client。
	// 示例使用 http 端点，因此必须显式开启 AllowInsecureHTTP；生产环境应使用 https。
	s3 := &gosql.S3Config{
		Endpoint:          "http://127.0.0.1:9000",
		Region:            "us-east-1",
		ForcePathStyle:    true, // MinIO 等兼容端点通常需要
		AllowInsecureHTTP: true,
		AccessKey:         "example-access-key",
		SecretKey:         "example-secret-key",
		SessionToken:      "",
		CredentialScope:   "quickstart",
		CredentialVersion: "v1",
		SecurityProfile:   "quickstart",
		MaxRetries:        3,
		RequestTimeout:    5 * time.Second,
	}
	if err := writef(w, `-- S3Config 示例（Open 仅注册 driver，未发起网络请求）
   Endpoint=%s Region=%s ForcePathStyle=%v AllowInsecureHTTP=%v
   MaxRetries=%d RequestTimeout=%s
   AccessKey/SecretKey/SessionToken 不会被示例回显；只有 Config.S3 非 nil 时才注册 S3 driver。
`, s3.Endpoint, s3.Region, s3.ForcePathStyle, s3.AllowInsecureHTTP, s3.MaxRetries, s3.RequestTimeout); err != nil {
		return err
	}
	s3DB, err := gosql.Open(gosql.Config{
		S3:          s3,
		TempDir:     filepath.Join(env.root, "s3-spill"),
		ManifestDir: filepath.Join(env.root, "s3-manifests"),
	})
	if err != nil {
		return err
	}
	if err := s3DB.Close(); err != nil {
		return err
	}
	if err := writef(w, "   Config.S3 注册成功；s3:// 读取/导出现已可用。\n"); err != nil {
		return err
	}

	// SampleCompatibility 只影响采样流的参考引擎兼容性；空值保留原生流。
	// 输入顺序和 chunk 边界必须与参考引擎一致，才能复现相同样本。
	sampleDB, err := gosql.Open(gosql.Config{
		SampleCompatibility: "duckdb",
		TempDir:             filepath.Join(env.root, "sample-spill"),
		ManifestDir:         filepath.Join(env.root, "sample-manifests"),
	})
	if err != nil {
		return err
	}
	sampleCount, err := queryScalarInt64(ctx, sampleDB, `SELECT count(*)
		FROM (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) t
		USING SAMPLE reservoir(3 ROWS) REPEATABLE(42)`)
	closeErr := sampleDB.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := writef(w, "-- SampleCompatibility=duckdb 的 reservoir(3 ROWS) 计数 = %d\n", sampleCount); err != nil {
		return err
	}

	// 非法值在 Open 阶段被拒绝，不会带着错误配置进入执行。
	if _, err := gosql.Open(gosql.Config{SampleCompatibility: "postgres"}); err == nil {
		return errUnexpectedSuccess("非法 SampleCompatibility")
	}
	return writef(w, "-- 非法 SampleCompatibility 在 Open 阶段被拒绝。\n")
}

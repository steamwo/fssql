package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/steamwo/fssql/pkg/gosql"
)

// demoExportKey 只是示例密钥，生产环境应从 KMS/密钥服务获取并避免落盘。
var demoExportKey = bytes.Repeat([]byte{0x42}, 32)

// demoKeyResolver 是 Config.ParquetKeys 的实现：按 keyID+algorithm 返回密钥字节。
// AES-128-GCM 必须返回 16 字节，AES-256-GCM 必须返回 32 字节。
func demoKeyResolver(ctx context.Context, keyID, algorithm string) ([]byte, error) {
	if algorithm != gosql.ParquetAES256GCM {
		return nil, fmt.Errorf("unexpected key algorithm %q", algorithm)
	}
	return demoReadKeyResolver(ctx, keyID)
}

func demoReadKeyResolver(_ context.Context, keyID string) ([]byte, error) {
	if keyID != "quickstart-key" {
		return nil, fmt.Errorf("unknown key ID %q", keyID)
	}
	return append([]byte(nil), demoExportKey...), nil
}

// demoFiles 演示本地文件数据源、内建读取函数、结构化导出和外部表。
//
// 关键边界：
//   - LocalFiles.RootDir 必须显式配置；配置 TempDir/ManifestDir 本身不会
//     启用本地数据读取；
//   - 本地路径统一用 gosql.FileURI 生成规范 file:// URI（尤其 Windows）；
//   - read_csv 的静态选项只支持 header、auto_detect、allow_quoted_nulls、
//     columns；read_csv_auto/read_json_auto 是兼容别名；
//   - CopyTo 的目标必须由已配置的 VFS driver 支持；目标已存在时不会覆盖；
//   - 原生加密 Parquet 通过 ParquetKeys 导出，通过 ParquetReadKeys 读回。
func demoFiles(ctx context.Context, w io.Writer, env *environment) error {
	// 1. 写入一个 CSV 源文件。示例数据与 demo_rows() 保持一致的前四列。
	csvPath := filepath.Join(env.root, "input.csv")
	const csvData = "id,region,amount,note\n" +
		"1,east,10,alpha\n" +
		"2,east,20,beta\n" +
		"3,west,30,gamma\n" +
		"4,west,,delta\n"
	if err := os.WriteFile(csvPath, []byte(csvData), 0o600); err != nil {
		return err
	}
	csvURI, err := gosql.FileURI(csvPath)
	if err != nil {
		return err
	}

	// 2. read_csv：显式 schema（{name:type} 形式）避免自动推断。
	if err := printQuery(ctx, w, env.sqlDB,
		"read_csv 显式 schema（header + columns 对象）",
		fmt.Sprintf(`SELECT id, region, amount, note
		             FROM read_csv(%s, header=true,
		                  columns={'id':'BIGINT','region':'VARCHAR','amount':'BIGINT','note':'VARCHAR'})
		             ORDER BY id`, quoteSQL(csvURI)),
	); err != nil {
		return err
	}

	// 3. read_csv：字符串 schema + auto_detect=false；空字段按 NULL 处理。
	if err := printQuery(ctx, w, env.sqlDB,
		"read_csv 字符串 schema 与 NULL 处理",
		fmt.Sprintf(`SELECT count(*) AS rows,
		                    count(amount) AS non_null_amount,
		                    sum(amount) AS total
		             FROM read_csv(%s, auto_detect=false, header=true,
		                  columns='id BIGINT, region VARCHAR, amount BIGINT, note VARCHAR')`,
			quoteSQL(csvURI)),
	); err != nil {
		return err
	}

	// 4. DESCRIBE 只绑定 schema，不执行查询。
	if err := printQuery(ctx, w, env.sqlDB,
		"DESCRIBE read_csv 的列定义",
		fmt.Sprintf("DESCRIBE SELECT * FROM read_csv(%s, header=true)", quoteSQL(csvURI)),
	); err != nil {
		return err
	}

	// 导出的公共列集合（排除 BLOB，避免 JSON 文本编码的二进制歧义）。
	const exportQuery = `SELECT id, region, amount, note, flag, day FROM demo_rows() ORDER BY id`

	// 5. CopyTo CSV：Header/Delimiter/Quote/ForceQuote/DuckDBCompatible 组合。
	//    Header 使用 *bool，nil 时默认 true（与 SQL COPY 一致）。
	csvOut := filepath.Join(env.root, "export_options.csv")
	csvOutURI, err := gosql.FileURI(csvOut)
	if err != nil {
		return err
	}
	csvResult, err := env.db.CopyTo(ctx, exportQuery, csvOutURI, gosql.CopyOptions{
		Format:           gosql.CopyFormatCSV,
		Compression:      "identity",
		Header:           boolPtr(true),
		Delimiter:        ';',
		Quote:            '"',
		ForceQuote:       true,
		DuckDBCompatible: true,
	})
	if err != nil {
		return err
	}
	if err := writef(w, "-- CopyTo CSV（分隔符 ';'、强制引号）\n   rows=%d bytes=%d columns=%d\n",
		csvResult.Rows, csvResult.Bytes, len(csvResult.Columns)); err != nil {
		return err
	}
	if err := printFilePreview(w, csvOut, 2); err != nil {
		return err
	}

	// 6. 标准逗号 CSV 导出后用 read_csv 读回，验证往返一致。
	plainCSV := filepath.Join(env.root, "export_plain.csv")
	plainCSVURI, err := gosql.FileURI(plainCSV)
	if err != nil {
		return err
	}
	if _, err := env.db.CopyTo(ctx, exportQuery, plainCSVURI, gosql.CopyOptions{
		Format: gosql.CopyFormatCSV,
	}); err != nil {
		return err
	}
	if err := printQuery(ctx, w, env.sqlDB,
		"CSV 导出后 read_csv 读回",
		fmt.Sprintf("SELECT count(*) AS rows, sum(amount) AS total FROM read_csv(%s, header=true)", quoteSQL(plainCSVURI)),
	); err != nil {
		return err
	}

	// 7. JSON（数组）与 NDJSON（逐行）导出，再分别用 read_json / read_ndjson 读回。
	jsonOut := filepath.Join(env.root, "export.json")
	jsonURI, err := gosql.FileURI(jsonOut)
	if err != nil {
		return err
	}
	jsonResult, err := env.db.CopyTo(ctx, exportQuery, jsonURI, gosql.CopyOptions{
		Format:      gosql.CopyFormatJSON,
		Compression: "identity",
	})
	if err != nil {
		return err
	}
	if err := writef(w, "-- CopyTo JSON（数组）: rows=%d bytes=%d\n", jsonResult.Rows, jsonResult.Bytes); err != nil {
		return err
	}
	if err := printQuery(ctx, w, env.sqlDB,
		"read_json 读回 JSON 数组",
		fmt.Sprintf("SELECT count(*) AS rows, sum(amount) AS total FROM read_json(%s)", quoteSQL(jsonURI)),
	); err != nil {
		return err
	}

	ndjsonOut := filepath.Join(env.root, "export.ndjson")
	ndjsonURI, err := gosql.FileURI(ndjsonOut)
	if err != nil {
		return err
	}
	ndjsonResult, err := env.db.CopyTo(ctx, exportQuery, ndjsonURI, gosql.CopyOptions{
		Format:      gosql.CopyFormatNDJSON,
		Compression: "identity",
	})
	if err != nil {
		return err
	}
	if err := writef(w, "-- CopyTo NDJSON: rows=%d bytes=%d\n", ndjsonResult.Rows, ndjsonResult.Bytes); err != nil {
		return err
	}
	if err := printQuery(ctx, w, env.sqlDB,
		"read_ndjson 读回逐行 JSON",
		fmt.Sprintf("SELECT count(*) AS rows, sum(amount) AS total FROM read_ndjson(%s)", quoteSQL(ndjsonURI)),
	); err != nil {
		return err
	}

	// 8. Parquet 导出（页压缩 snappy），再用 WHERE 触发投影/谓词下推路径。
	parquetOut := filepath.Join(env.root, "export.parquet")
	parquetURI, err := gosql.FileURI(parquetOut)
	if err != nil {
		return err
	}
	parquetResult, err := env.db.CopyTo(ctx, exportQuery, parquetURI, gosql.CopyOptions{
		Format:      gosql.CopyFormatParquet,
		Compression: "snappy",
	})
	if err != nil {
		return err
	}
	if err := writef(w, "-- CopyTo Parquet（snappy）: rows=%d bytes=%d\n", parquetResult.Rows, parquetResult.Bytes); err != nil {
		return err
	}
	if err := printQuery(ctx, w, env.sqlDB,
		"read_parquet 过滤读取",
		fmt.Sprintf("SELECT id, amount FROM read_parquet(%s) WHERE amount >= 20 ORDER BY id", quoteSQL(parquetURI)),
	); err != nil {
		return err
	}

	// 9. SQL COPY 走与 CopyTo 相同的 Export/VFS 原子提交路径。
	sqlCopyOut := filepath.Join(env.root, "copy_sql.parquet")
	sqlCopyURI, err := gosql.FileURI(sqlCopyOut)
	if err != nil {
		return err
	}
	if err := env.db.Exec(ctx, fmt.Sprintf(
		"COPY (%s) TO %s (FORMAT PARQUET, COMPRESSION SNAPPY)",
		exportQuery, quoteSQL(sqlCopyURI))); err != nil {
		return err
	}
	if err := writef(w, "-- SQL COPY 完成：%s（%d 字节）\n", filepath.Base(sqlCopyOut), fileSize(sqlCopyOut)); err != nil {
		return err
	}

	// 10. CREATE EXTERNAL TABLE 使用真实表函数 Bind 得到 schema，不保存 SQL 文本。
	if _, err := env.sqlDB.ExecContext(ctx,
		"CREATE EXTERNAL TABLE quickstart_events USING read_parquet("+quoteSQL(parquetURI)+")"); err != nil {
		return err
	}
	if err := printQuery(ctx, w, env.sqlDB,
		"CREATE EXTERNAL TABLE 后查询",
		`SELECT count(*) AS rows, sum(amount) AS total
		 FROM quickstart_events
		 WHERE flag = true`,
	); err != nil {
		return err
	}

	// 11. glob() 冻结文件集合并返回单列 file。
	dirURI, err := gosql.FileURI(env.root)
	if err != nil {
		return err
	}
	pattern := strings.TrimSuffix(dirURI, "/") + "/*.parquet"
	if err := printQuery(ctx, w, env.sqlDB,
		"glob 列出工作区 Parquet 文件",
		fmt.Sprintf("SELECT file FROM glob(%s) ORDER BY file", quoteSQL(pattern)),
	); err != nil {
		return err
	}

	// 12. 加密 Parquet 导出：key identity 写入清单，key bytes 只来自回调。
	//     ParquetKeys 必须按 keyID+algorithm 返回 16 字节（AES-128-GCM）或
	//     32 字节（AES-256-GCM）；SQL 与清单中不会出现密钥。
	encryptedOut := filepath.Join(env.root, "export_encrypted.parquet")
	encryptedURI, err := gosql.FileURI(encryptedOut)
	if err != nil {
		return err
	}
	encryptedResult, err := env.db.CopyTo(ctx, exportQuery, encryptedURI, gosql.CopyOptions{
		Format:      gosql.CopyFormatParquet,
		Compression: "snappy",
		Encryption: &gosql.ParquetEncryptionOptions{
			Algorithm: gosql.ParquetAES256GCM,
			KeyID:     "quickstart-key",
		},
	})
	if err != nil {
		return err
	}
	if err := writef(w, "-- 加密 Parquet 导出: rows=%d bytes=%d\n",
		encryptedResult.Rows, encryptedResult.Bytes); err != nil {
		return err
	}
	if err := printQuery(ctx, w, env.sqlDB, "读回加密 Parquet",
		"SELECT id, note FROM read_parquet("+quoteSQL(encryptedURI)+") ORDER BY id"); err != nil {
		return err
	}

	// 13. 文本格式整文件压缩：CSV 支持 identity/gzip/zstd；Parquet 只支持
	//     identity/snappy 页压缩。压缩文件不在本示例中读回，只展示大小。
	gzipOut := filepath.Join(env.root, "export.csv.gz")
	gzipURI, err := gosql.FileURI(gzipOut)
	if err != nil {
		return err
	}
	if _, err := env.db.CopyTo(ctx, exportQuery, gzipURI, gosql.CopyOptions{
		Format:      gosql.CopyFormatCSV,
		Compression: "gzip",
	}); err != nil {
		return err
	}
	zstdOut := filepath.Join(env.root, "export.ndjson.zst")
	zstdURI, err := gosql.FileURI(zstdOut)
	if err != nil {
		return err
	}
	if _, err := env.db.CopyTo(ctx, exportQuery, zstdURI, gosql.CopyOptions{
		Format:      gosql.CopyFormatNDJSON,
		Compression: "zstd",
	}); err != nil {
		return err
	}
	return writef(w, "-- 文本压缩导出: %s=%d 字节，%s=%d 字节\n",
		filepath.Base(gzipOut), fileSize(gzipOut), filepath.Base(zstdOut), fileSize(zstdOut))
}

// printFilePreview 打印文件前若干行，用于展示导出选项的实际效果。
func printFilePreview(w io.Writer, path string, lines int) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	text := string(data)
	parts := strings.SplitN(text, "\n", lines+1)
	if len(parts) > lines {
		parts = parts[:lines]
	}
	return writef(w, "   文件预览：\n%s\n", indent(strings.Join(parts, "\n"), "     "))
}

func indent(text, prefix string) string {
	return prefix + strings.ReplaceAll(text, "\n", "\n"+prefix)
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

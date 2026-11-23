// Package main 是 fssql 公共包 github.com/steamwo/fssql/pkg/gosql 的完整快速入门。
//
// 示例按功能分组覆盖当前公共 API：
//
//  1. 原生 Chunk 回调查询、列元数据与全部类型访问器；
//  2. database/sql 行式接口、参数绑定、预编译语句与类型映射；
//  3. SQL 查询能力：CTE、JOIN、聚合、窗口、采样、子查询、LATERAL、UNION ALL 等；
//  4. Go 表函数、标量 UDF、HMAC 辅助函数与 Materialize 物化快照；
//  5. 本地文件、内建读取函数、四种导出格式、SQL COPY、外部表、glob 与加密导出；
//  6. Catalog DDL、DESCRIBE、information_schema.columns 与 CREATE SECRET；
//  7. 资源配置、受管内存、S3 配置结构与 SampleCompatibility；
//  8. 事务、取消、类型误用等边界行为。
//
// 运行 `go run ./examples/quickstart` 会在系统临时目录中创建隔离工作区，
// 不需要外部数据源或凭据；运行结束后自动清理。S3 段落只展示配置结构，
// 不会发起网络请求。每个 demo 函数的注释说明它演示的 API 与注意事项。
package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/steamwo/fssql/pkg/gosql"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		log.Fatal(err)
	}
}

// run 依次执行全部示例，并把结果写入 output。
//
// 测试通过注入 bytes.Buffer 校验输出；调用方取消 ctx 后，run 会在写入任何
// 内容前返回 context.Canceled，便于验证取消边界。
func run(ctx context.Context, output io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	root, err := os.MkdirTemp("", "fssql-quickstart-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)

	env, err := openEnvironment(root)
	if err != nil {
		return err
	}
	defer env.close()

	demos := []struct {
		title string
		run   func(context.Context, io.Writer, *environment) error
	}{
		{"原生 Chunk 查询与全部类型访问器", demoNativeChunks},
		{"database/sql 行式接口与参数绑定", demoDatabaseSQL},
		{"SQL 查询能力", demoSQLFeatures},
		{"Go 表函数、标量 UDF、HMAC 与物化快照", demoFunctions},
		{"本地文件、读取函数与结构化导出", demoFiles},
		{"Catalog DDL、DESCRIBE 与元数据", demoCatalog},
		{"资源配置、受管内存与 S3", demoConfig},
		{"错误边界与上下文取消", demoBoundaries},
	}
	for i, demo := range demos {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := writef(output, "\n=== %d/%d %s ===\n", i+1, len(demos), demo.title); err != nil {
			return err
		}
		if err := demo.run(ctx, output, env); err != nil {
			return fmt.Errorf("%s: %w", demo.title, err)
		}
	}
	return writef(output, "\n全部示例执行完成。\n")
}

// environment 持有同一次运行中的原生 DB 与 database/sql 句柄。
//
// 两者使用相同配置分别 Open，因此拥有独立的 MemoryCatalog；示例只共享
// 工作区文件，不假设两个实例共享 SQL 对象。TableFunction 的 Open/Close
// 计数按实例分开统计，便于准确展示生命周期。
type environment struct {
	root      string
	db        *gosql.DB
	sqlDB     *sql.DB
	source    *sourceStats
	sqlSource *sourceStats
}

func openEnvironment(root string) (*environment, error) {
	env := &environment{
		root:      root,
		source:    &sourceStats{},
		sqlSource: &sourceStats{},
	}

	// HMAC 辅助函数把密钥复制进 DB 作用域的函数定义；SQL/计划中不会出现密钥。
	hmacFunction, err := gosql.NewHMACSHA256ScalarFunction([]byte("quickstart-demo-hmac-key"))
	if err != nil {
		return nil, err
	}

	nativeConfig := baseConfig(root)
	nativeConfig.TableFunctions = []gosql.TableFunction{demoRowsTableFunction(env.source)}
	nativeConfig.ScalarFunctions = demoScalarFunctions(hmacFunction)
	nativeConfig.ParquetKeys = demoKeyResolver
	native, err := gosql.Open(nativeConfig)
	if err != nil {
		return nil, err
	}

	sqlConfig := baseConfig(root)
	sqlConfig.TableFunctions = []gosql.TableFunction{demoRowsTableFunction(env.sqlSource)}
	sqlConfig.ScalarFunctions = demoScalarFunctions(hmacFunction)
	sqlConfig.ParquetKeys = demoKeyResolver
	sqlConfig.ParquetReadKeys = demoReadKeyResolver
	sqlDB, err := gosql.OpenSQLDB(sqlConfig)
	if err != nil {
		_ = native.Close()
		return nil, err
	}
	env.db = native
	env.sqlDB = sqlDB
	return env, nil
}

func (e *environment) close() {
	if e.sqlDB != nil {
		_ = e.sqlDB.Close()
	}
	if e.db != nil {
		_ = e.db.Close()
	}
}

// baseConfig 返回示例共用的资源配置。
//
// 这些字段只限制引擎受管内存、临时存储、spill、worker 与队列；LocalFiles
// 才是本地文件读取的显式开关。SpillMemoryBudgetBytes 是运维策略，不是额外
// 内存额度，所有受管分配仍共享 MemoryLimitBytes。
func baseConfig(root string) gosql.Config {
	return gosql.Config{
		MemoryLimitBytes:       64 << 20,  // 64 MiB 受管内存上限
		TempLimitBytes:         256 << 20, // 256 MiB 临时存储上限
		SpillMemoryBudgetBytes: 4 << 20,   // 4 MiB 后倾向落盘
		SpillBlockBytes:        64 << 10,  // 64 KiB spill 块
		TempDir:                filepath.Join(root, "spill"),
		ManifestDir:            filepath.Join(root, "manifests"),
		CatalogMaxEntries:      64,
		ParquetMetadataEntries: 16,
		Workers:                2,
		BlockingQueue:          2,
		// 只开放 root 以下的本地文件；未配置时 file:// 读取/导出会显式失败。
		LocalFiles: &gosql.LocalFileConfig{RootDir: root},
	}
}

// writef 统一处理输出错误：调用方的 writer 关闭后示例应立即停止。
func writef(w io.Writer, format string, args ...any) error {
	_, err := fmt.Fprintf(w, format, args...)
	return err
}

// quoteSQL 把字符串转成 SQL 字符串字面量。示例中的 URI 可能包含反斜杠或
// 单引号，必须转义；生产代码应优先使用 database/sql 参数绑定值。
func quoteSQL(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// boolPtr 用于 CopyOptions.Header 这类“未设置时取默认值”的可选布尔字段。
func boolPtr(value bool) *bool { return &value }

// formatValue 把 database/sql 扫描出的值格式化为稳定文本。
//
// DECIMAL/HUGEINT/UBIGINT 由适配器返回精确十进制文本；DATE/TIMESTAMP 返回
// time.Time；BLOB 返回 []byte 副本。示例显式区分这些表示，避免把文本再
// 转成浮点数而丢失精度。
func formatValue(value any) string {
	switch v := value.(type) {
	case nil:
		return "NULL"
	case []byte:
		return fmt.Sprintf("% x", v)
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano)
	case string:
		return strconv.Quote(v)
	default:
		return fmt.Sprint(v)
	}
}

// printQuery 通过 database/sql 执行 SELECT，并按“列名 | 值”打印结果。
//
// 参数使用 database/sql 的绑定语义：?、连续编号 $1/$2 或 sql.Named 的
// $name；示例不混用占位符样式。Rows.Close 是行式结果的生命周期边界。
func printQuery(ctx context.Context, w io.Writer, db *sql.DB, label, query string, args ...any) error {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}
	if err := writef(w, "-- %s\n", label); err != nil {
		return err
	}
	if err := writef(w, "   %s\n", strings.Join(columns, " | ")); err != nil {
		return err
	}
	for rows.Next() {
		values := make([]any, len(columns))
		dest := make([]any, len(columns))
		for i := range dest {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		cells := make([]string, len(columns))
		for i, value := range values {
			cells[i] = formatValue(value)
		}
		if err := writef(w, "   %s\n", strings.Join(cells, " | ")); err != nil {
			return err
		}
	}
	return rows.Err()
}

// queryScalarInt64 用原生接口读取单行单列的 BIGINT 结果，供不涉及打印的
// 校验型示例使用。Chunk 只在回调期间有效，因此这里立即复制出 int64 值。
func queryScalarInt64(ctx context.Context, db *gosql.DB, query string) (int64, error) {
	var result int64
	_, err := db.QueryChunks(ctx, query, func(chunk *gosql.Chunk) error {
		if chunk.RowCount() != 1 || chunk.ColumnCount() != 1 {
			return fmt.Errorf("期望 1 行 1 列，实际 %d 行 %d 列", chunk.RowCount(), chunk.ColumnCount())
		}
		value, valid, err := chunk.Int64(0, 0)
		if err != nil {
			return err
		}
		if !valid {
			return fmt.Errorf("期望非 NULL")
		}
		result = value
		return nil
	})
	return result, err
}

package main

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/steamwo/fssql/pkg/gosql"
)

// demoNativeChunks 演示 DB.QueryChunks 的原生回调接口和 Chunk 的全部公共访问器。
//
// QueryChunks 走生产 Parser → Binder → Planner → Push-Pipeline 路径。回调收到
// 的 *Chunk 是借用视图：只能在回调期间读取，不能保存到回调之外。因此示例在
// 回调内把每个值复制成字符串，回调结束后再用 QueryChunks 返回的 []Column
// 元数据（列名、逻辑类型、可空性）打印。
//
// 类型与访问器对应关系：
//   - TINYINT/SMALLINT/INTEGER/BIGINT/DATE/TIMESTAMP → Int64
//   - UTINYINT/USMALLINT/UINTEGER/UBIGINT             → Uint64
//   - FLOAT/DOUBLE                                     → Float64
//   - BOOLEAN                                          → Bool
//   - VARCHAR                                          → String
//   - BLOB                                             → Bytes（返回副本，可跨回调保留）
//   - HUGEINT 与宽度 > 18 的 DECIMAL                   → Int128
//   - 宽度 ≤ 18 的 DECIMAL                             → Int64（未缩放存储值）
//
// NULL 必须通过 IsNull 或访问器的 valid 返回值判断，不能用零值代替。
func demoNativeChunks(ctx context.Context, w io.Writer, env *environment) error {
	const query = `SELECT
		CAST(1 AS TINYINT) AS i8,
		CAST(2 AS SMALLINT) AS i16,
		CAST(3 AS INTEGER) AS i32,
		CAST(4 AS BIGINT) AS i64,
		CAST(5 AS UTINYINT) AS u8,
		CAST(6 AS USMALLINT) AS u16,
		CAST(7 AS UINTEGER) AS u32,
		CAST(8 AS UBIGINT) AS u64,
		CAST(1.5 AS FLOAT) AS f32,
		CAST(2.5 AS DOUBLE) AS f64,
		CAST(true AS BOOLEAN) AS flag,
		'hello' AS text_value,
		CAST('abc' AS BLOB) AS blob_value,
		CAST('12345678901234567890.12' AS DECIMAL(22,2)) AS wide_decimal,
		CAST('170141183460469231731687303715884105727' AS HUGEINT) AS huge_value,
		CAST('2026-01-02' AS DATE) AS day_value,
		CAST('2026-01-02 03:04:05.123456789' AS TIMESTAMP_NS) AS ns_value,
		CAST(NULL AS BIGINT) AS missing_value`

	// 每个单元格在回调内立即转成文本；captured 只保存副本，不持有 Chunk。
	var captured [][]string
	columns, err := env.db.QueryChunks(ctx, query, func(chunk *gosql.Chunk) error {
		for row := 0; row < chunk.RowCount(); row++ {
			cells := make([]string, chunk.ColumnCount())
			for col := 0; col < chunk.ColumnCount(); col++ {
				typ, err := chunk.Type(col)
				if err != nil {
					return err
				}
				text, err := formatChunkCell(chunk, row, col, typ)
				if err != nil {
					return fmt.Errorf("第 %d 行第 %d 列: %w", row, col, err)
				}
				cells[col] = text
			}
			captured = append(captured, cells)
		}
		return nil
	})
	if err != nil {
		return err
	}

	if err := writef(w, "QueryChunks 返回 %d 个列描述，共 %d 行。\n", len(columns), len(captured)); err != nil {
		return err
	}
	for row, cells := range captured {
		if err := writef(w, "第 %d 行：\n", row+1); err != nil {
			return err
		}
		for col, column := range columns {
			nullable := "NOT NULL"
			if column.Nullable {
				nullable = "NULL"
			}
			if err := writef(w, "  %-16s %-14s %-8s = %s\n", column.Name, column.Type.ID, nullable, cells[col]); err != nil {
				return err
			}
		}
	}
	return writef(w, "说明：DECIMAL 访问器返回未缩放整数；宽 DECIMAL/HUGEINT 用 Int128.String() 得到精确十进制。\n")
}

// formatChunkCell 按逻辑类型选择对应的 Chunk 访问器。
//
// 这是公共 API 的类型分派样例：访问器不匹配（例如对 UTINYINT 调用 Int64）
// 会返回错误，而不是静默转换。DECIMAL 的返回值为未缩放存储值，需要按
// Type.Scale 格式化。
func formatChunkCell(chunk *gosql.Chunk, row, col int, typ gosql.Type) (string, error) {
	null, err := chunk.IsNull(row, col)
	if err != nil {
		return "", err
	}
	if null {
		return "NULL", nil
	}
	switch typ.ID {
	case "BOOLEAN":
		value, _, err := chunk.Bool(row, col)
		return strconv.FormatBool(value), err
	case "TINYINT", "SMALLINT", "INTEGER", "BIGINT":
		value, _, err := chunk.Int64(row, col)
		return strconv.FormatInt(value, 10), err
	case "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT":
		value, _, err := chunk.Uint64(row, col)
		return strconv.FormatUint(value, 10), err
	case "FLOAT", "DOUBLE":
		value, _, err := chunk.Float64(row, col)
		return strconv.FormatFloat(value, 'g', -1, 64), err
	case "VARCHAR":
		value, _, err := chunk.String(row, col)
		return strconv.Quote(value), err
	case "BLOB":
		value, _, err := chunk.Bytes(row, col)
		return fmt.Sprintf("% x", value), err
	case "HUGEINT":
		value, _, err := chunk.Int128(row, col)
		return value.String(), err
	case "DECIMAL":
		if typ.Width <= 18 {
			value, _, err := chunk.Int64(row, col)
			return fmt.Sprintf("%d（未缩放，scale=%d）", value, typ.Scale), err
		}
		value, _, err := chunk.Int128(row, col)
		return fmt.Sprintf("%s（未缩放，scale=%d）", value.String(), typ.Scale), err
	case "DATE":
		// DATE 的原生表示是 Unix epoch 起的天数。
		value, _, err := chunk.Int64(row, col)
		return fmt.Sprintf("%d（%s）", value, time.Unix(value*86400, 0).UTC().Format("2006-01-02")), err
	case "TIMESTAMP_NS":
		// 显式 CAST 成 TIMESTAMP_NS 后，原生单位固定为纳秒。
		value, _, err := chunk.Int64(row, col)
		return fmt.Sprintf("%d（%s）", value, time.Unix(0, value).UTC().Format(time.RFC3339Nano)), err
	case "TIMESTAMP", "TIMESTAMPTZ":
		// 其他时间戳的原生单位取决于源逻辑类型，这里只展示原始整数。
		value, _, err := chunk.Int64(row, col)
		return fmt.Sprintf("%d（原生整数，单位取决于逻辑类型）", value), err
	default:
		// LIST/STRUCT 只公开类型元数据，当前 Chunk 没有嵌套元素访问器。
		return "", fmt.Errorf("类型 %s 没有公共标量访问器", typ.ID)
	}
}

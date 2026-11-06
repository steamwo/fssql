package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/steamwo/fssql/pkg/gosql"
)

// demoDatabaseSQL 演示 gosql.OpenSQLDB 返回的 *sql.DB 接口。
//
// OpenSQLDB 使用 sql.OpenDB + 私有 Connector，不注册进程级 driver 名称，
// 底层仍走同一套原生执行路径。当前边界：
//   - SELECT 支持 ?、连续编号 $1/$2、以及 sql.Named 对应的 $name；
//   - 参数只作为 AST 值绑定，不会拼接进 SQL，不能替换表名或列名；
//   - PrepareContext 缓存解析语法，每次执行重新绑定参数并取语句快照；
//   - 事务（Begin/BeginTx）明确不支持；
//   - RowsAffected 目前返回 0，需要导出计数时使用 DB.CopyTo；
//   - DECIMAL/HUGEINT/UBIGINT 以精确十进制文本返回，DATE/TIMESTAMP 以
//     time.Time 返回，LIST/STRUCT 非 NULL 值会被行适配器拒绝。
func demoDatabaseSQL(ctx context.Context, w io.Writer, env *environment) error {
	// 1. 位置参数 ?：值作为参数绑定，而不是字符串拼接。
	if err := printQuery(ctx, w, env.sqlDB,
		"位置参数 ?（按 region 过滤）",
		"SELECT id, region, amount FROM demo_rows() WHERE region = ? ORDER BY id",
		"east",
	); err != nil {
		return err
	}

	// 2. 编号参数 $1：编号必须从 $1 开始连续。
	if err := printQuery(ctx, w, env.sqlDB,
		"编号参数 $1/$2",
		"SELECT $1 AS first_value, $2 AS second_value",
		int64(7), "seven",
	); err != nil {
		return err
	}

	// 3. 命名参数：sql.Named("amount", ...) 对应 $amount。
	stmt, err := env.sqlDB.PrepareContext(ctx, "SELECT CAST($amount AS DECIMAL(38,2)) AS amount")
	if err != nil {
		return err
	}
	defer stmt.Close()
	var decimalText string
	if err := stmt.QueryRowContext(ctx, sql.Named("amount", "12345678901234567890.12")).Scan(&decimalText); err != nil {
		return err
	}
	if err := writef(w, "-- 命名参数 + DECIMAL 精确文本\n   amount = %s\n", decimalText); err != nil {
		return err
	}

	// 4. 类型映射：宽整数与 DECIMAL 以文本返回，日期/时间以 time.Time 返回。
	if err := printQuery(ctx, w, env.sqlDB,
		"行式结果的类型映射",
		`SELECT CAST('18446744073709551615' AS UBIGINT) AS ubig_max,
		        CAST('170141183460469231731687303715884105727' AS HUGEINT) AS huge_max,
		        CAST('1234.56' AS DECIMAL(10,2)) AS decimal_value,
		        CAST('2026-01-02' AS DATE) AS day_value,
		        CAST('2026-01-02 03:04:05.123456789' AS TIMESTAMP_NS) AS ns_value`,
	); err != nil {
		return err
	}

	// 5. ColumnTypes 暴露数据库类型名与可空性；精度/Go scan type 不由适配器补充。
	rows, err := env.sqlDB.QueryContext(ctx, "SELECT id, amount, region FROM demo_rows() LIMIT 1")
	if err != nil {
		return err
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		_ = rows.Close()
		return err
	}
	if err := writef(w, "-- Rows.ColumnTypes()\n"); err != nil {
		_ = rows.Close()
		return err
	}
	for _, columnType := range columnTypes {
		nullable, known := columnType.Nullable()
		if err := writef(w, "   %-8s database_type=%-8s nullable=%v known=%v\n", columnType.Name(), columnType.DatabaseTypeName(), nullable, known); err != nil {
			_ = rows.Close()
			return err
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}

	// 6. 预编译语句可重复执行；每次执行绑定自己的参数并重新规划。
	prepared, err := env.sqlDB.PrepareContext(ctx, "SELECT count(*) FROM demo_rows() WHERE amount >= ?")
	if err != nil {
		return err
	}
	defer prepared.Close()
	for _, threshold := range []int64{20, 40} {
		var count int64
		if err := prepared.QueryRowContext(ctx, threshold).Scan(&count); err != nil {
			return err
		}
		if err := writef(w, "-- 预编译语句（amount >= %d）: count = %d\n", threshold, count); err != nil {
			return err
		}
	}

	// 7. 事务是明确的边界，不是“暂未测试”的隐式行为。
	tx, err := env.sqlDB.BeginTx(ctx, nil)
	if err == nil {
		_ = tx.Rollback()
		return fmt.Errorf("事务意外可用：当前公共 API 应明确拒绝 BeginTx")
	}
	if err := writef(w, "-- BeginTx 返回预期错误：%v\n", err); err != nil {
		return err
	}

	// 8. 未配置的 scheme 不会被静默启用。
	if _, err := env.db.QueryChunks(ctx, "SELECT * FROM read_parquet('s3://bucket/data.parquet')", func(*gosql.Chunk) error { return nil }); err == nil {
		return fmt.Errorf("未配置 S3 driver 时 read_parquet(s3://...) 应失败")
	}
	return writef(w, "-- 未配置 S3 driver 时读取 s3:// 会显式失败（配置方式见第 7 节）。\n")
}

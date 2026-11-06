package main

import (
	"context"
	"io"
)

// demoSQLFeatures 演示当前实现的 SQL 能力边界。
//
// 示例使用第 4 节注册的 demo_rows() 表函数（id、region、amount、note、
// flag、day、raw），不依赖外部文件。每个查询对应一类能力：
//   - CTE、GROUP BY/HAVING、ORDER BY/LIMIT/OFFSET；
//   - INNER/LEFT/RIGHT/FULL/CROSS/SEMI/ANTI JOIN 与 USING 合并键；
//   - COUNT/SUM/AVG/MIN/MAX 及 DISTINCT 聚合；
//   - ROW_NUMBER 与 COUNT/SUM 的 OVER 窗口；
//   - TABLESAMPLE / USING SAMPLE 的行数、百分比与种子；
//   - 标量、EXISTS、IN 与相关子查询；
//   - 显式/隐式 LATERAL 派生表；
//   - UNION ALL、BETWEEN、LIKE/ESCAPE、IS [NOT] DISTINCT FROM；
//   - HUGEINT/UBIGINT/DECIMAL 精确数值。
//
// 未实现的语法（普通 UNION 去重、INTERSECT/EXCEPT、量化 ANY/ALL、递归
// CTE、UPDATE/DELETE 等）会在第 8 节演示其显式失败行为。
func demoSQLFeatures(ctx context.Context, w io.Writer, env *environment) error {
	// CTE + 聚合 + HAVING + 排序 + LIMIT。
	if err := printQuery(ctx, w, env.sqlDB,
		"CTE、GROUP BY/HAVING、ORDER BY/LIMIT",
		`WITH priced AS (
		     SELECT region, id, amount
		     FROM demo_rows()
		     WHERE amount IS NOT NULL
		 )
		 SELECT region,
		        count(*) AS row_count,
		        sum(amount) AS total,
		        avg(amount) AS average
		 FROM priced
		 GROUP BY region
		 HAVING count(*) >= 1
		 ORDER BY total DESC, region
		 LIMIT 3`,
	); err != nil {
		return err
	}

	// DISTINCT 聚合：COUNT/SUM/AVG 可共享同一参数的去重输入。
	if err := printQuery(ctx, w, env.sqlDB,
		"DISTINCT 聚合与普通聚合",
		`SELECT count(*) AS rows,
		        count(DISTINCT region) AS distinct_regions,
		        count(DISTINCT amount) AS distinct_amounts,
		        sum(DISTINCT amount) AS distinct_total
		 FROM demo_rows()`,
	); err != nil {
		return err
	}

	// 显式 FULL JOIN + USING：合并键可用不带限定符的 k 引用，
	// 同时保留两侧的限定列 l.k / r.k。
	if err := printQuery(ctx, w, env.sqlDB,
		"FULL JOIN USING 合并键",
		`WITH left_rows AS (
		     SELECT 1 AS k, 'L1' AS l
		     UNION ALL SELECT 2, 'L2'
		     UNION ALL SELECT 3, 'L3'
		 ),
		 right_rows AS (
		     SELECT 2 AS k, 'R2' AS r
		     UNION ALL SELECT 3, 'R3'
		     UNION ALL SELECT 4, 'R4'
		 )
		 SELECT k, l.k AS left_key, r.k AS right_key, l, r
		 FROM left_rows l FULL JOIN right_rows r USING (k)
		 ORDER BY k`,
	); err != nil {
		return err
	}

	// 窗口：ROW_NUMBER 分区编号；SUM 的显式 ROWS frame 只累加前一行。
	// SUM(BIGINT) 返回 HUGEINT，行式接口以精确十进制文本返回。
	if err := printQuery(ctx, w, env.sqlDB,
		"ROW_NUMBER 与显式 ROWS frame 窗口",
		`SELECT region,
		        id,
		        ROW_NUMBER() OVER (PARTITION BY region ORDER BY id) AS row_number,
		        SUM(amount) OVER (
		            PARTITION BY region
		            ORDER BY id
		            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
		        ) AS prior_sum
		 FROM demo_rows()
		 WHERE amount IS NOT NULL
		 ORDER BY region, id`,
	); err != nil {
		return err
	}

	// 全局窗口：COUNT(*) OVER () 不重算上游；每行看到同样的总行数。
	if err := printQuery(ctx, w, env.sqlDB,
		"全局 COUNT(*) OVER ()",
		`SELECT id, COUNT(*) OVER () AS total_rows
		 FROM demo_rows()
		 ORDER BY id`,
	); err != nil {
		return err
	}

	// 采样：固定行数默认 reservoir；百分比默认 system；REPEATABLE 固定种子。
	// 固定行数保证结果行数，因此示例打印 count(*) 而非具体 id。
	if err := printQuery(ctx, w, env.sqlDB,
		"USING SAMPLE 行数/百分比与 REPEATABLE 种子",
		`SELECT
		    (SELECT count(*) FROM demo_rows() USING SAMPLE reservoir(3 ROWS) REPEATABLE(42)) AS reservoir_rows,
		    (SELECT count(*) FROM demo_rows() USING SAMPLE bernoulli(100 PERCENT) REPEATABLE(42)) AS bernoulli_rows,
		    (SELECT count(*) FROM demo_rows() TABLESAMPLE reservoir(2 ROWS) REPEATABLE(42)) AS tablesample_rows`,
	); err != nil {
		return err
	}

	// 非相关标量子查询：零行返回 NULL，多行报错。
	if err := printQuery(ctx, w, env.sqlDB,
		"非相关标量子查询",
		`SELECT id, amount
		 FROM demo_rows()
		 WHERE amount > (SELECT avg(amount) FROM demo_rows())
		 ORDER BY amount`,
	); err != nil {
		return err
	}

	// IN 子查询与 EXISTS；相关 EXISTS 使用等值/参数域去相关。
	if err := printQuery(ctx, w, env.sqlDB,
		"IN 子查询与相关 EXISTS",
		`SELECT d.id,
		        d.region,
		        d.region IN (SELECT x.region FROM demo_rows() x WHERE x.amount > 40) AS has_high_value,
		        EXISTS (SELECT 1 FROM demo_rows() x WHERE x.region = d.region AND x.amount > d.amount) AS has_larger
		 FROM demo_rows() d
		 WHERE d.amount IS NOT NULL
		 ORDER BY d.id`,
	); err != nil {
		return err
	}

	// LATERAL 派生表：右侧子查询引用左侧 d.id / d.region。
	if err := printQuery(ctx, w, env.sqlDB,
		"LEFT JOIN LATERAL 相关派生表",
		`SELECT d.id,
		        d.region,
		        t.rows_so_far
		 FROM demo_rows() d
		 LEFT JOIN LATERAL (
		     SELECT count(*) AS rows_so_far
		     FROM demo_rows() x
		     WHERE x.region = d.region AND x.id <= d.id
		 ) t ON TRUE
		 ORDER BY d.id`,
	); err != nil {
		return err
	}

	// UNION ALL 保留重复行，ORDER BY 作用于合并后的结果。
	if err := printQuery(ctx, w, env.sqlDB,
		"UNION ALL 与合并结果排序",
		`SELECT 1 AS k, 'a' AS tag
		 UNION ALL SELECT 2, 'b'
		 UNION ALL SELECT 2, 'c'
		 ORDER BY k, tag`,
	); err != nil {
		return err
	}

	// 谓词：BETWEEN 只求值一次左侧；LIKE 支持 Unicode 与 ESCAPE；
	// IS [NOT] DISTINCT FROM 提供 NULL 安全比较。
	if err := printQuery(ctx, w, env.sqlDB,
		"BETWEEN、LIKE/ESCAPE、NULL 安全比较",
		`SELECT 5 BETWEEN 1 AND 10 AS between_ok,
		        '你好' LIKE '你_' AS unicode_like,
		        'a_b' LIKE 'a\_b' ESCAPE '\' AS escaped_like,
		        1 IS DISTINCT FROM NULL AS distinct_from_null,
		        NULL IS NOT DISTINCT FROM NULL AS null_safe_equal`,
	); err != nil {
		return err
	}

	// 精确数值：HUGEINT/UBIGINT/DECIMAL 不经过浮点数中转；% 对负操作数遵循
	// 符号语义，零除数报错（见第 8 节）。
	if err := printQuery(ctx, w, env.sqlDB,
		"HUGEINT、UBIGINT、DECIMAL 与取模",
		`SELECT CAST('170141183460469231731687303715884105727' AS HUGEINT) AS huge_max,
		        CAST('18446744073709551615' AS UBIGINT) AS ubig_max,
		        CAST('1234.56' AS DECIMAL(10,2)) AS decimal_value,
		        CAST(7 AS BIGINT) % 3 AS positive_modulo,
		        CAST(-7 AS BIGINT) % 3 AS negative_modulo`,
	); err != nil {
		return err
	}

	// ORDER BY / LIMIT / OFFSET 组合。
	return printQuery(ctx, w, env.sqlDB,
		"ORDER BY、LIMIT 与 OFFSET",
		`SELECT id, region FROM demo_rows() ORDER BY id LIMIT 2 OFFSET 2`,
	)
}

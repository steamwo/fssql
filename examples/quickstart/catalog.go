package main

import (
	"context"
	"io"
)

// demoCatalog 演示同一 DB 实例内的 MemoryCatalog、元数据语句与凭证登记。
//
// 当前 Catalog 边界：
//   - Open 构造的 MemoryCatalog 只在该 DB 实例内共享，不跨实例持久化；
//   - CREATE TEMP TABLE ... AS SELECT 创建 DB 生命周期的类型化快照；
//   - CREATE [OR REPLACE] VIEW 保存查询定义，查询时重新绑定；
//   - DESCRIBE 只绑定 schema；relation 形式保留真实可空性；
//   - information_schema.columns 是 Planner 虚拟关系，不进入函数目录；
//   - CREATE SECRET 把凭证写入内存 SecretManager，TYPE/SUBJECT/SCHEME/HOST
//     是显式 ACL 字段，不会出现在 SQL 结果或 Catalog 元数据中。
func demoCatalog(ctx context.Context, w io.Writer, env *environment) error {
	// 1. CTAS：把 demo_rows() 的结果冻结为类型化快照。
	if _, err := env.sqlDB.ExecContext(ctx,
		"CREATE TEMP TABLE quickstart_snapshot AS SELECT * FROM demo_rows()"); err != nil {
		return err
	}
	// 2. 视图：OR REPLACE 只替换定义，不影响快照。
	if _, err := env.sqlDB.ExecContext(ctx,
		`CREATE OR REPLACE TEMP VIEW quickstart_report AS
		 SELECT region, count(*) AS row_count, sum(amount) AS total
		 FROM quickstart_snapshot
		 GROUP BY region`); err != nil {
		return err
	}
	if err := printQuery(ctx, w, env.sqlDB,
		"CTAS 快照上的视图查询",
		"SELECT * FROM quickstart_report ORDER BY region",
	); err != nil {
		return err
	}

	// 3. DESCRIBE relation 形式：只读 schema，不执行快照查询。
	if err := printQuery(ctx, w, env.sqlDB,
		"DESCRIBE 快照 relation",
		"DESCRIBE quickstart_snapshot",
	); err != nil {
		return err
	}

	// 4. information_schema.columns：列出当前实例的 Catalog 元数据。
	if err := printQuery(ctx, w, env.sqlDB,
		"information_schema.columns",
		`SELECT table_name, column_name, data_type, is_nullable
		 FROM information_schema.columns
		 ORDER BY table_name, ordinal_position`,
	); err != nil {
		return err
	}

	// 5. CREATE SECRET：仅登记到内存 SecretManager，示例使用无效主机名，
	//    不会发起任何网络请求。TYPE 必须属于 Open 时允许的 scheme 集合。
	if _, err := env.sqlDB.ExecContext(ctx,
		`CREATE SECRET quickstart_s3 (
		     TYPE s3,
		     key_id 'demo-key-id',
		     secret_key 'demo-secret',
		     SUBJECT 'quickstart',
		     SCHEME 's3',
		     HOST 'example.invalid'
		 )`); err != nil {
		return err
	}
	// 6. 不支持的存储扫描器显式失败，不会回退到其他协议。
	if _, err := env.sqlDB.ExecContext(ctx, "ATTACH 'postgres:fixture' AS external"); err == nil {
		return errUnexpectedSuccess("ATTACH 到未支持的协议")
	}
	return writef(w, "-- CREATE SECRET 已登记到内存 SecretManager（未发起网络请求）。\n"+
		"-- 未支持的 ATTACH 协议按设计显式失败。\n")
}

// errUnexpectedSuccess 用于示例中的“预期失败”断言，避免静默放过能力回归。
type errUnexpectedSuccess string

func (e errUnexpectedSuccess) Error() string { return "预期失败但成功执行：" + string(e) }

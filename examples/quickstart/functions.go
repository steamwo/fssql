package main

import (
	"context"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/steamwo/fssql/pkg/gosql"
)

// sourceStats 记录表函数 Open/Close 次数，用来展示引擎的生命周期保证。
type sourceStats struct {
	opens  atomic.Int32
	closes atomic.Int32
}

// demoRow 是示例数据源的一行；指针字段表示可 NULL 列。
type demoRow struct {
	id        int64
	region    string
	amount    *int64
	note      *string
	flag      bool
	day       *time.Time
	raw       []byte
	seq       uint64
	ratio     *float64
	price     *string // DECIMAL 以精确十进制文本写入
	updated   *time.Time
	updatedNS *int64
}

// demoRows 是固定的演示数据，便于输出稳定、可重复校验。
var demoRows = []demoRow{
	{id: 1, region: "east", amount: int64Ptr(10), note: stringPtr("alpha"), flag: true, day: datePtr(2026, 1, 2), raw: []byte{0x01, 0x02}, seq: 1, ratio: float64Ptr(0.5), price: stringPtr("12.30"), updated: timestampPtr(), updatedNS: int64Ptr(timestampPtr().UnixNano() + 789)},
	{id: 2, region: "east", amount: int64Ptr(20), note: stringPtr("beta"), flag: false, day: datePtr(2026, 1, 3), raw: []byte{0x03}, seq: 2, ratio: float64Ptr(1.5), price: stringPtr("0.00"), updated: timestampPtr(), updatedNS: nil},
	{id: 3, region: "west", amount: int64Ptr(30), note: stringPtr("gamma"), flag: true, day: datePtr(2026, 1, 4), raw: nil, seq: 3, ratio: nil, price: stringPtr("99.99"), updated: nil, updatedNS: nil},
	{id: 4, region: "west", amount: nil, note: nil, flag: false, day: nil, raw: nil, seq: 4, ratio: float64Ptr(2.5), price: nil, updated: nil, updatedNS: nil},
	{id: 5, region: "north", amount: int64Ptr(50), note: stringPtr("αβγ"), flag: true, day: datePtr(2026, 1, 5), raw: []byte{0x04, 0x05}, seq: 5, ratio: nil, price: stringPtr("5.55"), updated: nil, updatedNS: nil},
}

func int64Ptr(value int64) *int64       { return &value }
func float64Ptr(value float64) *float64 { return &value }
func stringPtr(value string) *string    { return &value }
func datePtr(year int, month time.Month, day int) *time.Time {
	value := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	return &value
}
func timestampPtr() *time.Time {
	value := time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)
	return &value
}

// demoRowsTableFunction 注册一个固定 schema、零参数的 Go 表函数。
//
// TableFunction 契约：
//   - Columns 必须非空且列名/类型固定；注册名不能与已有函数冲突；
//   - Open 是惰性的，同一查询中最多调用一次；
//   - BatchReader.Next 每次最多填充 writer.Capacity() 行；
//   - 返回 (0, io.EOF) 结束；返回 (n>0, io.EOF) 发布最后一批；
//   - BatchWriter 只在 Next 期间有效，setter 会把值复制进引擎存储；
//   - 查询结束、失败或提前关闭时调用 Close 释放调用方资源。
func demoRowsTableFunction(stats *sourceStats) gosql.TableFunction {
	return gosql.TableFunction{
		Name: "demo_rows",
		Columns: []gosql.Column{
			{Name: "id", Type: gosql.Type{ID: "BIGINT"}},
			{Name: "region", Type: gosql.Type{ID: "VARCHAR"}},
			{Name: "amount", Type: gosql.Type{ID: "BIGINT"}, Nullable: true},
			{Name: "note", Type: gosql.Type{ID: "VARCHAR"}, Nullable: true},
			{Name: "flag", Type: gosql.Type{ID: "BOOLEAN"}},
			{Name: "day", Type: gosql.Type{ID: "DATE"}, Nullable: true},
			{Name: "raw", Type: gosql.BlobType(), Nullable: true},
			{Name: "seq", Type: gosql.UBigIntType()},
			{Name: "ratio", Type: gosql.Type{ID: "DOUBLE"}, Nullable: true},
			{Name: "price", Type: gosql.Type{ID: "DECIMAL", Width: 10, Scale: 2}, Nullable: true},
			{Name: "updated_at", Type: gosql.Type{ID: "TIMESTAMP"}, Nullable: true},
			{Name: "updated_ns", Type: gosql.TimestampNSType(), Nullable: true},
		},
		Open: func(context.Context) (gosql.BatchReader, error) {
			stats.opens.Add(1)
			return &demoRowsReader{stats: stats}, nil
		},
	}
}

type demoRowsReader struct {
	stats *sourceStats
	next  int
}

func (r *demoRowsReader) Next(ctx context.Context, writer gosql.BatchWriter) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if r.next >= len(demoRows) {
		return 0, io.EOF
	}
	n := writer.Capacity()
	if remaining := len(demoRows) - r.next; n > remaining {
		n = remaining
	}
	for i := 0; i < n; i++ {
		row := demoRows[r.next+i]
		if err := writer.SetInt64(i, 0, row.id); err != nil {
			return 0, err
		}
		if err := writer.SetString(i, 1, row.region); err != nil {
			return 0, err
		}
		if row.amount == nil {
			if err := writer.SetNull(i, 2); err != nil {
				return 0, err
			}
		} else if err := writer.SetInt64(i, 2, *row.amount); err != nil {
			return 0, err
		}
		if row.note == nil {
			if err := writer.SetNull(i, 3); err != nil {
				return 0, err
			}
		} else if err := writer.SetString(i, 3, *row.note); err != nil {
			return 0, err
		}
		if err := writer.SetBool(i, 4, row.flag); err != nil {
			return 0, err
		}
		if row.day == nil {
			if err := writer.SetNull(i, 5); err != nil {
				return 0, err
			}
		} else if err := writer.SetDate(i, 5, *row.day); err != nil {
			return 0, err
		}
		if row.raw == nil {
			if err := writer.SetNull(i, 6); err != nil {
				return 0, err
			}
		} else if err := writer.SetBytes(i, 6, row.raw); err != nil {
			return 0, err
		}
		if err := writer.SetUint64(i, 7, row.seq); err != nil {
			return 0, err
		}
		if row.ratio == nil {
			if err := writer.SetNull(i, 8); err != nil {
				return 0, err
			}
		} else if err := writer.SetFloat64(i, 8, *row.ratio); err != nil {
			return 0, err
		}
		if row.price == nil {
			if err := writer.SetNull(i, 9); err != nil {
				return 0, err
			}
		} else if err := writer.SetDecimal(i, 9, *row.price); err != nil {
			return 0, err
		}
		if row.updated == nil {
			if err := writer.SetNull(i, 10); err != nil {
				return 0, err
			}
		} else if err := writer.SetTimestamp(i, 10, *row.updated); err != nil {
			return 0, err
		}
		if row.updatedNS == nil {
			if err := writer.SetNull(i, 11); err != nil {
				return 0, err
			}
		} else if err := writer.SetTimestampNS(i, 11, *row.updatedNS); err != nil {
			return 0, err
		}
	}
	r.next += n
	if r.next == len(demoRows) {
		// n > 0 与 io.EOF 同时返回表示这是最后一批。
		return n, io.EOF
	}
	return n, nil
}

func (r *demoRowsReader) Close() error {
	r.stats.closes.Add(1)
	return nil
}

// demoScalarFunctions 返回两个标量 UDF 和 HMAC 辅助函数。
//
// ScalarNullPropagate（默认）：任一输入为 NULL 时不调用 Eval，直接返回 NULL。
// ScalarNullCall：Eval 会收到 Valid=false 的 ScalarValue，由函数自行决定结果。
// 返回值中的 String/Bytes 会在边界处复制进受管向量存储。
func demoScalarFunctions(hmac gosql.ScalarFunction) []gosql.ScalarFunction {
	upper := gosql.ScalarFunction{
		Name:          "app_upper",
		Arguments:     []gosql.Type{{ID: "VARCHAR"}},
		Result:        gosql.Type{ID: "VARCHAR"},
		NullHandling:  gosql.ScalarNullPropagate,
		Deterministic: true,
		Eval: func(args []gosql.ScalarValue) (gosql.ScalarValue, error) {
			return gosql.ScalarValue{
				Type:   gosql.Type{ID: "VARCHAR"},
				Valid:  true,
				String: strings.ToUpper(args[0].String),
			}, nil
		},
	}
	nullLabel := gosql.ScalarFunction{
		Name:          "app_null_label",
		Arguments:     []gosql.Type{{ID: "VARCHAR"}},
		Result:        gosql.Type{ID: "VARCHAR"},
		NullHandling:  gosql.ScalarNullCall,
		Deterministic: true,
		Eval: func(args []gosql.ScalarValue) (gosql.ScalarValue, error) {
			if !args[0].Valid {
				return gosql.ScalarValue{Type: gosql.Type{ID: "VARCHAR"}, Valid: true, String: "(NULL)"}, nil
			}
			return gosql.ScalarValue{Type: gosql.Type{ID: "VARCHAR"}, Valid: true, String: args[0].String}, nil
		},
	}
	return []gosql.ScalarFunction{upper, nullLabel, hmac}
}

// demoFunctions 演示 Go 扩展点与物化快照。
func demoFunctions(ctx context.Context, w io.Writer, env *environment) error {
	// 1. 原生接口调用表函数，并读取返回的列元数据。
	var rowCount int
	var amountTotal int64
	columns, err := env.db.QueryChunks(ctx, "SELECT id, region, amount FROM demo_rows() ORDER BY id", func(chunk *gosql.Chunk) error {
		for row := 0; row < chunk.RowCount(); row++ {
			rowCount++
			amount, valid, err := chunk.Int64(row, 2)
			if err != nil {
				return err
			}
			if valid {
				amountTotal += amount
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := writef(w, "-- 表函数 demo_rows()\n"); err != nil {
		return err
	}
	for _, column := range columns {
		if err := writef(w, "   %-8s %s\n", column.Name, column.Type.ID); err != nil {
			return err
		}
	}
	if err := writef(w, "   行数=%d，amount 合计=%d，Open=%d，Close=%d\n",
		rowCount, amountTotal, env.source.opens.Load(), env.source.closes.Load()); err != nil {
		return err
	}
	// 展示 BatchWriter 其余 setter 写入的 UBIGINT/DOUBLE/DECIMAL/TIMESTAMP 列。
	if err := printQuery(ctx, w, env.sqlDB,
		"表函数写入的无符号、浮点、DECIMAL 与时间列",
		`SELECT id, seq, ratio, price, updated_at, updated_ns
		 FROM demo_rows()
		 ORDER BY id`,
	); err != nil {
		return err
	}

	// 2. 标量 UDF：NULL 传播与显式处理 NULL 两种策略。
	if err := printQuery(ctx, w, env.sqlDB,
		"标量 UDF（NULL 传播与 NULL 调用）",
		`SELECT app_upper('hello') AS upper_value,
		        app_upper(NULL) AS propagated_null,
		        app_null_label(NULL) AS explicit_null_label`,
	); err != nil {
		return err
	}

	// 3. HMAC 辅助函数：确定性 VARCHAR→VARCHAR，输出小写十六进制。
	//    密钥只存在于 DB 作用域的函数定义中，不会写入 SQL 或计划。
	if err := printQuery(ctx, w, env.sqlDB,
		"HMAC-SHA256 标量函数",
		`SELECT data_plane_hmac_sha256('alice@example.com') AS masked_value`,
	); err != nil {
		return err
	}

	// 4. Materialize：一次性执行查询并发布有界快照，之后可重复扫描。
	//    快照独立于创建它的查询；Close 后拒绝新的绑定，已绑定的查询仍持有
	//    reader lease 直到自身关闭。物化数据在内存预算内驻留，超出后 spill。
	materialized, err := env.db.Materialize(ctx,
		`SELECT region, count(*) AS row_count, sum(amount) AS total
		 FROM demo_rows()
		 GROUP BY region
		 ORDER BY region`)
	if err != nil {
		return err
	}
	scan := func() (int, error) {
		rows := 0
		_, err := env.db.QueryChunks(ctx, materialized.SQL(), func(chunk *gosql.Chunk) error {
			rows += chunk.RowCount()
			return nil
		})
		return rows, err
	}
	first, err := scan()
	if err != nil {
		_ = materialized.Close()
		return err
	}
	second, err := scan()
	if err != nil {
		_ = materialized.Close()
		return err
	}
	if err := materialized.Close(); err != nil {
		return err
	}
	if err := writef(w, "-- Materialize 快照\n   两次重复扫描分别返回 %d 行和 %d 行（上游只执行一次）。\n", first, second); err != nil {
		return err
	}

	// 5. 所有查询和快照关闭后，受管内存应回到 0。
	//    MemoryUsed 只统计引擎受管内存，不是进程 RSS 或 Go heap。
	return writef(w, "   当前 DB.MemoryUsed() = %d 字节\n", env.db.MemoryUsed())
}

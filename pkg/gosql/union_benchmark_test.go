package gosql

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// unionChainSQL 生成按递增顺序返回整数的左结合 UNION ALL 长链。
func unionChainSQL(terms int) string {
	var sql strings.Builder
	for i := 0; i < terms; i++ {
		if i != 0 {
			sql.WriteString(" UNION ALL ")
		}
		fmt.Fprintf(&sql, "SELECT %d AS value", i)
	}
	return sql.String()
}

// BenchmarkUnionAllChain 衡量从解析到执行的完整长链开销，避免只测预先绑定的计划。
func BenchmarkUnionAllChain(b *testing.B) {
	for _, terms := range []int{100, 500, 1000, 2049, 4097} {
		b.Run(fmt.Sprint(terms), func(b *testing.B) {
			db, err := Open(Config{MemoryLimitBytes: 64 << 20, TempDir: b.TempDir(), ManifestDir: b.TempDir()})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			query := unionChainSQL(terms)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows := 0
				_, err := db.QueryChunks(context.Background(), query, func(chunk *Chunk) error {
					rows += chunk.RowCount()
					return nil
				})
				if err != nil {
					b.Fatal(err)
				}
				if rows != terms {
					b.Fatalf("rows=%d want %d", rows, terms)
				}
			}
		})
	}
}

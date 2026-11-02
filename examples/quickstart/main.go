// quickstart 演示仅通过公共包使用原生 Chunk 和 database/sql 查询接口。
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/steamwo/fssql/pkg/gosql"
)

// main 运行无需外部数据源和凭据的查询示例。
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		log.Fatal(err)
	}
}

// run 依次展示两种查询方式，并将查询或输出错误交给调用方。
func run(ctx context.Context, output io.Writer) error {
	native, err := gosql.Open(gosql.Config{})
	if err != nil {
		return err
	}
	defer native.Close()

	_, err = native.QueryChunks(ctx, "SELECT 42 AS answer", func(chunk *gosql.Chunk) error {
		for row := 0; row < chunk.RowCount(); row++ {
			answer, valid, err := chunk.Int64(row, 0)
			if err != nil {
				return err
			}
			if !valid {
				return fmt.Errorf("示例查询返回了 NULL")
			}
			if _, err := fmt.Fprintf(output, "native: %d\n", answer); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	sqlDB, err := gosql.OpenSQLDB(gosql.Config{})
	if err != nil {
		return err
	}
	defer sqlDB.Close()

	var answer int64
	if err := sqlDB.QueryRowContext(ctx, "SELECT 42 AS answer").Scan(&answer); err != nil {
		return err
	}
	_, err = fmt.Fprintf(output, "database/sql: %d\n", answer)
	return err
}

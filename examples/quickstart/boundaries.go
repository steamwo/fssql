package main

import (
	"context"
	"errors"
	"io"
	"path/filepath"

	"github.com/steamwo/fssql/pkg/gosql"
)

// demoBoundaries 演示公共 API 的显式错误边界。
//
// 示例刻意断言这些调用“必须失败”，用于说明引擎不会静默降级：
//   - INTERSECT BY NAME 不属于支持的集合语法；
//   - BIGINT 取模零除数返回错误；
//   - 无符号列必须用 Uint64 访问，Int64 会报类型不匹配；
//   - 取消的上下文立即返回 context.Canceled，且不产生结果；
//   - Close 后的 DB 拒绝新的查询；
//   - 事务在 database/sql 层明确不支持（见第 2 节）。
func demoBoundaries(ctx context.Context, w io.Writer, env *environment) error {
	// 1. 不合法的集合语法必须报错，而不是返回部分结果。
	if _, err := env.db.QueryChunks(ctx, "SELECT 1 INTERSECT BY NAME SELECT 2", func(*gosql.Chunk) error { return nil }); err == nil {
		return errUnexpectedSuccess("INTERSECT BY NAME")
	}
	if err := writef(w, "-- INTERSECT BY NAME 按设计拒绝。\n"); err != nil {
		return err
	}

	// 2. 零除数不会返回 NULL 或静默截断。
	if _, err := env.db.QueryChunks(ctx, "SELECT CAST(7 AS BIGINT) % 0", func(*gosql.Chunk) error { return nil }); err == nil {
		return errUnexpectedSuccess("零除数取模")
	}
	if err := writef(w, "-- BIGINT 取模零除数按设计报错。\n"); err != nil {
		return err
	}

	// 3. 访问器与逻辑类型必须匹配；无符号列不能走 Int64。
	_, err := env.db.QueryChunks(ctx, "SELECT CAST(1 AS UTINYINT)", func(chunk *gosql.Chunk) error {
		_, _, err := chunk.Int64(0, 0)
		return err
	})
	if err == nil {
		return errUnexpectedSuccess("对 UTINYINT 调用 Int64")
	}
	if err := writef(w, "-- 对 UTINYINT 调用 Int64 返回类型错误：%v\n", err); err != nil {
		return err
	}

	// 4. 取消的上下文必须解除执行并返回 context.Canceled。
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = env.db.QueryChunks(canceled, "SELECT 1", func(*gosql.Chunk) error { return nil })
	if !errors.Is(err, context.Canceled) {
		return errors.New("取消的上下文未返回 context.Canceled")
	}
	if err := writef(w, "-- 取消的上下文返回 context.Canceled。\n"); err != nil {
		return err
	}

	// 5. Close 后拒绝新工作；进行中的查询由各自的 context 负责结束。
	closedDB, err := gosql.Open(gosql.Config{
		TempDir:     filepath.Join(env.root, "closed-spill"),
		ManifestDir: filepath.Join(env.root, "closed-manifests"),
	})
	if err != nil {
		return err
	}
	if err := closedDB.Close(); err != nil {
		return err
	}
	if _, err := closedDB.QueryChunks(ctx, "SELECT 1", func(*gosql.Chunk) error { return nil }); err == nil {
		return errUnexpectedSuccess("Close 后的 QueryChunks")
	}
	return writef(w, "-- Close 后的 DB 拒绝新的 QueryChunks。\n")
}

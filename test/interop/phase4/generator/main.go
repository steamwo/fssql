package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/export"
)

// fileSink 把 Parquet writer 的 ByteSink 输出写入本地 fixture 文件；它只用于 interop 测试生成器。
type fileSink struct {
	// file 是当前 fixture 的独占文件句柄。
	file *os.File
	// path 用于 Abort 时删除 incomplete fixture。
	path string
	// bytes 记录成功写入的文件字节数。
	bytes int64
	// terminal 防止 Close/Abort 后再次写入。
	terminal bool
}

// Write 顺序写入 fixture，并观察取消状态。
func (s *fileSink) Write(ctx context.Context, p []byte) (int, error) {
	if s == nil || s.file == nil || s.terminal {
		return 0, errors.New("phase4 generator: sink closed")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	n, err := s.file.Write(p)
	s.bytes += int64(n)
	return n, err
}

// Flush 将已写数据同步到本地文件。
func (s *fileSink) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.file.Sync()
}

// Close 完成 fixture 文件并返回文件级结果。
func (s *fileSink) Close(ctx context.Context) (export.WriteResult, error) {
	if s == nil || s.file == nil || s.terminal {
		return export.WriteResult{}, errors.New("phase4 generator: sink closed")
	}
	if err := ctx.Err(); err != nil {
		return export.WriteResult{}, err
	}
	if err := s.file.Sync(); err != nil {
		return export.WriteResult{}, err
	}
	if err := s.file.Close(); err != nil {
		return export.WriteResult{}, err
	}
	s.terminal = true
	return export.WriteResult{Bytes: s.bytes}, nil
}

// Abort 关闭并删除 incomplete fixture；可重复调用。
func (s *fileSink) Abort(context.Context) error {
	if s == nil || s.terminal {
		return nil
	}
	s.terminal = true
	var closeErr error
	if s.file != nil {
		closeErr = s.file.Close()
	}
	removeErr := os.Remove(s.path)
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}
	return errors.Join(closeErr, removeErr)
}

// main 生成一个包含 INTEGER/VARCHAR/NULL 的 fssql Parquet 文件，供 DuckDB/PyArrow 读取验证。
func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: go run ./test/interop/phase4/generator <output.parquet>")
		os.Exit(2)
	}
	path := os.Args[1]
	file, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	sink := &fileSink{file: file, path: path}

	manager := memory.NewManager(16 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	idType := types.LogicalType{ID: types.Integer}
	nameType := types.LogicalType{ID: types.Varchar}
	id, err := vector.NewFlat(scope, buffers, idType, 3)
	if err != nil {
		panic(err)
	}
	name, err := vector.NewFlat(scope, buffers, nameType, 3)
	if err != nil {
		id.Release()
		panic(err)
	}
	id.SetInt32(0, 1)
	id.SetInt32(1, 2)
	id.SetInt32(2, 3)
	if err := name.SetString(0, "alpha"); err != nil {
		panic(err)
	}
	if err := name.SetString(1, "beta"); err != nil {
		panic(err)
	}
	name.SetNull(2)
	chunk, err := vector.NewDataChunk(id, name)
	if err != nil {
		id.Release()
		name.Release()
		panic(err)
	}

	writer, err := export.NewParquetWriter([]export.Column{{Name: "id", Type: idType}, {Name: "name", Type: nameType}}, export.ParquetWriteOptions{RowGroupRows: 2, PageRows: 1}, scope, buffers, sink)
	if err != nil {
		chunk.Release()
		_ = sink.Abort(context.Background())
		panic(err)
	}
	if err := writer.WriteChunk(context.Background(), chunk); err != nil {
		chunk.Release()
		_ = writer.Abort(context.Background())
		panic(err)
	}
	chunk.Release()
	if _, err := writer.Close(context.Background()); err != nil {
		panic(err)
	}
	if scope.Used() != 0 {
		panic(fmt.Sprintf("managed memory leak: %d", scope.Used()))
	}
}

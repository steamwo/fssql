package local

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// fileRangeReader 固定已通过根目录校验的文件句柄，不缓存文件内容或跳过身份检查。
type fileRangeReader struct {
	file     *os.File           // file 在整个 reader 生命周期内固定，避免反复打开路径。
	path     string             // path 用于检测路径被替换，读取始终通过固定句柄完成。
	uri      vfs.URI            // uri 限制 reader 只能读取打开时的对象。
	identity vfs.ObjectIdentity // identity 是打开时冻结的 size/mtime 身份。
	once     sync.Once          // once 保证失败清理与正常结束只关闭一次句柄。
	closeErr error              // closeErr 保存首次关闭的结果。
	closed   atomic.Bool        // closed 提供跨平台一致的关闭后错误，避免依赖无效句柄错误码。
}

// OpenRangeReader 在根目录与预期身份校验成功后打开只读句柄，供单次扫描复用。
func (d *Driver) OpenRangeReader(ctx context.Context, uri vfs.URI, expected *vfs.ObjectIdentity) (vfs.RangeReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := d.localPath(uri, true)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	identity := infoIdentity(info)
	if !info.Mode().IsRegular() || (expected != nil && !expected.Matches(identity)) {
		_ = file.Close()
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("local: not a regular file: %s", uri.String())
		}
		return nil, vfs.ErrObjectChanged
	}
	return &fileRangeReader{file: file, path: path, uri: uri, identity: identity}, nil
}

// ReadRange 通过固定句柄读取，同时保留原 Driver 的受管内存、取消与对象变更边界。
func (r *fileRangeReader) ReadRange(ctx context.Context, request vfs.RangeRequest, scope *memory.Scope, bm *memory.BufferManager) (vfs.RangeResult, error) {
	if r.closed.Load() {
		return vfs.RangeResult{}, os.ErrClosed
	}
	if request.URI.IdentityKey() != r.uri.IdentityKey() {
		return vfs.RangeResult{}, fmt.Errorf("local: range reader URI mismatch")
	}
	return readFileRange(ctx, request, scope, bm, r.file, r.path, &r.identity)
}

// Close 幂等地关闭固定文件句柄。
func (r *fileRangeReader) Close() error {
	r.once.Do(func() { r.closed.Store(true); r.closeErr = r.file.Close() })
	return r.closeErr
}

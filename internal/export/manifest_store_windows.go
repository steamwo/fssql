package export

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

const (
	// manifestMoveReplaceExisting 允许同目录发布替换已有的完整 snapshot。
	manifestMoveReplaceExisting = 0x1
	// manifestMoveWriteThrough 要求 Windows 在移动写入磁盘后才返回成功。
	manifestMoveWriteThrough = 0x8
)

// manifestMoveFileExW 是 Windows 的同步移动入口，不能退回普通 rename 或忽略同步错误。
var manifestMoveFileExW = syscall.NewLazyDLL("kernel32.dll").NewProc("MoveFileExW")

// manifestDirectoryMu 序列化同进程的首次目录发布，避免并发查询竞争同一个 Manifest 根目录。
var manifestDirectoryMu sync.Mutex

// prepareLocalManifestDir 串行准备本地目录，避免同进程首次发布同名目录时发生竞争。
func prepareLocalManifestDir(dir string) error {
	manifestDirectoryMu.Lock()
	defer manifestDirectoryMu.Unlock()
	return createLocalManifestDir(dir)
}

// createLocalManifestDir 在持锁时逐级发布新目录，使首次保存也使用同步移动边界。
func createLocalManifestDir(dir string) error {
	info, err := os.Stat(dir)
	if err == nil {
		if !info.IsDir() {
			return &os.PathError{Op: "mkdir", Path: dir, Err: syscall.ENOTDIR}
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(dir)
	if parent == dir {
		return err
	}
	if err := createLocalManifestDir(parent); err != nil {
		return err
	}
	temporary, err := os.MkdirTemp(parent, ".fssql-manifest-dir-*")
	if err != nil {
		return err
	}
	defer os.Remove(temporary)
	return moveLocalManifest(temporary, dir, manifestMoveWriteThrough)
}

// publishLocalManifest 用同卷 write-through 替换发布已同步的临时文件。
func publishLocalManifest(ctx context.Context, temporary, path string) error {
	return retryLocalManifestMove(ctx, func() error {
		return moveLocalManifest(temporary, path, manifestMoveReplaceExisting|manifestMoveWriteThrough)
	})
}

// removeLocalManifest 先同步移走恢复入口，再清理 tombstone，避免复用不支持的目录 Sync。
func removeLocalManifest(ctx context.Context, path string) error {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	tombstone, err := os.CreateTemp(filepath.Dir(path), ".fssql-manifest-deleted-*")
	if err != nil {
		return err
	}
	name := tombstone.Name()
	defer os.Remove(name)
	if err := tombstone.Close(); err != nil {
		return err
	}
	if err := retryLocalManifestMove(ctx, func() error {
		return moveLocalManifest(path, name, manifestMoveReplaceExisting|manifestMoveWriteThrough)
	}); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	// 原路径的移除已经由同步移动确认，遗留 tombstone 不会被 Load 当作有效状态。
	return os.Remove(name)
}

// retryLocalManifestMove 容忍 Windows 文件句柄短暂拒绝共享删除；持续权限错误仍返回原生错误。
// 所有尝试保持相同的原子移动和 write-through 标志，不使用删除目标或复制文件降级。
func retryLocalManifestMove(ctx context.Context, move func() error) error {
	deadline := time.Now().Add(2 * time.Second)
	delay := time.Millisecond
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if lastErr != nil && !time.Now().Before(deadline) {
			return lastErr
		}
		err := move()
		if !errors.Is(err, syscall.ERROR_ACCESS_DENIED) && !errors.Is(err, syscall.Errno(32)) {
			return err
		}
		lastErr = err
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return lastErr
		}
		wait := delay
		if wait > remaining {
			wait = remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if delay < 50*time.Millisecond {
			delay *= 2
			if delay > 50*time.Millisecond {
				delay = 50 * time.Millisecond
			}
		}
	}
}

// moveLocalManifest 调用原生同步移动并保留系统错误，不启用跨卷 copy/delete fallback。
func moveLocalManifest(from, to string, flags uintptr) error {
	source, err := localManifestWindowsPath(from)
	if err != nil {
		return err
	}
	target, err := localManifestWindowsPath(to)
	if err != nil {
		return err
	}
	result, _, callErr := manifestMoveFileExW.Call(uintptr(unsafe.Pointer(source)), uintptr(unsafe.Pointer(target)), flags)
	runtime.KeepAlive(source)
	runtime.KeepAlive(target)
	if result == 0 {
		if callErr == syscall.Errno(0) {
			callErr = syscall.EINVAL
		}
		return &os.LinkError{Op: "move-manifest", Old: from, New: to, Err: callErr}
	}
	return nil
}

// localManifestWindowsPath 将相对路径转为扩展绝对路径，保留 UNC、Unicode 与长路径支持。
func localManifestWindowsPath(path string) (*uint16, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(absolute, `\\?\`) && !strings.HasPrefix(absolute, `\\.\`) {
		if strings.HasPrefix(absolute, `\\`) {
			absolute = `\\?\UNC\` + absolute[2:]
		} else {
			absolute = `\\?\` + absolute
		}
	}
	return syscall.UTF16PtrFromString(absolute)
}

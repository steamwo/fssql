//go:build !windows

package export

import (
	"errors"
	"os"
	"path/filepath"
)

// prepareLocalManifestDir 创建 Manifest 的本地目录，保持现有 Unix 权限设置。
func prepareLocalManifestDir(dir string) error {
	return os.MkdirAll(dir, 0o700)
}

// publishLocalManifest 原子替换同目录文件，并同步目录中的新名称。
func publishLocalManifest(temporary, path string) error {
	if err := os.Rename(temporary, path); err != nil {
		return err
	}
	return syncLocalManifestDir(filepath.Dir(path))
}

// removeLocalManifest 删除恢复入口后同步目录，确保成功清理进入持久化边界。
func removeLocalManifest(path string) error {
	if err := os.Remove(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	return syncLocalManifestDir(filepath.Dir(path))
}

// syncLocalManifestDir 同步 Unix 目录元数据，传播底层文件系统的同步失败。
func syncLocalManifestDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

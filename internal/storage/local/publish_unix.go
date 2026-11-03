//go:build !windows

package local

import "os"

func publishFile(stagingPath, finalPath string) error {
	if err := os.Rename(stagingPath, finalPath); err != nil {
		return err
	}
	// The atomic visibility contract is satisfied by rename. Directory sync is
	// best-effort durability hardening: reporting a failure after rename would
	// incorrectly tell the caller that the previous final object was preserved.
	if dir, err := os.Open(filepathDir(finalPath)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

func filepathDir(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == os.PathSeparator {
			if i == 0 {
				return string(os.PathSeparator)
			}
			return path[:i]
		}
	}
	return "."
}

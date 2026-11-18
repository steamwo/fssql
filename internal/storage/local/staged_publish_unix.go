//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly || solaris || aix

package local

import (
	"fmt"
	"os"
	"syscall"
)

func publishFileID(f *os.File) (string, error) {
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	i, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "", fmt.Errorf("local: missing file identity")
	}
	return fmt.Sprintf("%x-%x", i.Dev, i.Ino), nil
}
func syncPublishDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func publishStagedFile(stage, final string) error { return os.Link(stage, final) }

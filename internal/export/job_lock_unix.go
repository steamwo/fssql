//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package export

import (
	"errors"
	"os"
	"syscall"
)

func tryLockJob(f *os.File) error { return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) }
func unlockJob(f *os.File) error  { return syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }
func jobLockBusy(err error) bool {
	return errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN)
}

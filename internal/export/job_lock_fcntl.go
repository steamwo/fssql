//go:build (solaris && !illumos) || aix

package export

import (
	"errors"
	"os"
	"syscall"
)

func tryLockJob(f *os.File) error {
	lock := syscall.Flock_t{Type: syscall.F_WRLCK}
	return syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, &lock)
}

func unlockJob(f *os.File) error {
	lock := syscall.Flock_t{Type: syscall.F_UNLCK}
	return syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, &lock)
}

func jobLockBusy(err error) bool {
	return errors.Is(err, syscall.EACCES) || errors.Is(err, syscall.EAGAIN)
}

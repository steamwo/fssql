package export

import (
	"errors"
	"os"
	"syscall"
	"unsafe"
)

var jobLockFileEx = syscall.NewLazyDLL("kernel32.dll").NewProc("LockFileEx")
var jobUnlockFileEx = syscall.NewLazyDLL("kernel32.dll").NewProc("UnlockFileEx")

func tryLockJob(f *os.File) error {
	var overlapped syscall.Overlapped
	r, _, err := jobLockFileEx.Call(f.Fd(), 3, 0, 1, 0, uintptr(unsafe.Pointer(&overlapped)))
	if r == 0 {
		return err
	}
	return nil
}
func unlockJob(f *os.File) error {
	var overlapped syscall.Overlapped
	r, _, err := jobUnlockFileEx.Call(f.Fd(), 0, 1, 0, uintptr(unsafe.Pointer(&overlapped)))
	if r == 0 {
		return err
	}
	return nil
}
func jobLockBusy(err error) bool { return errors.Is(err, syscall.Errno(33)) }

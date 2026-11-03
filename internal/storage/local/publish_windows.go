//go:build windows

package local

import (
	"fmt"
	"syscall"
	"unsafe"
)

const (
	moveFileReplaceExisting = 0x1
	moveFileWriteThrough    = 0x8
)

var moveFileExW = syscall.NewLazyDLL("kernel32.dll").NewProc("MoveFileExW")

func publishFile(stagingPath, finalPath string) error {
	from, err := syscall.UTF16PtrFromString(stagingPath)
	if err != nil {
		return err
	}
	to, err := syscall.UTF16PtrFromString(finalPath)
	if err != nil {
		return err
	}
	ok, _, callErr := moveFileExW.Call(
		uintptr(unsafe.Pointer(from)),
		uintptr(unsafe.Pointer(to)),
		uintptr(moveFileReplaceExisting|moveFileWriteThrough),
	)
	if ok != 0 {
		return nil
	}
	if callErr != syscall.Errno(0) {
		return callErr
	}
	return fmt.Errorf("local: MoveFileExW failed")
}

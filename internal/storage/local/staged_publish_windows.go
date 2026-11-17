package local

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

func publishFileID(f *os.File) (string, error) {
	var i syscall.ByHandleFileInformation
	if err := syscall.GetFileInformationByHandle(syscall.Handle(f.Fd()), &i); err != nil {
		return "", err
	}
	return fmt.Sprintf("%08x-%08x%08x", i.VolumeSerialNumber, i.FileIndexHigh, i.FileIndexLow), nil
}

// Publication uses a no-replace hard link, as the existing local writer does.
// Windows does not provide the Unix directory fsync contract via os.File.Sync.
func syncPublishDirectory(string) error { return nil }

// MoveFileEx without REPLACE_EXISTING preserves no-replace and uses the same
// write-through durability primitive as the existing Windows manifest store.
func publishStagedFile(stage, final string) error {
	from, err := syscall.UTF16PtrFromString(stage)
	if err != nil {
		return err
	}
	to, err := syscall.UTF16PtrFromString(final)
	if err != nil {
		return err
	}
	ok, _, err := moveFileExW.Call(uintptr(unsafe.Pointer(from)), uintptr(unsafe.Pointer(to)), moveFileWriteThrough)
	if ok == 0 {
		return err
	}
	return nil
}

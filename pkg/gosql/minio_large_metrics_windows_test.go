package gosql

import (
	"syscall"
	"unsafe"
)

// minIOLargeWindowsMemory 对应 Win32 PROCESS_MEMORY_COUNTERS，SIZE_T 使用 uintptr 兼容位宽。
type minIOLargeWindowsMemory struct {
	Size              uint32  // Size 是原生结构体大小。
	PageFaults        uint32  // PageFaults 是累计缺页计数。
	PeakWorkingSet    uintptr // PeakWorkingSet 是进程历史驻留集峰值。
	WorkingSet        uintptr // WorkingSet 是当前驻留集。
	PeakPagedPool     uintptr // PeakPagedPool 是分页池历史峰值。
	PagedPool         uintptr // PagedPool 是当前分页池使用量。
	PeakNonPagedPool  uintptr // PeakNonPagedPool 是非分页池峰值。
	NonPagedPool      uintptr // NonPagedPool 是当前非分页池使用量。
	PagefileUsage     uintptr // PagefileUsage 是当前提交量。
	PeakPagefileUsage uintptr // PeakPagefileUsage 是提交量历史峰值。
}

// minIOLargeMemoryInfo 是当前进程的只读内存查询接口。
var minIOLargeMemoryInfo = syscall.NewLazyDLL("kernel32.dll").NewProc("K32GetProcessMemoryInfo")

// minIOLargeSystemMetrics 返回当前 RSS 与真实进程 CPU 秒数，不把 Go heap 当作 RSS。
func minIOLargeSystemMetrics() (uint64, float64, error) {
	process, err := syscall.GetCurrentProcess()
	if err != nil {
		return 0, 0, err
	}
	var counters minIOLargeWindowsMemory
	counters.Size = uint32(unsafe.Sizeof(counters))
	result, _, callErr := minIOLargeMemoryInfo.Call(uintptr(process), uintptr(unsafe.Pointer(&counters)), uintptr(counters.Size))
	if result == 0 {
		return 0, 0, callErr
	}
	var created, exited, kernel, user syscall.Filetime
	if err := syscall.GetProcessTimes(process, &created, &exited, &kernel, &user); err != nil {
		return 0, 0, err
	}
	ticks := (uint64(kernel.HighDateTime)<<32 | uint64(kernel.LowDateTime)) + (uint64(user.HighDateTime)<<32 | uint64(user.LowDateTime))
	return uint64(counters.WorkingSet), float64(ticks) / 10000000, nil
}

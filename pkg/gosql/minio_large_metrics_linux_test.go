package gosql

import (
	"os"
	"strconv"
	"strings"
	"syscall"
)

// minIOLargeSystemMetrics 通过 Linux rusage 与 statm 读取真实 CPU 时间及当前 RSS。
func minIOLargeSystemMetrics() (uint64, float64, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, 0, err
	}
	raw, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, 0, err
	}
	fields := strings.Fields(string(raw))
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	cpu := float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1000000
	return pages * uint64(os.Getpagesize()), cpu, nil
}

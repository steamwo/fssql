//go:build !windows && !linux

package gosql

import "errors"

// minIOLargeSystemMetrics 对未实现遥测的平台显式失败，避免把缺失指标报告为零占用。
func minIOLargeSystemMetrics() (uint64, float64, error) {
	return 0, 0, errors.New("large MinIO process telemetry currently supports Windows/Linux only")
}

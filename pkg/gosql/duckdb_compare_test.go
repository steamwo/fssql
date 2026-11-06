package gosql

// 本文件只提供 DuckDB 对标测试的单用例执行入口，不改变引擎行为。
// 每次进程只执行一个已由外部编排器选定的场景，便于与 DuckDB 交替采样。

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
	"time"
)

// compareCase 是外部编排器传入的单个查询场景。
type compareCase struct {
	Name         string `json:"name"`
	SQL          string `json:"sql"`
	Source       string `json:"source"`
	ExpectedRows int64  `json:"expected_rows"`
	MemoryLimit  int64  `json:"memory_limit"`
	SpillBudget  int64  `json:"spill_budget"`
	TempLimit    int64  `json:"temp_limit"`
	Workers      int    `json:"workers"`
	OrderColumns []int  `json:"order_columns"`
}

// compareResult 保留单次查询的耗时、行数与资源指标。
type compareResult struct {
	Name             string  `json:"name"`
	Source           string  `json:"source"`
	Seconds          float64 `json:"seconds"`
	Rows             int64   `json:"rows"`
	CPUSeconds       float64 `json:"cpu_seconds"`
	CPUPercent       float64 `json:"cpu_percent"`
	BaseRSS          uint64  `json:"rss_base_bytes"`
	PeakRSS          uint64  `json:"rss_peak_bytes"`
	PeakHeap         uint64  `json:"go_heap_peak_bytes"`
	Allocated        uint64  `json:"go_allocated_bytes"`
	GCCycles         uint32  `json:"gc_cycles"`
	ManagedPeak      int64   `json:"managed_peak_bytes"`
	ManagedAfter     int64   `json:"managed_after_bytes"`
	MemoryLimit      int64   `json:"memory_limit_bytes"`
	TempPeak         int64   `json:"temp_peak_bytes"`
	TempAfter        int64   `json:"temp_after_bytes"`
	NetworkAvailable bool    `json:"network_available"`
	IORequests       int64   `json:"io_requests"`
	IOBytes          int64   `json:"io_bytes"`
	RangeCalls       int64   `json:"range_requests"`
	FullGETs         int64   `json:"full_get_requests"`
	BadRanges        int64   `json:"non_206_range_responses"`
	OrderViolations  int64   `json:"order_violations"`
	FirstViolation   int64   `json:"first_violation_row"`
}

// compareConfig 按来源构造公共配置；本地与 S3 都通过真实驱动读取。
func compareConfig(t *testing.T, scene compareCase) Config {
	t.Helper()
	tempLimit := scene.TempLimit
	if tempLimit == 0 {
		tempLimit = 1 << 30
	}
	workers := scene.Workers
	if workers == 0 {
		workers = 1
	}
	config := Config{
		MemoryLimitBytes:       scene.MemoryLimit,
		SpillMemoryBudgetBytes: scene.SpillBudget,
		TempLimitBytes:         tempLimit,
		TempDir:                t.TempDir(),
		ManifestDir:            t.TempDir(),
		Workers:                workers,
	}
	switch scene.Source {
	case "local":
		root := os.Getenv("FSSQL_COMPARE_ROOT")
		if root == "" {
			t.Fatal("FSSQL_COMPARE_ROOT is required for local source")
		}
		config.LocalFiles = &LocalFileConfig{RootDir: root}
	case "s3":
		if !strings.HasPrefix(os.Getenv("FSSQL_MINIO_BUCKET"), "fssql-agent-test-") {
			t.Fatal("large compare requires a dedicated test bucket")
		}
		config.S3 = &S3Config{
			Endpoint: os.Getenv("FSSQL_MINIO_ENDPOINT"), Region: "us-east-1", ForcePathStyle: true,
			AllowInsecureHTTP: true, AccessKey: os.Getenv("FSSQL_MINIO_ACCESS_KEY"),
			SecretKey: os.Getenv("FSSQL_MINIO_SECRET_KEY"), RequestTimeout: 30 * time.Second,
		}
	default:
		t.Fatalf("unsupported source %q", scene.Source)
	}
	return config
}

// TestDuckDBCompareCase 执行一个场景并把结果写入 FSSQL_COMPARE_OUT。
func TestDuckDBCompareCase(t *testing.T) {
	path := os.Getenv("FSSQL_COMPARE_CASE")
	out := os.Getenv("FSSQL_COMPARE_OUT")
	if path == "" || out == "" {
		t.Skip("set FSSQL_COMPARE_CASE and FSSQL_COMPARE_OUT to run one compare case")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var scene compareCase
	if err := json.Unmarshal(raw, &scene); err != nil {
		t.Fatal(err)
	}
	config := compareConfig(t, scene)
	db, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var meter *minIOLargeMeter
	if scene.Source == "s3" {
		meter = minIOLargeInstallMeter(t)
		meter.reset()
	}
	debug.FreeOSMemory()
	finish := minIOLargeObserve(config.TempDir)
	defer finish()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, cpuBefore, err := minIOLargeSystemMetrics()
	if err != nil {
		t.Fatal(err)
	}

	var rows int64
	var orderViolations int64
	firstViolation := int64(-1)
	var previous []int64
	havePrevious := false
	start := time.Now()
	_, queryErr := db.QueryChunks(context.Background(), scene.SQL, func(chunk *Chunk) error {
		if len(scene.OrderColumns) > 0 {
			values := make([]int64, len(scene.OrderColumns))
			for row := 0; row < chunk.RowCount(); row++ {
				for index, column := range scene.OrderColumns {
					value, _, err := chunk.Int64(row, column)
					if err != nil {
						return err
					}
					values[index] = value
				}
				if havePrevious && compareOrderLess(values, previous) {
					orderViolations++
					if firstViolation < 0 {
						firstViolation = rows + int64(row)
					}
				}
				previous = append(previous[:0], values...)
				havePrevious = true
			}
		}
		rows += int64(chunk.RowCount())
		return nil
	})
	seconds := time.Since(start).Seconds()
	if queryErr != nil {
		t.Fatalf("query failed: %v", queryErr)
	}
	if rows != scene.ExpectedRows {
		t.Fatalf("rows=%d want=%d", rows, scene.ExpectedRows)
	}
	runtime.ReadMemStats(&after)
	_, cpuAfter, err := minIOLargeSystemMetrics()
	if err != nil {
		t.Fatal(err)
	}
	resources := finish()
	managedAfter := db.MemoryUsed()
	tempAfter := minIOLargeTempBytes(config.TempDir)
	// 使用精确配额补充目录采样，保持 spill 与回收指标跨平台一致。
	resources.TempPeak = max(resources.TempPeak, db.temporary.Peak())
	tempAfter = max(tempAfter, db.temporary.Used())
	if managedAfter != 0 || tempAfter != 0 {
		t.Fatalf("resource leak: managed=%d temp=%d", managedAfter, tempAfter)
	}
	result := compareResult{
		Name: scene.Name, Source: scene.Source, Seconds: seconds, Rows: rows,
		CPUSeconds: cpuAfter - cpuBefore, BaseRSS: resources.BaseRSS, PeakRSS: resources.PeakRSS,
		PeakHeap: resources.PeakHeap, Allocated: after.TotalAlloc - before.TotalAlloc,
		GCCycles: after.NumGC - before.NumGC, ManagedPeak: db.memory.Peak(),
		ManagedAfter: managedAfter, MemoryLimit: scene.MemoryLimit,
		TempPeak: resources.TempPeak, TempAfter: tempAfter,
		OrderViolations: orderViolations, FirstViolation: firstViolation,
	}
	if seconds > 0 {
		result.CPUPercent = result.CPUSeconds / seconds * 100
	}
	if meter != nil {
		result.NetworkAvailable = true
		result.IORequests = meter.requests.Load()
		result.IOBytes = meter.bytes.Load()
		result.RangeCalls = meter.ranges.Load()
		result.FullGETs = meter.full.Load()
		result.BadRanges = meter.bad.Load()
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(out, append(encoded, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Log(fmt.Sprintf("%s %s %.3fs rows=%d managed_peak=%d rss_peak=%d",
		result.Name, result.Source, result.Seconds, result.Rows, result.ManagedPeak, result.PeakRSS))
}

// compareOrderLess 判断当前元组是否严格小于前一元组，用于发现 ORDER BY 逆序。
func compareOrderLess(current, previous []int64) bool {
	for index := range current {
		if current[index] != previous[index] {
			return current[index] < previous[index]
		}
	}
	return false
}

package gosql

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"math/bits"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

// minIOLargeCase 描述一个真实数据场景及独立参考值；所有资源预算均显式有界。
type minIOLargeCase struct {
	Name           string `json:"name"`            // Name 是报告中的稳定场景标识。
	SQL            string `json:"sql"`             // SQL 只访问专用测试 bucket。
	Kind           string `json:"kind"`            // Kind 区分正常、预期失败与不支持的能力。
	Ordered        bool   `json:"ordered"`         // Ordered 决定结果比较是否考虑行顺序。
	MemoryLimit    int64  `json:"memory_limit"`    // MemoryLimit 是引擎受管内存上限。
	SpillBudget    int64  `json:"spill_budget"`    // SpillBudget 是各阻塞算子的内存预算。
	TempLimit      int64  `json:"temp_limit"`      // TempLimit 限制测试临时文件。
	NeedsSpill     bool   `json:"needs_spill"`     // NeedsSpill 要求观测到实际 spill 文件。
	NoSpill        bool   `json:"no_spill"`        // NoSpill 标记用于比较的内存执行场景。
	Workers        int    `json:"workers"`         // Workers 覆盖默认单 worker，以验证并行执行。
	ExpectedRows   int64  `json:"expected_rows"`   // ExpectedRows 来自独立 DuckDB 执行。
	ExpectedDigest string `json:"expected_digest"` // ExpectedDigest 是完整结果摘要。
	ErrorContains  string `json:"error_contains"`  // ErrorContains 限制预期的不支持错误。
	CopySource     string `json:"copy_source"`     // CopySource 是 COPY 的实际输入查询。
}

// minIOLargeManifest 保存大文件规模与完整场景列表，不包含服务凭据。
type minIOLargeManifest struct {
	Rows  int64            `json:"rows"`  // Rows 是主数据集行数。
	Files map[string]int64 `json:"files"` // Files 是实际上传的对象大小。
	Cases []minIOLargeCase `json:"cases"` // Cases 是独立生成的参考场景。
}

// minIOLargeResult 是每个场景的资源与正确性证据，CPU 100% 表示一个逻辑核。
type minIOLargeResult struct {
	Name         string  `json:"name"`                    // Name 对应参考场景。
	Kind         string  `json:"kind"`                    // Kind 记录测试类型，避免混淆未支持能力。
	Status       string  `json:"status"`                  // Status 是 pass、fail、unsupported 或 expected_error。
	Error        string  `json:"error,omitempty"`         // Error 保存真实错误，不包含凭据。
	Rows         int64   `json:"rows"`                    // Rows 是实际返回或抽取行数。
	Seconds      float64 `json:"seconds"`                 // Seconds 是端到端查询耗时。
	CPUSeconds   float64 `json:"cpu_seconds"`             // CPUSeconds 是进程用户态与内核态 CPU 时间差。
	CPUPercent   float64 `json:"cpu_percent"`             // CPUPercent 允许多核使用率大于 100。
	PeakCPU      float64 `json:"peak_cpu_percent"`        // PeakCPU 是定时采样得到的 CPU 峰值。
	LogicalCPUs  int     `json:"logical_cpus"`            // LogicalCPUs 是进程可见逻辑核数。
	BaseRSS      uint64  `json:"rss_base_bytes"`          // BaseRSS 是查询开始前驻留集。
	PeakRSS      uint64  `json:"rss_peak_bytes"`          // PeakRSS 是采样驻留集峰值。
	PeakHeap     uint64  `json:"go_heap_peak_bytes"`      // PeakHeap 是采样 Go live heap 峰值。
	Allocated    uint64  `json:"go_allocated_bytes"`      // Allocated 是查询期间 Go 累计分配量。
	GCCycles     uint32  `json:"gc_cycles"`               // GCCycles 是查询期间完成的 GC 次数。
	ManagedPeak  int64   `json:"managed_peak_bytes"`      // ManagedPeak 是 MemoryManager 的精确峰值。
	ManagedAfter int64   `json:"managed_after_bytes"`     // ManagedAfter 必须归零。
	MemoryLimit  int64   `json:"memory_limit_bytes"`      // MemoryLimit 是场景上限。
	TempPeak     int64   `json:"temp_peak_bytes"`         // TempPeak 是采样临时文件逻辑大小峰值。
	TempAfter    int64   `json:"temp_after_bytes"`        // TempAfter 必须归零。
	IORequests   int64   `json:"io_requests"`             // IORequests 包括 HEAD、List 和 Range。
	IOBytes      int64   `json:"io_bytes"`                // IOBytes 是实际读取的 HTTP body 字节数。
	RangeCalls   int64   `json:"range_requests"`          // RangeCalls 是带 Range 的 GET 次数。
	FullGETs     int64   `json:"full_get_requests"`       // FullGETs 标记对象读取是否静默退回整文件。
	BadRanges    int64   `json:"non_206_range_responses"` // BadRanges 标记未遵守 Range 的成功响应。
}

// minIOLargeMeter 在真实 S3 HTTP 传输上统计实际读取量，不替换 VFS 或远端服务。
type minIOLargeMeter struct {
	base     http.RoundTripper // base 保留正常 HTTP 传输行为。
	host     string            // host 仅匹配测试 endpoint。
	requests atomic.Int64      // requests 是总请求数。
	bytes    atomic.Int64      // bytes 是消费的响应 body 大小。
	ranges   atomic.Int64      // ranges 是 Range 请求数。
	full     atomic.Int64      // full 是无 Range 的对象 GET 数。
	bad      atomic.Int64      // bad 是 Range 返回非 206 的成功响应数。
}

// minIOLargeBody 只统计响应流的消费量，不缓存大文件。
type minIOLargeBody struct {
	io.ReadCloser               // ReadCloser 保留原始响应流生命周期。
	bytes         *atomic.Int64 // bytes 是所属场景的计数器。
}

// Read 在底层读取后累计真实字节数。
func (b *minIOLargeBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.bytes.Add(int64(n))
	return n, err
}

// RoundTrip 观测真实 HTTP 请求，错误和响应均原样交回 S3 driver。
func (m *minIOLargeMeter) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Host != m.host {
		return m.base.RoundTrip(req)
	}
	m.requests.Add(1)
	ranged := req.Method == http.MethodGet && req.Header.Get("Range") != ""
	if ranged {
		m.ranges.Add(1)
	} else if req.Method == http.MethodGet && req.URL.Query().Get("list-type") == "" {
		m.full.Add(1)
	}
	response, err := m.base.RoundTrip(req)
	if err == nil {
		if ranged && response.StatusCode >= 200 && response.StatusCode < 300 && response.StatusCode != http.StatusPartialContent {
			m.bad.Add(1)
		}
		response.Body = &minIOLargeBody{ReadCloser: response.Body, bytes: &m.bytes}
	}
	return response, err
}

// reset 仅在前一个场景的所有查询完成后重置原子计数器。
func (m *minIOLargeMeter) reset() {
	m.requests.Store(0)
	m.bytes.Store(0)
	m.ranges.Store(0)
	m.full.Store(0)
	m.bad.Store(0)
}

// minIOLargeDigest 按有序流 SHA256 或无序行 SHA256 和对比完整结果，不物化结果集。
type minIOLargeDigest struct {
	ordered bool      // ordered 对应 SQL 是否显式排序。
	hash    hash.Hash // hash 保存有序流的摘要。
	sum     [4]uint64 // sum 保存模 2^256 的无序行摘要和。
	rows    int64     // rows 记录结果行数。
}

// add 累计一行规范编码，固定大小摘要不随数据量增长。
func (d *minIOLargeDigest) add(row []byte) {
	if d.ordered {
		_, _ = d.hash.Write(row)
	} else {
		sum := sha256.Sum256(row)
		carry := uint64(0)
		for index := range d.sum {
			d.sum[index], carry = bits.Add64(d.sum[index], binary.LittleEndian.Uint64(sum[index*8:]), carry)
		}
	}
	d.rows++
}

// text 返回与 Python 参考程序一致的十六进制摘要。
func (d *minIOLargeDigest) text() string {
	if d.ordered {
		return hex.EncodeToString(d.hash.Sum(nil))
	}
	var raw [32]byte
	for index, value := range d.sum {
		binary.LittleEndian.PutUint64(raw[index*8:], value)
	}
	return hex.EncodeToString(raw[:])
}

// minIOLargeEncodeRow 编码公开 Chunk 中的标量；结果校验使用有界的单行 scratch。
func minIOLargeEncodeRow(chunk *Chunk, row int, types []Type, scratch []byte) ([]byte, error) {
	for column, logical := range types {
		null, err := chunk.IsNull(row, column)
		if err != nil {
			return nil, err
		}
		if null {
			scratch = append(scratch, 'N', ';')
			continue
		}
		switch logical.ID {
		case "VARCHAR":
			value, _, err := chunk.String(row, column)
			if err != nil {
				return nil, err
			}
			scratch = append(scratch, 'S')
			scratch = strconv.AppendInt(scratch, int64(len(value)), 10)
			scratch = append(scratch, ':')
			scratch = append(scratch, value...)
		case "BOOLEAN":
			value, _, err := chunk.Bool(row, column)
			if err != nil {
				return nil, err
			}
			scratch = append(scratch, 'B')
			if value {
				scratch = append(scratch, '1')
			} else {
				scratch = append(scratch, '0')
			}
		case "FLOAT", "DOUBLE":
			value, _, err := chunk.Float64(row, column)
			if err != nil {
				return nil, err
			}
			scratch = append(scratch, 'F')
			scratch = strconv.AppendFloat(scratch, value, 'g', 9, 64)
		case "HUGEINT":
			value, _, err := chunk.Int128(row, column)
			if err != nil {
				return nil, err
			}
			scratch = append(scratch, 'I')
			scratch = append(scratch, value.String()...)
		default:
			value, _, err := chunk.Int64(row, column)
			if err != nil {
				return nil, err
			}
			scratch = append(scratch, 'I')
			scratch = strconv.AppendInt(scratch, value, 10)
		}
		scratch = append(scratch, ';')
	}
	return append(scratch, '\n'), nil
}

// minIOLargeTempBytes 统计查询 spill 目录的逻辑文件大小，容忍并发清理导致的文件消失。
func minIOLargeTempBytes(root string) int64 {
	var size int64
	_ = filepath.WalkDir(root, func(_ string, entry fs.DirEntry, err error) error {
		if err == nil && !entry.IsDir() {
			if info, err := entry.Info(); err == nil {
				size += info.Size()
			}
		}
		return nil
	})
	return size
}

// minIOLargeObserve 每 20ms 采样资源峰值，返回前停止唯一采样 goroutine。
func minIOLargeObserve(temp string) func() minIOLargeResult {
	stop, done := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var result minIOLargeResult
	result.BaseRSS, _, _ = minIOLargeSystemMetrics()
	go func() {
		defer close(done)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		_, previousCPU, _ := minIOLargeSystemMetrics()
		previousTime := time.Now()
		for {
			var heap runtime.MemStats
			runtime.ReadMemStats(&heap)
			if heap.HeapAlloc > result.PeakHeap {
				result.PeakHeap = heap.HeapAlloc
			}
			rss, cpu, _ := minIOLargeSystemMetrics()
			if rss > result.PeakRSS {
				result.PeakRSS = rss
			}
			now := time.Now()
			// Windows CPU 时间有计时粒度，峰值至少积累 100ms，避免首个短窗口夸大结果。
			if interval := now.Sub(previousTime).Seconds(); interval >= 0.1 {
				usage := (cpu - previousCPU) / interval * 100
				if usage > result.PeakCPU {
					result.PeakCPU = usage
				}
				previousCPU, previousTime = cpu, now
			}
			if size := minIOLargeTempBytes(temp); size > result.TempPeak {
				result.TempPeak = size
			}
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
		}
	}()
	return func() minIOLargeResult {
		once.Do(func() {
			close(stop)
			<-done
		})
		return result
	}
}

// minIOLargeLoad 读取最多 1MiB 的场景描述；大数据本身始终通过 MinIO 流式读取。
func minIOLargeLoad(tb testing.TB) minIOLargeManifest {
	tb.Helper()
	path := os.Getenv("FSSQL_LARGE_MANIFEST")
	if path == "" {
		tb.Skip("设置 FSSQL_LARGE_MANIFEST 后启用真实大文件矩阵")
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() > 1<<20 {
		tb.Fatalf("invalid fixture manifest: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		tb.Fatal(err)
	}
	var manifest minIOLargeManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		tb.Fatal(err)
	}
	return manifest
}

// minIOLargeConfig 构造有界测试配置，只允许专用测试 bucket。
func minIOLargeConfig(tb testing.TB, scene minIOLargeCase) Config {
	tb.Helper()
	readOnlyBucket := os.Getenv("FSSQL_LARGE_READONLY_BUCKET")
	readOnly := readOnlyBucket != "" && readOnlyBucket == os.Getenv("FSSQL_MINIO_BUCKET") && scene.Kind == "compare" && strings.HasPrefix(strings.ToUpper(strings.TrimSpace(scene.SQL)), "SELECT ")
	if !strings.HasPrefix(os.Getenv("FSSQL_MINIO_BUCKET"), "fssql-agent-test-") && !readOnly {
		tb.Fatal("large matrix requires a dedicated test bucket")
	}
	limit := scene.TempLimit
	if limit == 0 {
		limit = 1 << 30
	}
	workers := scene.Workers
	if workers == 0 {
		workers = 1
	}
	return Config{MemoryLimitBytes: scene.MemoryLimit, SpillMemoryBudgetBytes: scene.SpillBudget,
		TempLimitBytes: limit, TempDir: tb.TempDir(), ManifestDir: tb.TempDir(), Workers: workers,
		S3: &S3Config{Endpoint: os.Getenv("FSSQL_MINIO_ENDPOINT"), Region: "us-east-1", ForcePathStyle: true,
			AllowInsecureHTTP: true, AccessKey: os.Getenv("FSSQL_MINIO_ACCESS_KEY"), SecretKey: os.Getenv("FSSQL_MINIO_SECRET_KEY"), RequestTimeout: 30 * time.Second}}
}

// minIOLargeInstallMeter 安装仅用于串行测试进程的传输观测器，结束时恢复默认 transport。
func minIOLargeInstallMeter(tb testing.TB) *minIOLargeMeter {
	tb.Helper()
	endpoint, err := url.Parse(os.Getenv("FSSQL_MINIO_ENDPOINT"))
	if err != nil || endpoint.Host == "" {
		tb.Fatal("invalid MinIO endpoint")
	}
	original := http.DefaultTransport
	transport := original.(*http.Transport).Clone()
	meter := &minIOLargeMeter{base: transport, host: endpoint.Host}
	http.DefaultTransport = meter
	tb.Cleanup(func() {
		http.DefaultTransport = original
		transport.CloseIdleConnections()
	})
	return meter
}

// minIOLargeSplitMix 生成与 Python 参考相同的 64 位随机数。
func minIOLargeSplitMix(state *uint64) uint64 {
	*state += 0x9e3779b97f4a7c15
	value := *state
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

// minIOLargeReservoirConsider 用 Algorithm R 更新固定容量样本，拒绝取模偏差且不保留完整输入。
func minIOLargeReservoirConsider(sample []int64, seen, value int64, state *uint64) {
	if seen <= int64(len(sample)) {
		sample[seen-1] = value
		return
	}
	bound := uint64(seen)
	limit := ^uint64(0) - ^uint64(0)%bound
	random := minIOLargeSplitMix(state)
	for random >= limit {
		random = minIOLargeSplitMix(state)
	}
	if slot := random % bound; slot < uint64(len(sample)) {
		sample[slot] = value
	}
}

// minIOLargeRun 执行一个场景并对完整结果与错误类型做验证。
func minIOLargeRun(tb testing.TB, scene minIOLargeCase, manifest minIOLargeManifest, meter *minIOLargeMeter) minIOLargeResult {
	tb.Helper()
	config := minIOLargeConfig(tb, scene)
	db, err := Open(config)
	if err != nil {
		tb.Fatal(err)
	}
	defer db.Close()
	debug.FreeOSMemory()
	meter.reset()
	finish := minIOLargeObserve(config.TempDir)
	defer finish()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, cpuBefore, systemErr := minIOLargeSystemMetrics()
	if systemErr != nil {
		_ = finish()
		tb.Fatal(systemErr)
	}
	start := time.Now()
	deadline := 2 * time.Minute
	if scene.Kind == "repeat" {
		deadline = 10 * time.Minute
	}
	if setting := os.Getenv("FSSQL_LARGE_TIMEOUT"); setting != "" {
		configured, parseErr := time.ParseDuration(setting)
		if parseErr != nil || configured < time.Second || configured > 20*time.Minute {
			tb.Fatal("FSSQL_LARGE_TIMEOUT must be between 1s and 20m")
		}
		deadline = configured
	}
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	digest := &minIOLargeDigest{ordered: scene.Ordered, hash: sha256.New()}
	consumerError := errors.New("large matrix consumer stopped")
	var sample [1000]int64
	var seen int64
	state := uint64(42)
	var types []Type
	var scratch [2048]byte
	var callbackMu sync.Mutex
	callback := func(chunk *Chunk) error {
		callbackMu.Lock()
		defer callbackMu.Unlock()
		if scene.Kind == "cancel" {
			cancel()
			return nil
		}
		if scene.Kind == "consumer_error" {
			return consumerError
		}
		if types == nil {
			types = make([]Type, chunk.ColumnCount())
			for column := range types {
				types[column], _ = chunk.Type(column)
			}
		}
		for row := 0; row < chunk.RowCount(); row++ {
			if scene.Kind == "reservoir" {
				identifier, _, err := chunk.Int64(row, 0)
				if err != nil {
					return err
				}
				seen++
				minIOLargeReservoirConsider(sample[:], seen, identifier, &state)
				continue
			}
			if scene.Kind == "limit" || scene.Kind == "first_chunk" {
				identifier, _, err := chunk.Int64(row, 0)
				if err != nil || identifier < 0 || identifier >= manifest.Rows {
					return fmt.Errorf("invalid limited row id=%d err=%v", identifier, err)
				}
				payload, _, err := chunk.String(row, 1)
				value := sha256.Sum256(strconv.AppendInt(scratch[:0], identifier, 10))
				if err != nil || payload != strings.Repeat(hex.EncodeToString(value[:]), 4) {
					return fmt.Errorf("invalid limited row payload: id=%d err=%v", identifier, err)
				}
			}
			encoded, err := minIOLargeEncodeRow(chunk, row, types, scratch[:0])
			if err != nil {
				return err
			}
			digest.add(encoded)
		}
		if scene.Kind == "first_chunk" {
			return consumerError
		}
		return nil
	}
	query := scene.SQL
	if scene.Kind == "copy" {
		uri := fmt.Sprintf("s3://%s/large/output-%d.parquet", os.Getenv("FSSQL_MINIO_BUCKET"), time.Now().UnixNano())
		err = db.Exec(ctx, fmt.Sprintf("COPY (%s) TO '%s' (FORMAT parquet)", scene.CopySource, uri))
		query = fmt.Sprintf("SELECT id, value, payload FROM read_parquet('%s')", uri)
	}
	if err == nil && scene.Kind == "concurrent" {
		var group sync.WaitGroup
		results := make(chan error, 4)
		for worker := 0; worker < 4; worker++ {
			group.Add(1)
			go func() {
				defer group.Done()
				local := &minIOLargeDigest{hash: sha256.New()}
				_, err := db.QueryChunks(ctx, query, func(chunk *Chunk) error {
					types := make([]Type, chunk.ColumnCount())
					for index := range types {
						types[index], _ = chunk.Type(index)
					}
					var rowBuffer [2048]byte
					for row := 0; row < chunk.RowCount(); row++ {
						encoded, err := minIOLargeEncodeRow(chunk, row, types, rowBuffer[:0])
						if err != nil {
							return err
						}
						local.add(encoded)
					}
					return nil
				})
				if err == nil && (local.rows != scene.ExpectedRows || local.text() != scene.ExpectedDigest) {
					err = errors.New("concurrent result mismatch")
				}
				results <- err
			}()
		}
		group.Wait()
		close(results)
		for failure := range results {
			err = errors.Join(err, failure)
		}
		digest.rows = scene.ExpectedRows * 4
	} else if err == nil {
		repeats := 1
		if scene.Kind == "repeat" {
			repeats = 20
		}
		for iteration := 0; iteration < repeats; iteration++ {
			digest.rows, digest.sum = 0, [4]uint64{}
			digest.hash.Reset()
			_, err = db.QueryChunks(ctx, query, callback)
			if err != nil {
				break
			}
			if used := db.MemoryUsed(); used != 0 {
				err = fmt.Errorf("iteration %d retained managed memory=%d", iteration, used)
				break
			}
		}
	}
	if scene.Kind == "reservoir" && err == nil {
		if seen != manifest.Rows {
			err = fmt.Errorf("reservoir input rows=%d want=%d", seen, manifest.Rows)
		}
		for _, identifier := range sample {
			encoded := append(strconv.AppendInt(append(scratch[:0], 'I'), identifier, 10), ';', '\n')
			digest.add(encoded)
		}
	}
	elapsed := time.Since(start).Seconds()
	_, cpuAfter, _ := minIOLargeSystemMetrics()
	runtime.ReadMemStats(&after)
	result := finish()
	result.Name, result.Kind, result.Status = scene.Name, scene.Kind, "pass"
	result.Rows, result.Seconds = digest.rows, elapsed
	result.CPUSeconds = cpuAfter - cpuBefore
	if elapsed > 0 {
		result.CPUPercent = result.CPUSeconds / elapsed * 100
	}
	result.LogicalCPUs = runtime.NumCPU()
	result.ManagedPeak, result.ManagedAfter, result.MemoryLimit = db.memory.Peak(), db.MemoryUsed(), scene.MemoryLimit
	result.TempAfter = minIOLargeTempBytes(config.TempDir)
	result.Allocated, result.GCCycles = after.TotalAlloc-before.TotalAlloc, after.NumGC-before.NumGC
	result.IORequests, result.IOBytes = meter.requests.Load(), meter.bytes.Load()
	result.RangeCalls, result.FullGETs, result.BadRanges = meter.ranges.Load(), meter.full.Load(), meter.bad.Load()
	expected := false
	switch scene.Kind {
	case "unsupported":
		expected = err != nil && strings.Contains(strings.ToLower(err.Error()), scene.ErrorContains)
		result.Status = "unsupported"
	case "cancel":
		expected = errors.Is(err, context.Canceled)
		result.Status = "expected_error"
	case "consumer_error":
		expected = errors.Is(err, consumerError)
		result.Status = "expected_error"
	case "first_chunk":
		expected = errors.Is(err, consumerError) && digest.rows > 0
		result.Status = "expected_error"
	case "memory_limit":
		expected = errors.Is(err, memory.ErrLimitExceeded)
		result.Status = "expected_error"
	case "temp_quota":
		expected = errors.Is(err, storagetemp.ErrQuotaExceeded)
		result.Status = "expected_error"
	default:
		expected = err == nil
		if expected && scene.Kind != "concurrent" && scene.Kind != "limit" && (digest.rows != scene.ExpectedRows || digest.text() != scene.ExpectedDigest) {
			expected = false
			err = fmt.Errorf("DuckDB mismatch: rows=%d/%d digest=%s/%s", digest.rows, scene.ExpectedRows, digest.text(), scene.ExpectedDigest)
		}
		if expected && scene.Kind == "limit" && digest.rows != scene.ExpectedRows {
			expected = false
			err = fmt.Errorf("limited rows=%d want=%d", digest.rows, scene.ExpectedRows)
		}
	}
	if !expected || result.ManagedAfter != 0 || result.TempAfter != 0 || result.ManagedPeak > scene.MemoryLimit || result.FullGETs != 0 || result.BadRanges != 0 || (scene.NeedsSpill && result.TempPeak == 0) || (scene.NoSpill && result.TempPeak != 0) {
		result.Status = "fail"
		result.Error = fmt.Sprintf("error=%v; managed=%d peak=%d/%d temp=%d peak=%d full_get=%d bad_range=%d", err, result.ManagedAfter, result.ManagedPeak, scene.MemoryLimit, result.TempAfter, result.TempPeak, result.FullGETs, result.BadRanges)
	} else if err != nil {
		result.Error = err.Error()
	}
	return result
}

// TestMinIOLargeReservoirProperties 验证多种固定种子的样本唯一性、取值范围及分层覆盖。
func TestMinIOLargeReservoirProperties(t *testing.T) {
	var bins [10]int
	for seed := uint64(0); seed < 128; seed++ {
		var sample [100]int64
		state := seed
		for value := int64(0); value < 10000; value++ {
			minIOLargeReservoirConsider(sample[:], value+1, value, &state)
		}
		seen := make(map[int64]bool, len(sample))
		for _, value := range sample {
			if value < 0 || value >= 10000 || seen[value] {
				t.Fatalf("invalid reservoir value=%d seed=%d", value, seed)
			}
			seen[value] = true
			bins[value/1000]++
		}
	}
	// 固定输入和种子保证可重现，宽容差用于发现偏向流头或流尾的明显错误。
	for index, count := range bins {
		if count < 900 || count > 1700 {
			t.Fatalf("biased stratum %d count=%d bins=%v", index, count, bins)
		}
	}
}

// TestMinIOLargeMatrix 验证百万行真实远端数据的正确性、资源边界与失败清理。
func TestMinIOLargeMatrix(t *testing.T) {
	manifest := minIOLargeLoad(t)
	meter := minIOLargeInstallMeter(t)
	report := os.Getenv("FSSQL_LARGE_REPORT")
	if report == "" {
		t.Fatal("FSSQL_LARGE_REPORT is required to retain evidence")
	}
	if err := os.WriteFile(report, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	selected := "," + os.Getenv("FSSQL_LARGE_CASES") + ","
	for _, scene := range manifest.Cases {
		if selected != ",," && !strings.Contains(selected, ","+scene.Name+",") {
			continue
		}
		t.Run(scene.Name, func(t *testing.T) {
			result := minIOLargeRun(t, scene, manifest, meter)
			raw, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			file, err := os.OpenFile(report, os.O_APPEND|os.O_WRONLY, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			_, writeErr := file.Write(append(raw, '\n'))
			closeErr := file.Close()
			if writeErr != nil || closeErr != nil {
				t.Fatalf("report write=%v close=%v", writeErr, closeErr)
			}
			t.Logf("%s", raw)
			if result.Status == "fail" {
				t.Error(result.Error)
			}
		})
	}
}

// BenchmarkMinIOLarge 使用只计行数的回调测量真实查询，排除逐行 SHA 校验的开销。
func BenchmarkMinIOLarge(b *testing.B) {
	manifest := minIOLargeLoad(b)
	meter := minIOLargeInstallMeter(b)
	cases := append([]minIOLargeCase(nil), manifest.Cases...)
	for _, scene := range manifest.Cases {
		if scene.Name == "scan_narrow" {
			scene.Name, scene.Workers, scene.MemoryLimit = "scan_workers4", 4, 128<<20
			cases = append(cases, scene)
			break
		}
	}
	for _, scene := range cases {
		if scene.Name != "scan_narrow" && scene.Name != "scan_wide" && scene.Name != "filter_range" && scene.Name != "topn" && scene.Name != "sort_spill" && scene.Name != "sort_in_memory" && scene.Name != "aggregate_spill" && scene.Name != "join_spill" && scene.Name != "scan_workers4" {
			continue
		}
		b.Run(scene.Name, func(b *testing.B) {
			config := minIOLargeConfig(b, scene)
			db, err := Open(config)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			meter.reset()
			b.ReportAllocs()
			finish := minIOLargeObserve(config.TempDir)
			defer finish()
			_, cpuBefore, err := minIOLargeSystemMetrics()
			if err != nil {
				_ = finish()
				b.Fatal(err)
			}
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				var rows atomic.Int64
				_, err := db.QueryChunks(context.Background(), scene.SQL, func(chunk *Chunk) error {
					rows.Add(int64(chunk.RowCount()))
					return nil
				})
				if err != nil || rows.Load() != scene.ExpectedRows || db.MemoryUsed() != 0 {
					b.Fatalf("rows=%d/%d memory=%d err=%v", rows.Load(), scene.ExpectedRows, db.MemoryUsed(), err)
				}
			}
			b.StopTimer()
			_, cpuAfter, _ := minIOLargeSystemMetrics()
			resources := finish()
			b.ReportMetric(float64(scene.ExpectedRows), "rows/op")
			b.ReportMetric(float64(scene.ExpectedRows)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
			b.ReportMetric(float64(meter.bytes.Load())/float64(b.N), "net-B/op")
			b.ReportMetric(float64(db.memory.Peak()), "managed-peak-B")
			b.ReportMetric(float64(resources.PeakRSS), "rss-peak-B")
			b.ReportMetric(float64(resources.PeakHeap), "heap-peak-B")
			b.ReportMetric(float64(resources.TempPeak), "temp-peak-B")
			b.ReportMetric((cpuAfter-cpuBefore)/b.Elapsed().Seconds()*100, "cpu-%")
			b.ReportMetric((cpuAfter-cpuBefore)/float64(b.N), "cpu-s/op")
		})
	}
}

// TestMinIOLargeWorkers4 对同一百万行源验证四个 CPU worker 的结果与资源回收。
func TestMinIOLargeWorkers4(t *testing.T) {
	manifest := minIOLargeLoad(t)
	meter := minIOLargeInstallMeter(t)
	for _, scene := range manifest.Cases {
		if scene.Name != "scan_narrow" {
			continue
		}
		scene.Name, scene.Workers, scene.MemoryLimit = "scan_workers4", 4, 128<<20
		result := minIOLargeRun(t, scene, manifest, meter)
		raw, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(os.Getenv("FSSQL_LARGE_REPORT"), append(raw, '\n'), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Log(string(raw))
		if result.Status == "fail" {
			t.Error(result.Error)
		}
		return
	}
	t.Fatal("scan_narrow reference missing")
}

// TestMinIOLargeDefaultBudget 验证默认 256MiB 引擎预算能否扫描本轮大文本对象。
func TestMinIOLargeDefaultBudget(t *testing.T) {
	manifest := minIOLargeLoad(t)
	meter := minIOLargeInstallMeter(t)
	report := os.Getenv("FSSQL_LARGE_REPORT")
	if err := os.WriteFile(report, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, scene := range manifest.Cases {
		if scene.Name != "csv_scan" && scene.Name != "ndjson_scan" {
			continue
		}
		scene.Name += "_default256"
		scene.MemoryLimit = defaultMemoryLimit
		scene.SpillBudget = defaultSpillMemoryBudget
		t.Run(scene.Name, func(t *testing.T) {
			result := minIOLargeRun(t, scene, manifest, meter)
			raw, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			file, err := os.OpenFile(report, os.O_APPEND|os.O_WRONLY, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			_, writeErr := file.Write(append(raw, '\n'))
			closeErr := file.Close()
			if writeErr != nil || closeErr != nil {
				t.Fatalf("report write=%v close=%v", writeErr, closeErr)
			}
			t.Log(string(raw))
			if result.Status == "fail" {
				t.Error(result.Error)
			}
		})
	}
}

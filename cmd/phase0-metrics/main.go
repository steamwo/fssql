package main

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"

	phase0 "github.com/steamwo/fssql/internal/demo/phase0"
)

type metrics struct {
	Rows                  int64  `json:"rows"`
	Sum                   int64  `json:"sum"`
	ManagedPeakBytes      int64  `json:"managed_peak_bytes"`
	ManagedEndBytes       int64  `json:"managed_end_bytes"`
	GoHeapLiveBytes       uint64 `json:"go_heap_live_bytes"`
	GoTotalAllocatedBytes uint64 `json:"go_total_allocated_bytes"`
	RSSBytes              uint64 `json:"rss_bytes"`
	GCCycles              uint32 `json:"gc_cycles"`
	TempDiskPeakBytes     int64  `json:"temp_disk_peak_bytes"`
}

func rssBytes() uint64 {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	f := strings.Fields(string(b))
	if len(f) < 2 {
		return 0
	}
	pages, err := strconv.ParseUint(f[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * uint64(os.Getpagesize())
}
func main() {
	r, err := phase0.Vector10M(10_000_000)
	if err != nil {
		panic(err)
	}
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	m := metrics{Rows: r.Rows, Sum: r.Sum, ManagedPeakBytes: r.ManagedPeak, ManagedEndBytes: r.ManagedEnd, GoHeapLiveBytes: ms.HeapAlloc, GoTotalAllocatedBytes: ms.TotalAlloc, RSSBytes: rssBytes(), GCCycles: ms.NumGC, TempDiskPeakBytes: 0}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(m); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

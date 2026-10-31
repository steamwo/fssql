package main

import (
	"flag"
	"fmt"
	phase0 "github.com/steamwo/fssql/internal/demo/phase0"
	"log"
)

func main() {
	mode := flag.String("mode", "vector", "vector|memory|pipeline")
	rows := flag.Int64("rows", 10_000_000, "rows")
	workers := flag.Int("workers", 4, "pipeline workers")
	flag.Parse()
	var r phase0.Result
	var err error
	switch *mode {
	case "vector":
		r, err = phase0.Vector10M(*rows)
	case "memory":
		r, err = phase0.MemoryAccounting()
	case "pipeline":
		r, err = phase0.PushPipeline(*rows, *workers)
	default:
		log.Fatalf("unknown mode %q", *mode)
	}
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows=%d sum=%d managed_peak=%d managed_end=%d\n", r.Rows, r.Sum, r.ManagedPeak, r.ManagedEnd)
}

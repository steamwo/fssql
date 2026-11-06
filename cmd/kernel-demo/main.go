package main

import (
	"flag"
	"fmt"
	kernel "github.com/steamwo/fssql/internal/demo/kernel"
	"log"
)

func main() {
	mode := flag.String("mode", "vector", "vector|memory|pipeline")
	rows := flag.Int64("rows", 10_000_000, "rows")
	workers := flag.Int("workers", 4, "pipeline workers")
	flag.Parse()
	var r kernel.Result
	var err error
	switch *mode {
	case "vector":
		r, err = kernel.Vector10M(*rows)
	case "memory":
		r, err = kernel.MemoryAccounting()
	case "pipeline":
		r, err = kernel.PushPipeline(*rows, *workers)
	default:
		log.Fatalf("unknown mode %q", *mode)
	}
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows=%d sum=%d managed_peak=%d managed_end=%d\n", r.Rows, r.Sum, r.ManagedPeak, r.ManagedEnd)
}

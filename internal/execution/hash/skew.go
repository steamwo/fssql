package hash

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
)

const partitionStatWidth = 16

type PartitionStat struct {
	Rows  uint64
	Bytes uint64
}

// PartitionHistogram is intentionally worker-local. Its potentially large
// bucket storage is a managed buffer; parallel workers merge histograms at a
// pipeline barrier instead of contending on per-row atomics.
type PartitionHistogram struct {
	buf        *memory.Buffer
	partitions int
	totalRows  uint64
	totalBytes uint64
}

func NewPartitionHistogram(scope *memory.Scope, bm *memory.BufferManager, partitions int) (*PartitionHistogram, error) {
	if scope == nil || bm == nil {
		return nil, errors.New("memory scope and buffer manager are required")
	}
	if partitions <= 0 || partitions > 1<<16 {
		return nil, errors.New("partition count must be 1..65536")
	}
	b, err := bm.Allocate(scope, memory.TagHashTable, partitions*partitionStatWidth)
	if err != nil {
		return nil, err
	}
	return &PartitionHistogram{buf: b, partitions: partitions}, nil
}

func (h *PartitionHistogram) Count() int { return h.partitions }
func (h *PartitionHistogram) Total() PartitionStat {
	return PartitionStat{Rows: h.totalRows, Bytes: h.totalBytes}
}

func (h *PartitionHistogram) statBytes(partition int) ([]byte, error) {
	if partition < 0 || partition >= h.partitions {
		return nil, fmt.Errorf("partition %d out of range", partition)
	}
	off := partition * partitionStatWidth
	return h.buf.Bytes()[off : off+partitionStatWidth], nil
}

func (h *PartitionHistogram) Add(partition int, rowBytes uint64) error {
	s, err := h.statBytes(partition)
	if err != nil {
		return err
	}
	rows := binary.LittleEndian.Uint64(s[:8])
	bytes := binary.LittleEndian.Uint64(s[8:16])
	if rows == ^uint64(0) || ^uint64(0)-bytes < rowBytes || h.totalRows == ^uint64(0) || ^uint64(0)-h.totalBytes < rowBytes {
		return errors.New("partition histogram counter overflow")
	}
	binary.LittleEndian.PutUint64(s[:8], rows+1)
	binary.LittleEndian.PutUint64(s[8:16], bytes+rowBytes)
	h.totalRows++
	h.totalBytes += rowBytes
	return nil
}

func (h *PartitionHistogram) AddHash(hash uint64, rowBytes uint64, partitioner RadixPartitioner, depth int) error {
	partition, err := partitioner.PartitionAtDepth(hash, depth)
	if err != nil {
		return err
	}
	if partition >= h.partitions {
		return errors.New("histogram partition count does not match radix partitioner")
	}
	return h.Add(partition, rowBytes)
}

func (h *PartitionHistogram) Stat(partition int) (PartitionStat, error) {
	s, err := h.statBytes(partition)
	if err != nil {
		return PartitionStat{}, err
	}
	return PartitionStat{
		Rows:  binary.LittleEndian.Uint64(s[:8]),
		Bytes: binary.LittleEndian.Uint64(s[8:16]),
	}, nil
}

func (h *PartitionHistogram) Merge(other *PartitionHistogram) error {
	if other == nil || h.partitions != other.partitions {
		return errors.New("partition histogram shape mismatch")
	}
	if ^uint64(0)-h.totalRows < other.totalRows || ^uint64(0)-h.totalBytes < other.totalBytes {
		return errors.New("partition histogram counter overflow")
	}
	for i := 0; i < h.partitions; i++ {
		a, _ := h.statBytes(i)
		b, _ := other.statBytes(i)
		ar, ab := binary.LittleEndian.Uint64(a[:8]), binary.LittleEndian.Uint64(a[8:16])
		br, bb := binary.LittleEndian.Uint64(b[:8]), binary.LittleEndian.Uint64(b[8:16])
		if ^uint64(0)-ar < br || ^uint64(0)-ab < bb {
			return errors.New("partition histogram counter overflow")
		}
		binary.LittleEndian.PutUint64(a[:8], ar+br)
		binary.LittleEndian.PutUint64(a[8:16], ab+bb)
	}
	h.totalRows += other.totalRows
	h.totalBytes += other.totalBytes
	return nil
}

func (h *PartitionHistogram) Dominant() (partition int, stat PartitionStat) {
	partition = -1
	for i := 0; i < h.partitions; i++ {
		current, _ := h.Stat(i)
		if partition == -1 || current.Bytes > stat.Bytes || current.Bytes == stat.Bytes && current.Rows > stat.Rows {
			partition, stat = i, current
		}
	}
	return partition, stat
}

func (h *PartitionHistogram) Release() {
	if h.buf != nil {
		h.buf.Release()
		h.buf = nil
	}
}

type SkewDetector struct {
	DominantPercent uint8
	MinRows         uint64
	MinBytes        uint64
}

type SkewReport struct {
	Skewed          bool
	Partition       int
	Dominant        PartitionStat
	Total           PartitionStat
	DominantPercent uint8
}

func NewSkewDetector(dominantPercent uint8, minRows, minBytes uint64) (SkewDetector, error) {
	if dominantPercent < 50 || dominantPercent > 100 {
		return SkewDetector{}, errors.New("dominant skew percent must be 50..100")
	}
	return SkewDetector{DominantPercent: dominantPercent, MinRows: minRows, MinBytes: minBytes}, nil
}

func percentAtLeast(part, total uint64, percent uint8) bool {
	if total == 0 {
		return false
	}
	missing := total - part
	allowedMissingPercent := uint64(100 - percent)
	allowedMissing := (total/100)*allowedMissingPercent + ((total%100)*allowedMissingPercent)/100
	return missing <= allowedMissing
}

func (d SkewDetector) Detect(h *PartitionHistogram) (SkewReport, error) {
	if h == nil {
		return SkewReport{}, errors.New("nil partition histogram")
	}
	partition, dominant := h.Dominant()
	total := h.Total()
	report := SkewReport{Partition: partition, Dominant: dominant, Total: total, DominantPercent: d.DominantPercent}
	if total.Rows < d.MinRows || total.Bytes < d.MinBytes {
		return report, nil
	}
	if total.Bytes != 0 {
		report.Skewed = percentAtLeast(dominant.Bytes, total.Bytes, d.DominantPercent)
	} else {
		report.Skewed = percentAtLeast(dominant.Rows, total.Rows, d.DominantPercent)
	}
	return report, nil
}

type RepartitionAction uint8

const (
	PartitionFits RepartitionAction = iota
	PartitionRepartition
	PartitionTerminalSkew
)

type RepartitionReason uint8

const (
	ReasonFits RepartitionReason = iota
	ReasonOversized
	ReasonMaxDepth
	ReasonNoProgress
)

type RepartitionDecision struct {
	Action    RepartitionAction
	Reason    RepartitionReason
	NextDepth int
}

type RepartitionPolicy struct {
	MaxDepth            int
	MinReductionPercent uint8
}

func NewRepartitionPolicy(maxDepth int, minReductionPercent uint8) (RepartitionPolicy, error) {
	if maxDepth < 0 {
		return RepartitionPolicy{}, errors.New("max repartition depth must be non-negative")
	}
	if minReductionPercent == 0 || minReductionPercent > 100 {
		return RepartitionPolicy{}, errors.New("minimum reduction percent must be 1..100")
	}
	return RepartitionPolicy{MaxDepth: maxDepth, MinReductionPercent: minReductionPercent}, nil
}

func percentOf(total uint64, percent uint8) uint64 {
	p := uint64(percent)
	return (total/100)*p + ((total%100)*p)/100
}

// Decide classifies one partition when it is about to be loaded. parentBytes
// is the size before the previous repartition; it is zero for the initial
// partitioning level where progress cannot yet be measured.
func (p RepartitionPolicy) Decide(depth int, parentBytes, partitionBytes, memoryBudget uint64) RepartitionDecision {
	if partitionBytes <= memoryBudget {
		return RepartitionDecision{Action: PartitionFits, Reason: ReasonFits, NextDepth: depth}
	}
	if depth >= p.MaxDepth {
		return RepartitionDecision{Action: PartitionTerminalSkew, Reason: ReasonMaxDepth, NextDepth: depth}
	}
	if parentBytes != 0 && partitionBytes <= parentBytes {
		reduction := parentBytes - partitionBytes
		if reduction < percentOf(parentBytes, p.MinReductionPercent) {
			return RepartitionDecision{Action: PartitionTerminalSkew, Reason: ReasonNoProgress, NextDepth: depth}
		}
	}
	return RepartitionDecision{Action: PartitionRepartition, Reason: ReasonOversized, NextDepth: depth + 1}
}

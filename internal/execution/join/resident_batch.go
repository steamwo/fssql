package join

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/steamwo/fssql/internal/core/vector"
	exechash "github.com/steamwo/fssql/internal/execution/hash"
)

// heads 按候选轮次批量探测，使用与 Insert/Head 一致的槽位混合规则。
func (h *rowHashTable) heads(hashes []uint64, rows []int, heads, pending []int) {
	for i := range rows {
		heads[i] = -1
		pending[i] = i
	}
	n := len(rows)
	for round := 0; n > 0 && round < h.capacity; round++ {
		kept := 0
		for _, candidate := range pending[:n] {
			hash := hashes[rows[candidate]]
			slot := h.slot((rowHashStart(hash, h.capacity) + round) & (h.capacity - 1))
			if slot[0] == 0 {
				continue
			}
			if binary.LittleEndian.Uint64(slot[8:16]) == hash {
				heads[candidate] = int(binary.LittleEndian.Uint32(slot[16:20])) - 1
			} else {
				pending[kept] = candidate
				kept++
			}
		}
		n = kept
	}
}

type residentProbeScratch struct {
	heads, activeRows, activeBuild, selection [vector.DefaultVectorSize]int
	buildRows                                 [vector.DefaultVectorSize][]byte
	matched                                   [vector.DefaultVectorSize]bool
}

// probeBatch 按分区批量探测，取消检查以 256 行为界；分区存储在 Finalize 前保持不可变。
func (j *ExternalHashJoin) probeBatch(ctx context.Context, chunk *vector.DataChunk, hashes []uint64, comparer *exechash.ChunkKeyComparer, emitter *batchEmitter, stable *StableExternalHashJoin) error {
	var counts, starts [256]int
	var partitionRows, partitions [vector.DefaultVectorSize]int
	for row, hash := range hashes {
		if row&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		part, err := j.partitionFor(hash, 0)
		if err != nil {
			return err
		}
		partitions[row] = part
		if j.partitions[part].spilled {
			if err := j.appendProbeSpill(ctx, &j.partitions[part], hash, chunk, row); err != nil {
				return err
			}
			continue
		}
		counts[part]++
	}
	position := 0
	for part := range j.partitions {
		starts[part] = position
		position += counts[part]
		counts[part] = 0
	}
	for row, part := range partitions[:len(hashes)] {
		if j.partitions[part].spilled {
			continue
		}
		partitionRows[starts[part]+counts[part]] = row
		counts[part]++
	}
	var scratch residentProbeScratch
	for part, count := range counts[:len(j.partitions)] {
		if count == 0 {
			continue
		}
		rows := partitionRows[starts[part] : starts[part]+count]
		if err := j.probeResidentBatch(ctx, chunk, hashes, comparer, emitter, part, rows, stable, &scratch); err != nil {
			return err
		}
		if emitter.stopped {
			return nil
		}
	}
	return nil
}

func (j *ExternalHashJoin) probeResidentBatch(ctx context.Context, chunk *vector.DataChunk, hashes []uint64, comparer *exechash.ChunkKeyComparer, emitter *batchEmitter, part int, rows []int, stable *StableExternalHashJoin, scratch *residentProbeScratch) error {
	partition := &j.partitions[part]
	heads, activeRows, activeBuild, selection := scratch.heads[:], scratch.activeRows[:], scratch.activeBuild[:], scratch.selection[:]
	buildRows, matched := scratch.buildRows[:], scratch.matched[:]
	n := 0
	if partition.index != nil && partition.resident != nil {
		partition.index.heads(hashes, rows, heads[:len(rows)], selection[:len(rows)])
		for i, head := range heads[:len(rows)] {
			if head >= 0 {
				activeRows[n] = rows[i]
				activeBuild[n] = head
				n++
			}
		}
	}
	reader := partition.resident.borrowReader()
	for n > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		for i, build := range activeBuild[:n] {
			_, row, valid := reader.record(build)
			if !valid {
				return errors.New("invalid resident build record")
			}
			buildRows[i] = row
		}
		matches, err := comparer.SelectEqual(activeRows[:n], j.buildLayout, buildRows[:n], j.buildKeys, j.nullEquality(), selection[:n])
		if err != nil {
			return err
		}
		if j.config.Type == Full && stable != nil && matches > 0 {
			// One bitmap borrow and critical section per collision-chain round.
			stable.matchedMu.Lock()
			bitmap := stable.residentMatched[part].Bytes()
			for _, candidate := range selection[:matches] {
				build := activeBuild[candidate]
				bitmap[build>>3] |= byte(1 << uint(build&7))
			}
			stable.matchedMu.Unlock()
		}
		for _, candidate := range selection[:matches] {
			row := activeRows[candidate]
			matched[row] = true
			switch j.config.Type {
			case Anti:
			case Semi:
				if err := emitter.appendProbeOnly(ctx, chunk, row); err != nil {
					return err
				}
			default:
				if err := emitter.appendPair(ctx, chunk, row, buildRows[candidate]); err != nil {
					return err
				}
			}
		}
		if emitter.stopped {
			return nil
		}
		kept := 0
		for i, row := range activeRows[:n] {
			if (j.config.Type == Semi || j.config.Type == Anti) && matched[row] {
				continue
			}
			if next, ok := partition.index.Next(activeBuild[i]); ok {
				activeRows[kept], activeBuild[kept] = row, next
				kept++
			}
		}
		n = kept
	}
	if stable != nil && stable.probeIsPreservedOrAnti() {
		for _, row := range rows {
			if !matched[row] {
				if err := emitter.appendProbeOnly(ctx, chunk, row); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

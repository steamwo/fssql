package join

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	exechash "github.com/steamwo/fssql/internal/execution/hash"
)

// maxDenseJoinEntries 限制可选直接索引的范围，避免稀疏键占用大量内存。
const maxDenseJoinEntries = 1 << 20

// denseIntegerIndex 为单个唯一整数键保存直接行引用；所有借用在 build 完成后保持只读。
type denseIntegerIndex struct {
	buffer     *memory.Buffer // buffer 将索引计入 JOIN 的 managed memory。
	slots      []byte         // slots 每八字节保存行偏移及八位分区号，零表示缺失。
	partitions [][]byte       // partitions 借用冻结后的 resident record 数据。
	minimum    uint64         // minimum 是按数值顺序归一化的最小键。
	signMask   uint64         // signMask 翻转有符号键的符号位，无符号键使用零。
	width      int            // width 是整数键的物理字节数。
	nullRef    uint64         // nullRef 仅在 NULL 等值语义下保存唯一 NULL 行。
}

// denseIntegerKey 将本机小端整数归一化，使无符号比较保持原类型的数值顺序。
func denseIntegerKey(data []byte, width int, signMask uint64) uint64 {
	var value uint64
	switch width {
	case 1:
		value = uint64(data[0])
	case 2:
		value = uint64(binary.LittleEndian.Uint16(data))
	case 4:
		value = uint64(binary.LittleEndian.Uint32(data))
	case 8:
		value = binary.LittleEndian.Uint64(data)
	}
	return value ^ signMask
}

// tryBuildDenseIntegerIndex 根据实际构建数据证明范围和唯一性；不满足条件或配额不足则保留哈希路径。
func (j *ExternalHashJoin) tryBuildDenseIntegerIndex(ctx context.Context) error {
	if j.config.Type != Inner || len(j.buildKeys) != 1 {
		return nil
	}
	column := j.buildKeys[0]
	typ := j.buildTypes[column]
	switch typ.ID {
	case types.TinyInt, types.SmallInt, types.Integer, types.BigInt:
	default:
		return nil
	}
	width, _ := typ.FixedSize()
	signMask := uint64(0)
	if !typ.Unsigned {
		signMask = uint64(1) << uint(width*8-1)
	}
	minimum, maximum, count := ^uint64(0), uint64(0), uint64(0)
	for part := range j.partitions {
		p := &j.partitions[part]
		if p.spilled {
			return nil
		}
		reader := p.resident.borrowReader()
		for row := 0; row < reader.count; row++ {
			if row&255 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			_, encoded, ok := reader.record(row)
			if !ok {
				return errors.New("invalid resident build record")
			}
			data, valid, err := j.buildLayout.ColumnData(encoded, column)
			if err != nil {
				return err
			}
			if !valid {
				continue
			}
			key := denseIntegerKey(data, width, signMask)
			minimum = min(minimum, key)
			maximum = max(maximum, key)
			count++
		}
	}
	if count == 0 {
		return nil
	}
	// 先比较差值再加一，覆盖 BIGINT/UBIGINT 两端之间的溢出边界。
	spread := maximum - minimum
	if spread >= maxDenseJoinEntries || spread >= count*2 || count > spread+1 {
		return nil
	}
	bytes := int64((spread + 1) * 8)
	if bytes > j.config.MemoryBudget/16 || bytes > j.config.MemoryBudget-j.residentBytes() {
		return nil
	}
	buffer, err := j.config.Buffers.Allocate(j.config.Scope, memory.TagHashTable, int(bytes))
	if errors.Is(err, memory.ErrLimitExceeded) {
		return nil
	}
	if err != nil {
		return err
	}
	index := &denseIntegerIndex{buffer: buffer, slots: buffer.Bytes(), partitions: make([][]byte, len(j.partitions)), minimum: minimum, signMask: signMask, width: width}
	clear(index.slots)
	defer func() {
		if j.dense != index {
			index.release()
		}
	}()
	for part := range j.partitions {
		reader := j.partitions[part].resident.borrowReader()
		index.partitions[part] = reader.data
		for row := 0; row < reader.count; row++ {
			if row&255 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			_, encoded, _ := reader.record(row)
			// 行数据从十二字节头部之后开始，引用天然非零，且最多使用四十位。
			start := binary.LittleEndian.Uint32(reader.offsets[row*4:]) + 12
			ref := uint64(start)<<8 | uint64(part)
			data, valid, err := j.buildLayout.ColumnData(encoded, column)
			if err != nil {
				return err
			}
			if !valid {
				if j.config.NullsEqual {
					if index.nullRef != 0 {
						return nil
					}
					index.nullRef = ref
				}
				continue
			}
			offset := int(denseIntegerKey(data, width, signMask)-minimum) * 8
			slot := index.slots[offset : offset+8]
			if binary.LittleEndian.Uint64(slot) != 0 {
				return nil
			}
			binary.LittleEndian.PutUint64(slot, ref)
		}
	}
	j.dense = index
	return ctx.Err()
}

// release 仅释放直接索引自身，借用的行数据由 resident partitions 统一回收。
func (d *denseIntegerIndex) release() {
	d.buffer.Release()
	d.slots, d.partitions = nil, nil
}

// probeChunk 选择已证明安全的直接索引，其他场景保持通用哈希和碰撞比较语义。
func (j *ExternalHashJoin) probeChunk(ctx context.Context, chunk *vector.DataChunk, emitter *batchEmitter, stable *StableExternalHashJoin) error {
	if chunk.Cardinality < 0 || chunk.Cardinality > vector.DefaultVectorSize {
		return errors.New("invalid join probe cardinality")
	}
	if j.dense != nil {
		return j.probeDenseIntegers(ctx, chunk, emitter)
	}
	var hashes [vector.DefaultVectorSize]uint64
	if err := exechash.HashChunk(chunk, j.probeKeys, hashes[:chunk.Cardinality]); err != nil {
		return err
	}
	comparer, err := exechash.NewChunkKeyComparer(chunk, j.probeKeys)
	if err != nil {
		return err
	}
	return j.probeBatch(ctx, chunk, hashes[:chunk.Cardinality], comparer, emitter, stable)
}

// probeDenseIntegers 一次收集整批匹配行；唯一构建键保证每个 probe 行最多产生一行。
func (j *ExternalHashJoin) probeDenseIntegers(ctx context.Context, chunk *vector.DataChunk, emitter *batchEmitter) error {
	d := j.dense
	key := chunk.Columns[j.probeKeys[0]]
	if !key.Type.Same(j.buildTypes[j.buildKeys[0]]) || key.Count() < chunk.Cardinality {
		return errors.New("dense join probe key shape mismatch")
	}
	view, ok := key.BorrowFixedWidth()
	if !ok {
		return errors.New("invalid dense join probe key")
	}
	flat, validity, isFlat := view.FlatData()
	allValid := isFlat && validity.AllValid(chunk.Cardinality)
	emitter.probeChunk = chunk
	for row := 0; row < chunk.Cardinality; row++ {
		if row&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		var data []byte
		valid := true
		if isFlat {
			valid = allValid || validity.IsValid(row)
			data = flat[row*d.width : (row+1)*d.width]
		} else {
			data, valid = view.Value(row)
		}
		ref := d.nullRef
		if valid {
			offset := denseIntegerKey(data, d.width, d.signMask) - d.minimum
			if offset >= uint64(len(d.slots)/8) {
				continue
			}
			ref = binary.LittleEndian.Uint64(d.slots[int(offset)*8:])
		}
		if ref == 0 {
			continue
		}
		data = d.partitions[ref&255]
		start := int(ref >> 8)
		length := int(binary.LittleEndian.Uint32(data[start-4 : start]))
		emitter.probeRows[emitter.count] = row
		emitter.buildRows[emitter.count] = data[start : start+length]
		emitter.count++
	}
	return ctx.Err()
}

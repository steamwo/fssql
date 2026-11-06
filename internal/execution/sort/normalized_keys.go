package sort

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
)

// normalizedKeyWidth 返回可无损编码为字节序排序键的宽度；浮点与变长键保留原比较路径。
func normalizedKeyWidth(keys []preparedSortKey) int {
	width := 0
	for _, key := range keys {
		switch key.column.Type.ID {
		case types.Boolean, types.TinyInt, types.SmallInt, types.Integer, types.BigInt, types.HugeInt, types.Decimal, types.DateType, types.TimestampType:
		default:
			return 0
		}
		if key.column.Variable || key.column.Width < 1 || key.column.Width > 16 || width > math.MaxInt-1-key.column.Width {
			return 0
		}
		width += 1 + key.column.Width
	}
	return width
}

// normalizedSortBytes 为整数排序键及双索引预留预算，避免排序时才发现没有工作内存。
func (s *ExternalSort) normalizedSortBytes() int64 {
	width := normalizedKeyWidth(s.preparedKeys)
	if width == 0 {
		return 0
	}
	return int64(s.builder.Count()) * int64(width+8)
}

// sortNormalizedIndices 借鉴排序键归一化：每行只解码一次，后续使用受管索引的稳定基数排序。
// 所有键与 scratch 均计入原排序预算；不适用或预算不足时返回原比较排序路径。
func (s *ExternalSort) sortNormalizedIndices(idx *managedIndices, row func(int) []byte) (bool, error) {
	width := normalizedKeyWidth(s.preparedKeys)
	if width == 0 || idx.n > math.MaxInt/width {
		return false, nil
	}
	needed := int64(idx.n) * int64(width+8)
	if needed > s.cfg.MemoryBudget-s.builderBytes() {
		return false, nil
	}
	keys, err := s.cfg.Buffers.Allocate(s.cfg.Scope, memory.TagSort, idx.n*width)
	if errors.Is(err, memory.ErrLimitExceeded) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	defer keys.Release()
	scratch, err := s.cfg.Buffers.Allocate(s.cfg.Scope, memory.TagSort, idx.n*4)
	if errors.Is(err, memory.ErrLimitExceeded) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	defer scratch.Release()
	data := keys.Bytes()
	for i := 0; i < idx.n; i++ {
		encoded := row(i)
		entry := data[i*width : (i+1)*width]
		pos := 0
		for _, key := range s.preparedKeys {
			isNull := encoded[key.key.Column>>3]&(1<<uint(key.key.Column&7)) != 0
			if isNull != key.key.NullsFirst {
				entry[pos] = 1
			}
			pos++
			value := entry[pos : pos+key.column.Width]
			pos += len(value)
			if isNull {
				continue
			}
			raw, err := preparedColumnData(key.column, key.fixedWidth, encoded)
			if err != nil {
				return true, err
			}
			for j := range value {
				value[j] = raw[len(raw)-1-j]
			}
			if !key.column.Type.Unsigned && key.column.Type.ID != types.Boolean {
				value[0] ^= 0x80
			}
			if key.key.Desc {
				for j := range value {
					value[j] = ^value[j]
				}
			}
		}
	}
	src, dst := idx.data, scratch.Bytes()
	for offset := width - 1; offset >= 0; offset-- {
		var counts [256]int
		for i := 0; i < idx.n; i++ {
			value := int(binary.LittleEndian.Uint32(src[i*4:]))
			counts[data[value*width+offset]]++
		}
		uniform := false
		position := 0
		for i, count := range counts {
			if count == idx.n {
				uniform = true
				break
			}
			counts[i], position = position, position+count
		}
		if uniform {
			continue
		}
		for i := 0; i < idx.n; i++ {
			value := binary.LittleEndian.Uint32(src[i*4:])
			bucket := data[int(value)*width+offset]
			binary.LittleEndian.PutUint32(dst[counts[bucket]*4:], value)
			counts[bucket]++
		}
		src, dst = dst, src
	}
	copy(idx.data, src)
	return true, nil
}

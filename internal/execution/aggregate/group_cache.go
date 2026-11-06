package aggregate

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

const integerGroupCacheSlots = 4096 // integerGroupCacheSlots 限制单整数键的直接映射缓存规模。

// integerGroupCache 为已建立的小整数分组提供精确键缓存；碰撞回退到正常哈希表。
func (a *HashAggregate) integerGroupCache(projected *vector.DataChunk) ([]byte, vector.FixedWidthView, int, error) {
	if len(a.groupTypes) != 1 || a.groups.Count() < 128 {
		return nil, vector.FixedWidthView{}, 0, nil
	}
	typ := a.groupTypes[0]
	switch typ.ID {
	case types.Boolean, types.TinyInt, types.SmallInt, types.Integer, types.BigInt, types.HugeInt, types.Decimal, types.DateType, types.TimestampType:
	default:
		return nil, vector.FixedWidthView{}, 0, nil
	}
	view, ok := projected.Columns[0].BorrowFixedWidth()
	if !ok {
		return nil, vector.FixedWidthView{}, 0, nil
	}
	width, _ := typ.FixedSize()
	if int64(integerGroupCacheSlots*(5+width)) > a.groupCacheBudget {
		return nil, vector.FixedWidthView{}, 0, nil
	}
	if a.groupCache == nil {
		buffer, err := a.bm.Allocate(a.scope, memory.TagAggregate, integerGroupCacheSlots*(5+width))
		if errors.Is(err, memory.ErrLimitExceeded) {
			return nil, vector.FixedWidthView{}, 0, nil
		}
		if err != nil {
			return nil, vector.FixedWidthView{}, 0, err
		}
		a.groupCache = buffer
	}
	return a.groupCache.Bytes(), view, width, nil
}

// cachedIntegerGroup 验证完整键及 NULL 状态，缓存不以哈希相等代替 SQL 相等。
func cachedIntegerGroup(slot, key []byte, valid bool) (int, bool) {
	id := binary.LittleEndian.Uint32(slot)
	if id == 0 || (slot[4] != 0) != valid || valid && !bytes.Equal(slot[5:], key) {
		return 0, false
	}
	return int(id - 1), true
}

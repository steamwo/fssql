package sort

import (
	"encoding/binary"
	"errors"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
)

const stringPrefixWidth = 9 // NULL rank + eight leading bytes, all pointer-free.

func (s *ExternalSort) newStringPrefixes(row func(int) []byte, n int) (*memory.Buffer, error) {
	if len(s.preparedKeys) == 0 || s.preparedKeys[0].column.Type.ID != types.Varchar || n > int(^uint(0)>>1)/stringPrefixWidth {
		return nil, nil
	}
	needed := int64(n) * stringPrefixWidth
	// Index state and builder payload count against the existing sort budget.
	if needed > s.cfg.MemoryBudget-s.builderBytes()-int64(n)*4 {
		return nil, nil
	}
	buffer, err := s.cfg.Buffers.Allocate(s.cfg.Scope, memory.TagSort, int(needed))
	if errors.Is(err, memory.ErrLimitExceeded) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	data := buffer.Bytes()
	key := s.preparedKeys[0]
	for i := 0; i < n; i++ {
		encoded := row(i)
		entry := data[i*stringPrefixWidth : (i+1)*stringPrefixWidth]
		clear(entry)
		isNull := encoded[key.key.Column>>3]&(1<<uint(key.key.Column&7)) != 0
		if isNull != key.key.NullsFirst {
			entry[0] = 1
		}
		if isNull {
			continue
		}
		value, err := preparedColumnData(key.column, key.fixedWidth, encoded)
		if err != nil {
			buffer.Release()
			return nil, err
		}
		copy(entry[1:], value)
	}
	return buffer, nil
}

func (s *ExternalSort) prefixComparator(data []byte, row func(int) []byte) func(int, int) (int, error) {
	key := s.preparedKeys[0]
	return func(a, b int) (int, error) {
		x, y := data[a*stringPrefixWidth:(a+1)*stringPrefixWidth], data[b*stringPrefixWidth:(b+1)*stringPrefixWidth]
		if x[0] < y[0] {
			return -1, nil
		}
		if x[0] > y[0] {
			return 1, nil
		}
		xp, yp := binary.BigEndian.Uint64(x[1:]), binary.BigEndian.Uint64(y[1:])
		if xp != yp {
			cmp := -1
			if xp > yp {
				cmp = 1
			}
			if key.key.Desc {
				cmp = -cmp
			}
			return cmp, nil
		}
		// Equal prefixes include short strings, embedded NULs and arbitrarily
		// long common prefixes. Always resolve them with complete SQL keys.
		return s.compareRows(row(a), row(b))
	}
}

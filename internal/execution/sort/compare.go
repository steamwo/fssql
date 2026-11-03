package sort

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/steamwo/fssql/internal/core/types"
	exechash "github.com/steamwo/fssql/internal/execution/hash"
)

// Resolve the row-layout metadata once per sort instead of twice per key on
// every comparison. Variable descriptors remain validated for spill input.
type preparedSortKey struct {
	key        Key
	column     exechash.ColumnLayout
	fixedWidth int
}

func comparePreparedRows(keys []preparedSortKey, a, b []byte) (int, error) {
	for _, k := range keys {
		nullByte, mask := k.key.Column>>3, byte(1<<uint(k.key.Column&7))
		if nullByte >= len(a) || nullByte >= len(b) {
			return 0, errors.New("short sort row")
		}
		an, bn := a[nullByte]&mask != 0, b[nullByte]&mask != 0
		if an || bn {
			if an && bn {
				continue
			}
			if an == k.key.NullsFirst {
				return -1, nil
			}
			return 1, nil
		}
		ad, err := preparedColumnData(k.column, k.fixedWidth, a)
		if err != nil {
			return 0, err
		}
		bd, err := preparedColumnData(k.column, k.fixedWidth, b)
		if err != nil {
			return 0, err
		}
		var cmp int
		if k.column.Type.ID == types.Varchar {
			cmp = bytes.Compare(ad, bd)
		} else {
			cmp, err = compareEncoded(k.column.Type, ad, bd)
			if err != nil {
				return 0, err
			}
		}
		if cmp != 0 {
			if k.key.Desc {
				cmp = -cmp
			}
			return cmp, nil
		}
	}
	return 0, nil
}

func preparedColumnData(column exechash.ColumnLayout, fixedWidth int, row []byte) ([]byte, error) {
	if column.Offset < 0 || column.Offset > len(row) || column.Width > len(row)-column.Offset {
		return nil, errors.New("short sort row")
	}
	data := row[column.Offset : column.Offset+column.Width]
	if !column.Variable {
		return data, nil
	}
	offset, n := int(binary.LittleEndian.Uint32(data[:4])), int(binary.LittleEndian.Uint32(data[4:8]))
	if offset < fixedWidth || offset > len(row) || n < 0 || n > len(row)-offset {
		return nil, errors.New("invalid sort string descriptor")
	}
	return row[offset : offset+n], nil
}

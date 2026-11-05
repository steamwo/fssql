package parquet

import (
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
)

var errPageIndex = errors.New("parquet: invalid page index")

func indexI64(r *compactReader) (int64, error) {
	start := r.off
	value, err := r.uvarint()
	if err != nil {
		return 0, err
	}
	if r.off-start == 10 && r.b[r.off-1] > 1 {
		return 0, errPageIndex
	}
	return zz(value), nil
}

func indexI32(r *compactReader) (int32, error) {
	value, err := indexI64(r)
	if err != nil {
		return 0, err
	}
	if value < -(1<<31) || value > (1<<31)-1 {
		return 0, errPageIndex
	}
	return int32(value), nil
}

func decodeOffsetIndex(data []byte, c *ColumnCursor) ([]indexedPage, *memory.Reservation, error) {
	r := newCompactReader(data)
	var pages []indexedPage
	var reservation *memory.Reservation
	last := int16(0)
	for {
		field, next, err := r.field(last)
		last = next
		if err != nil {
			return nil, reservation, err
		}
		if field.typ == ctStop {
			break
		}
		if field.id != 1 {
			if err := r.skip(field.typ, 1); err != nil {
				return nil, reservation, err
			}
			continue
		}
		if field.typ != ctList || pages != nil {
			return nil, reservation, errPageIndex
		}
		typ, count, err := r.listHeader()
		if err != nil {
			return nil, reservation, err
		}
		if typ != ctStruct || count < 1 || count > maxIndexedPages || count > r.remaining()/6 {
			return nil, reservation, errPageIndex
		}
		// Covers the decoded structs/slice bookkeeping in addition to raw buffers.
		reservation, err = c.scope.Reserve(memory.TagMetadata, int64(count)*128)
		if err != nil {
			return nil, nil, err
		}
		pages = make([]indexedPage, count)
		for i := range pages {
			page, err := decodePageLocation(r)
			if err != nil {
				return nil, reservation, err
			}
			if page.firstRow < 0 || page.firstRow >= c.meta.NumValues || page.size <= 0 || page.offset < c.meta.DataPageOffset || page.offset > c.end || int64(page.size) > c.end-page.offset {
				return nil, reservation, errPageIndex
			}
			if i == 0 {
				if page.firstRow != 0 || page.offset != c.meta.DataPageOffset {
					return nil, reservation, errPageIndex
				}
			} else {
				previous := pages[i-1]
				if page.firstRow <= previous.firstRow || page.offset < previous.offset+int64(previous.size) {
					return nil, reservation, errPageIndex
				}
			}
			pages[i] = page
		}
	}
	if pages == nil || r.remaining() != 0 {
		return nil, reservation, errPageIndex
	}
	return pages, reservation, nil
}

func decodePageLocation(r *compactReader) (indexedPage, error) {
	var page indexedPage
	seen := uint8(0)
	last := int16(0)
	for {
		field, next, err := r.field(last)
		last = next
		if err != nil {
			return page, err
		}
		if field.typ == ctStop {
			break
		}
		if field.id < 1 || field.id > 3 {
			if err := r.skip(field.typ, 1); err != nil {
				return page, err
			}
			continue
		}
		bit := uint8(1) << uint(field.id-1)
		if seen&bit != 0 {
			return page, errPageIndex
		}
		seen |= bit
		if field.id == 2 {
			if field.typ != ctI32 {
				return page, errPageIndex
			}
			page.size, err = indexI32(r)
		} else {
			if field.typ != ctI64 {
				return page, errPageIndex
			}
			value, e := indexI64(r)
			err = e
			if field.id == 1 {
				page.offset = value
			} else {
				page.firstRow = value
			}
		}
		if err != nil {
			return page, err
		}
	}
	if seen != 7 {
		return page, errPageIndex
	}
	return page, nil
}

func decodeColumnIndex(data []byte, pages []indexedPage, rows int64) error {
	r := newCompactReader(data)
	last := int16(0)
	seen := uint8(0)
	for {
		field, next, err := r.field(last)
		last = next
		if err != nil {
			return err
		}
		if field.typ == ctStop {
			break
		}
		if field.id < 1 || field.id > 5 {
			if err := r.skip(field.typ, 1); err != nil {
				return err
			}
			continue
		}
		bit := uint8(1) << uint(field.id-1)
		if seen&bit != 0 {
			return errPageIndex
		}
		seen |= bit
		if field.id == 4 {
			if field.typ != ctI32 {
				return errPageIndex
			}
			order, err := indexI32(r)
			if err != nil {
				return err
			}
			if order < 0 || order > 2 {
				return errPageIndex
			}
			continue
		}
		if field.typ != ctList {
			return errPageIndex
		}
		typ, count, err := r.listHeader()
		if err != nil {
			return err
		}
		if count != len(pages) {
			return errPageIndex
		}
		for i := range pages {
			switch field.id {
			case 1:
				if typ != ctBoolTrue && typ != ctBoolFalse {
					return errPageIndex
				}
				value, err := r.byte()
				if err != nil {
					return err
				}
				if value != ctBoolTrue && value != ctBoolFalse {
					return errPageIndex
				}
				pages[i].nullPage = value == ctBoolTrue
			case 2, 3:
				if typ != ctBinary {
					return errPageIndex
				}
				value, err := r.binary()
				if err != nil {
					return err
				}
				if field.id == 2 {
					pages[i].min = value
				} else {
					pages[i].max = value
				}
			case 5:
				if typ != ctI64 {
					return errPageIndex
				}
				nulls, err := indexI64(r)
				if err != nil {
					return err
				}
				pages[i].nulls, pages[i].hasNulls = nulls, true
			}
		}
	}
	if seen&15 != 15 || r.remaining() != 0 {
		return errPageIndex
	}
	for i, page := range pages {
		end := rows
		if i+1 < len(pages) {
			end = pages[i+1].firstRow
		}
		count := end - page.firstRow
		if page.hasNulls && (page.nulls < 0 || page.nulls > count || page.nullPage && page.nulls != count || !page.nullPage && page.nulls == count) {
			return errPageIndex
		}
		if page.nullPage && (len(page.min) != 0 || len(page.max) != 0) {
			return fmt.Errorf("%w: null page has bounds", errPageIndex)
		}
	}
	return nil
}

package hash

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/semantics"
)

// MaterializeSelectedRow is the spill/page counterpart of the batch gather.
// Nil rows produce SQL NULLs; unneeded output positions retain placeholders.
func (l RowLayout) MaterializeSelectedRow(row []byte, dst []*vector.Vector, dstRow int, needed []bool) error {
	if !l.relocatable || len(dst) != len(l.Columns) || (needed != nil && len(needed) != len(dst)) {
		return errors.New("invalid materialize shape")
	}
	for i, col := range l.Columns {
		if needed != nil && !needed[i] {
			continue
		}
		if row == nil {
			dst[i].SetNull(dstRow)
			continue
		}
		data, valid, err := l.ColumnData(row, i)
		if err != nil {
			return err
		}
		if !valid {
			dst[i].SetNull(dstRow)
			continue
		}
		if err := setEncodedValue(dst[i], dstRow, col, data); err != nil {
			return err
		}
	}
	return nil
}

// SelectEqual 逐列压缩匹配候选；固定宽度键只在批次入口验证 schema 和列偏移。
func (c *ChunkKeyComparer) SelectEqual(probeRows []int, layout RowLayout, rows [][]byte, columns []int, nulls NullEquality, selection []int) (int, error) {
	if !layout.relocatable || len(columns) != len(c.columns) || len(rows) != len(probeRows) || len(selection) < len(rows) {
		return 0, errors.New("invalid batch key comparison shape")
	}
	for i, row := range probeRows {
		if row < 0 || row >= c.chunk.Cardinality {
			return 0, errors.New("probe row out of range")
		}
		selection[i] = i
	}
	n := len(rows)
	for key, column := range columns {
		if column < 0 || column >= len(layout.Columns) || !c.chunk.Columns[c.columns[key]].Type.Same(layout.Columns[column].Type) {
			return 0, errors.New("projected row key type mismatch")
		}
		col := layout.Columns[column]
		if col.Offset < 0 || col.Width < 0 || col.Offset > layout.FixedWidth || col.Width > layout.FixedWidth-col.Offset {
			return 0, errors.New("invalid encoded key column")
		}
		// Select the semantic kernel once per column, including canonical NaNs.
		equal := bytes.Equal
		switch layout.Columns[column].Type.ID {
		case types.Float:
			equal = func(a, b []byte) bool {
				return semantics.EqualFloat32(math.Float32frombits(binary.LittleEndian.Uint32(a)), math.Float32frombits(binary.LittleEndian.Uint32(b)))
			}
		case types.Double:
			equal = func(a, b []byte) bool {
				return semantics.EqualFloat64(math.Float64frombits(binary.LittleEndian.Uint64(a)), math.Float64frombits(binary.LittleEndian.Uint64(b)))
			}
		}
		kept := 0
		for _, candidate := range selection[:n] {
			var data []byte
			var valid bool
			if c.viewOK[key] {
				data, valid = c.views[key].Value(probeRows[candidate])
			} else {
				data, valid = c.byteViews[key].Value(probeRows[candidate])
			}
			var encoded []byte
			var encodedValid bool
			if !col.Variable {
				row := rows[candidate]
				if len(row) < layout.FixedWidth {
					return 0, errors.New("short encoded key row")
				}
				encodedValid = row[column>>3]&(1<<uint(column&7)) == 0
				encoded = row[col.Offset : col.Offset+col.Width]
			} else {
				var err error
				encoded, encodedValid, err = layout.ColumnData(rows[candidate], column)
				if err != nil {
					return 0, err
				}
			}
			if !valid || !encodedValid {
				if valid || encodedValid || nulls != NullsEqual {
					continue
				}
			} else if !equal(data, encoded) {
				continue
			}
			selection[kept] = candidate
			kept++
		}
		n = kept
		if n == 0 {
			break
		}
	}
	return n, nil
}

// MaterializeSelectedRows 保持 schema 位置并跳过未读取的列；定宽列仅在批次入口验证偏移。
// nil mask 表示读取全部列，false 位置可使用 nil 目标向量。
func (l RowLayout) MaterializeSelectedRows(rows [][]byte, dst []*vector.Vector, needed []bool) error {
	if !l.relocatable || l.NullBytes < (len(l.Columns)+7)/8 || l.FixedWidth < l.NullBytes || len(dst) != len(l.Columns) || (needed != nil && len(needed) != len(dst)) {
		return errors.New("invalid materialize shape")
	}
	for _, row := range rows {
		if row != nil && len(row) < l.FixedWidth {
			return errors.New("short encoded materialize row")
		}
	}
	for column, col := range l.Columns {
		if needed != nil && !needed[column] {
			continue
		}
		out := dst[column]
		if out == nil || out.Count() < len(rows) || !out.Type.Same(col.Type) {
			return errors.New("invalid materialize destination")
		}
		if data, validity, ok := out.MutableFixedWidth(); ok {
			width, _ := out.Type.FixedSize()
			if col.Variable || col.Width != width || col.Offset < 0 || col.Offset > l.FixedWidth || width > l.FixedWidth-col.Offset {
				return errors.New("invalid encoded materialize column")
			}
			for i, row := range rows {
				if row == nil || row[column>>3]&(1<<uint(column&7)) != 0 {
					validity.SetValid(i, false)
					continue
				}
				validity.SetValid(i, true)
				copy(data[i*width:(i+1)*width], row[col.Offset:col.Offset+width])
			}
			continue
		}
		if col.Type.ID == types.Varchar {
			writer, err := out.NewStringBatchWriter()
			if err != nil {
				return err
			}
			err = func() error {
				defer writer.Commit()
				for i, row := range rows {
					if row == nil {
						out.SetNull(i)
						continue
					}
					value, valid, err := l.ColumnData(row, column)
					if err != nil {
						return err
					}
					if !valid {
						out.SetNull(i)
						continue
					}
					if err := writer.Append(i, value); err != nil {
						return err
					}
				}
				return nil
			}()
			if err != nil {
				return err
			}
			continue
		}
		return errors.New("materialize unsupported destination")
	}
	return nil
}

package csv

import (
	"bytes"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/semantics"
)

func decodeFormattedField(dst *vector.Vector, typ types.LogicalType, row, column int, raw, nullLiteral []byte, opts DecodeOptions) error {
	if nullLiteral != nil && bytes.Equal(raw, nullLiteral) {
		dst.SetNull(row)
		return nil
	}
	format := ""
	if column < len(opts.Formats) {
		format = opts.Formats[column]
	}
	if format == "" {
		if typ.ID == types.DateType {
			format = opts.DateFormat
		} else if typ.ID == types.TimestampType {
			format = opts.TimestampFormat
		}
	}
	if format == "" || (typ.ID != types.DateType && typ.ID != types.TimestampType) {
		return decodeField(dst, typ, row, raw, nullLiteral)
	}
	tm, err := semantics.ParseTimeFormat(string(raw), format)
	if err != nil {
		return err
	}
	if typ.ID == types.DateType {
		raw, err := semantics.ParseDateLiteral(semantics.FormatCalendarDate(tm))
		if err != nil {
			return err
		}
		dst.SetInt32(row, int32(raw))
		return nil
	}
	text := semantics.FormatCalendarDate(tm) + tm.Format(" 15:04:05.999999999")
	if typ.Timestamp.AdjustedToUTC {
		text = tm.Format("2006-01-02T15:04:05.999999999Z07:00")
	}
	value, err := semantics.ParseTimestampText(text, typ.Timestamp.Unit, typ.Timestamp.AdjustedToUTC)
	if err != nil {
		return err
	}
	dst.SetInt64(row, value)
	return nil
}

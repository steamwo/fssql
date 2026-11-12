package expression

import (
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/semantics"
	"time"
)

func castScalarInZone(value ScalarValue, target types.LogicalType, zone string) (ScalarValue, error) {
	if zone == "" {
		zone = "UTC"
	}
	value.TimeZoneName = zone
	if !value.Valid {
		return ScalarValue{Type: target, Valid: false}, nil
	}
	if value.Type.ID == types.Varchar && !value.Type.Binary && target.ID == types.TimestampType {
		raw, err := semantics.ParseTimestampTextInZone(string(value.Bytes), target.Timestamp.Unit, target.Timestamp.AdjustedToUTC, zone)
		return ScalarValue{Type: target, Valid: true, Int64: raw, TimeZoneName: zone}, err
	}
	if value.Type.ID == types.TimestampType && semantics.TimestampInfinity(value.Int64) == 0 {
		if target.ID == types.DateType && value.Type.Timestamp.AdjustedToUTC {
			tm := temporalTime(value)
			days, err := semantics.ParseDateLiteral(semantics.FormatCalendarDate(tm))
			return ScalarValue{Type: target, Valid: true, Int64: days}, err
		}
		if target.ID == types.TimestampType && value.Type.Timestamp.AdjustedToUTC != target.Timestamp.AdjustedToUTC {
			tm := temporalTime(value)
			text := semantics.FormatCalendarDate(tm) + tm.Format(" 15:04:05.999999999")
			raw, err := semantics.ParseTimestampTextInZone(text, target.Timestamp.Unit, target.Timestamp.AdjustedToUTC, zone)
			return ScalarValue{Type: target, Valid: true, Int64: raw, TimeZoneName: zone}, err
		}
	}
	if value.Type.ID == types.DateType && target.ID == types.TimestampType && target.Timestamp.AdjustedToUTC && semantics.DateInfinity(value.Int64) == 0 {
		tm := time.Unix(value.Int64*86400, 0).UTC()
		raw, err := semantics.ParseTimestampTextInZone(semantics.FormatCalendarDate(tm), target.Timestamp.Unit, true, zone)
		return ScalarValue{Type: target, Valid: true, Int64: raw, TimeZoneName: zone}, err
	}
	return castScalarWithDecimalRescaling(value, target)
}

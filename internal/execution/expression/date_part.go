package expression

import (
	"fmt"
	"time"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/semantics"
)

func datePartScalar(resultType types.LogicalType, part string, value ScalarValue) (ScalarValue, error) {
	if (value.Type.ID == types.DateType && semantics.DateInfinity(value.Int64) != 0) || (value.Type.ID == types.TimestampType && semantics.TimestampInfinity(value.Int64) != 0) {
		return ScalarValue{Type: resultType, Valid: false}, nil
	}
	part, err := semantics.NormalizeDatePart(part)
	if err != nil {
		return ScalarValue{}, err
	}
	// DuckDB 2.0's scalar date_part overload extracts TIMESTAMP_NS through a
	// microsecond timestamp, rounding halves away from zero before extraction.
	if value.Type.ID == types.TimestampType && value.Type.Timestamp.Unit == types.TimestampNanos {
		q, r := value.Int64/1000, value.Int64%1000
		if r >= 500 {
			q++
		} else if r <= -500 {
			q--
		}
		value.Int64 = q
		value.Type.Timestamp.Unit = types.TimestampMicros
	}
	tm := temporalTime(value)
	year, week := tm.ISOWeek()
	out := ScalarValue{Type: resultType, Valid: true}
	var integer int64
	switch part {
	case "year":
		integer = int64(tm.Year())
	case "month":
		integer = int64(tm.Month())
	case "day":
		integer = int64(tm.Day())
	case "decade":
		integer = int64(tm.Year() / 10)
	case "century":
		integer = int64((tm.Year()-1)/100 + 1)
		if tm.Year() <= 0 {
			integer = int64(tm.Year()/100 - 1)
		}
	case "millennium":
		integer = int64((tm.Year()-1)/1000 + 1)
		if tm.Year() <= 0 {
			integer = int64(tm.Year()/1000 - 1)
		}
	case "quarter":
		integer = int64((tm.Month()-1)/3 + 1)
	case "hour":
		integer = int64(tm.Hour())
	case "minute":
		integer = int64(tm.Minute())
	case "second":
		integer = int64(tm.Second())
	case "microsecond":
		integer = int64(tm.Second())*1_000_000 + int64(tm.Nanosecond()/1000)
	case "millisecond":
		integer = int64(tm.Second())*1000 + int64(tm.Nanosecond()/1_000_000)
	case "dayofweek":
		integer = int64(tm.Weekday())
	case "isodow":
		integer = (int64(tm.Weekday())+6)%7 + 1
	case "dayofyear":
		integer = int64(tm.YearDay())
	case "week":
		integer = int64(week)
	case "isoyear":
		integer = int64(year)
	case "yearweek":
		integer = int64(year*100 + week)
		if year < 0 {
			integer = int64(year*100 - week)
		}
	case "era":
		if tm.Year() > 0 {
			integer = 1
		}
	case "timezone", "timezone_hour", "timezone_minute":
		if value.Type.ID == types.DateType {
			return ScalarValue{}, fmt.Errorf("date_part DATE timezone units are unsupported")
		}
		_, offset := tm.Zone()
		integer = int64(offset)
		if part == "timezone_hour" {
			integer /= 3600
		}
		if part == "timezone_minute" {
			integer = (integer % 3600) / 60
		}
	case "epoch":
		if value.Type.ID == types.DateType {
			out.Float64 = float64(value.Int64) * 86400
		} else {
			units, err := timestampUnitsPerSecond(value.Type.Timestamp.Unit)
			if err != nil {
				return ScalarValue{}, err
			}
			out.Float64 = float64(value.Int64) / float64(units)
		}
		return out, nil
	case "julian":
		midnight := time.Date(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0, time.UTC).Unix()
		days := midnight / 86400
		if midnight%86400 < 0 {
			days--
		}
		micros := int64(tm.Hour()*3600+tm.Minute()*60+tm.Second())*1_000_000 + int64(tm.Nanosecond()/1000)
		out.Float64 = float64(days+2440588) + float64(micros)/86_400_000_000
		return out, nil
	default:
		return ScalarValue{}, fmt.Errorf("cannot parse date_part specifier %q", part)
	}
	out.Int64 = integer
	if resultType.ID == types.Double {
		out.Float64 = float64(integer)
	}
	return out, nil
}

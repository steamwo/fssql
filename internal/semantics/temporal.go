package semantics

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/steamwo/fssql/internal/core/types"
)

// ParseDateLiteral parses the canonical DATE text used by CAST and returns days
// since 1970-01-01.
func ParseDateLiteral(text string) (int64,error) {
 if sign,ok:=TemporalInfinity(text);ok{if sign<0{return math.MinInt32+1,nil};return math.MaxInt32,nil}
 tm,_,_,err:=calendarTimestamp(strings.TrimSpace(text));if err!=nil{return 0,err}
 // DATE retains the written date even when 24:00:00 normalizes to tomorrow.
 m:=timestampText.FindStringSubmatch(strings.TrimSpace(text));if m[5]=="24"{tm=tm.Add(-24*time.Hour)}
 days:=tm.Unix()/86400;if tm.Unix()%86400<0{days--};if days<=math.MinInt32+1||days>=math.MaxInt32{return 0,fmt.Errorf("cannot parse DATE: value exceeds range")};return days,nil
}

// ParseTimestampLiteral parses a TIMESTAMP WITHOUT TIME ZONE literal.
func ParseTimestampLiteral(text string, unit types.TimestampUnit) (int64, error) {
	return ParseTimestampText(text, unit, false)
}

// ConvertTimestampUnit converts a raw whole-second timestamp between engine
// units, truncating toward negative infinity for sub-second precision loss.
func ConvertTimestampUnit(raw int64, from, to types.TimestampUnit) (int64, error) {
 if TimestampInfinity(raw)!=0{return raw,nil}
	fromUnits, err := timestampUnitsPerSecond(from)
	if err != nil {
		return 0, err
	}
	toUnits, err := timestampUnitsPerSecond(to)
	if err != nil {
		return 0, err
	}
	if fromUnits == toUnits {
		return raw, nil
	}
	if toUnits > fromUnits {
		return checkedMulScale(raw, toUnits/fromUnits)
	}
	return floorDiv(raw, fromUnits/toUnits), nil
}

func timestampUnitsPerSecond(unit types.TimestampUnit) (int64, error) {
	switch unit {
	case types.TimestampMillis:
		return 1000, nil
	case types.TimestampMicros:
		return 1000000, nil
	case types.TimestampNanos:
		return 1000000000, nil
	default:
		return 0, fmt.Errorf("invalid timestamp unit %d", unit)
	}
}

func checkedMulScale(value, factor int64) (int64, error) {
	if factor <= 0 {
		return 0, fmt.Errorf("invalid timestamp scale")
	}
	if value > 0 && value > math.MaxInt64/factor {
		return 0, fmt.Errorf("TIMESTAMP overflow")
	}
	if value < 0 && value < math.MinInt64/factor {
		return 0, fmt.Errorf("TIMESTAMP overflow")
	}
	return value * factor, nil
}

func floorDiv(value, divisor int64) int64 {
	quotient := value / divisor
	if value%divisor != 0 && value < 0 {
		quotient--
	}
	return quotient
}

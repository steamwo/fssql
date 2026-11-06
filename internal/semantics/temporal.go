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
func ParseDateLiteral(text string) (int64, error) {
	value, err := time.Parse("2006-01-02", strings.TrimSpace(text))
	if err != nil {
		return 0, fmt.Errorf("invalid DATE %q", text)
	}
	return value.Unix() / 86400, nil
}

// ParseTimestampLiteral parses the canonical TIMESTAMP text used by CAST and
// returns the raw value in unit. It intentionally matches parseScalarText.
func ParseTimestampLiteral(text string, unit types.TimestampUnit) (int64, error) {
	value, err := time.ParseInLocation("2006-01-02 15:04:05.999999999", strings.TrimSpace(text), time.UTC)
	if err != nil {
		return 0, fmt.Errorf("invalid TIMESTAMP %q", text)
	}
	switch unit {
	case types.TimestampNanos:
		return value.UnixNano(), nil
	case types.TimestampMillis:
		return value.UnixMilli(), nil
	case types.TimestampMicros:
		return value.UnixMicro(), nil
	default:
		return 0, fmt.Errorf("invalid timestamp unit %d", unit)
	}
}

// ConvertTimestampUnit converts a raw whole-second timestamp between engine
// units, truncating toward negative infinity for sub-second precision loss.
func ConvertTimestampUnit(raw int64, from, to types.TimestampUnit) (int64, error) {
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

package semantics

import (
	"fmt"
	"strings"
)

// NormalizeDatePart shares specifier validation between binding and execution.
func NormalizeDatePart(part string) (string, error) {
	switch strings.ToLower(part) {
	case "year", "years", "y", "yr", "yrs":
		return "year", nil
	case "month", "months", "mon", "mons":
		return "month", nil
	case "day", "days", "d", "dayofmonth":
		return "day", nil
	case "decade", "decades", "dec", "decs":
		return "decade", nil
	case "century", "centuries", "cent", "c":
		return "century", nil
	case "millennium", "millenniums", "millennia", "mil", "mils", "millenium":
		return "millennium", nil
	case "quarter", "quarters":
		return "quarter", nil
	case "hour", "hours", "hr", "hrs", "h":
		return "hour", nil
	case "minute", "minutes", "min", "mins", "m":
		return "minute", nil
	case "second", "seconds", "sec", "secs", "s":
		return "second", nil
	case "microsecond", "microseconds", "us", "usec", "usecs", "usecond", "useconds":
		return "microsecond", nil
	case "millisecond", "milliseconds", "ms", "msec", "msecs", "msecond", "mseconds":
		return "millisecond", nil
	case "dayofweek", "weekday", "dow":
		return "dayofweek", nil
	case "isodow":
		return "isodow", nil
	case "dayofyear", "doy":
		return "dayofyear", nil
	case "week", "weeks", "w", "weekofyear":
		return "week", nil
	case "isoyear":
		return "isoyear", nil
	case "yearweek":
		return "yearweek", nil
	case "era":
		return "era", nil
	case "timezone", "timezone_hour", "timezone_minute":
		return strings.ToLower(part), nil
	case "epoch":
		return "epoch", nil
	case "julian":
		return "julian", nil
	default:
		return "", fmt.Errorf("cannot parse date_part specifier %q", part)
	}
}

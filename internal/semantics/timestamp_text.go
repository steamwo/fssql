package semantics

import (
	"fmt"
	"github.com/steamwo/fssql/internal/core/types"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"
)

var timestampText = regexp.MustCompile(`^([0-9]{1,7})-([0-9]{1,2})-([0-9]{1,2})(?: (\(BC\)))?(?:[ T]([0-9]{2}):([0-9]{2})(?::([0-9]{2})(?:\.([0-9]+))?)?(?: ?(Z|[+-][0-9]{2}(?:[0-9]{2}|:[0-9]{2}(?::[0-9]{2})?)?|[A-Za-z_][A-Za-z0-9_/+\-]*))?)?$`)

func TemporalInfinity(text string) (int, bool) {
	switch strings.ToLower(strings.TrimSpace(text)) {
	case "infinity", "+infinity":
		return 1, true
	case "-infinity":
		return -1, true
	}
	return 0, false
}
func TimestampInfinity(raw int64) int {
	if raw == math.MaxInt64 {
		return 1
	}
	if raw == math.MinInt64+1 {
		return -1
	}
	return 0
}
func DateInfinity(raw int64) int {
	if raw == math.MaxInt32 {
		return 1
	}
	if raw == math.MinInt32+1 {
		return -1
	}
	return 0
}
func InfinityText(sign int) string {
	if sign < 0 {
		return "-infinity"
	}
	return "infinity"
}

func calendarTimestamp(text string) (time.Time, int, string, error) {
	m := timestampText.FindStringSubmatch(strings.TrimSpace(text))
	if m == nil {
		return time.Time{}, 0, "", fmt.Errorf("cannot parse TIMESTAMP %q", text)
	}
	number := func(s string) int { n, _ := strconv.Atoi(s); return n }
	year, month, day := number(m[1]), number(m[2]), number(m[3])
	if m[4] != "" {
		if year == 0 {
			return time.Time{}, 0, "", fmt.Errorf("cannot parse BC year zero")
		}
		year = 1 - year
	}
	date := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	if date.Year() != year || int(date.Month()) != month || date.Day() != day {
		return time.Time{}, 0, "", fmt.Errorf("cannot parse TIMESTAMP %q", text)
	}
	hour, minute, second := number(m[5]), number(m[6]), number(m[7])
	fraction := m[8]
	if hour > 24 || minute > 59 || second > 59 || (hour == 24 && (minute != 0 || second != 0 || strings.Trim(fraction, "0") != "")) {
		return time.Time{}, 0, "", fmt.Errorf("cannot parse TIMESTAMP %q", text)
	}
	if len(fraction) > 9 {
		fraction = fraction[:9]
	}
	nanos := number(fraction + strings.Repeat("0", 9-len(fraction)))
	return date.Add(time.Duration(hour*3600+minute*60+second) * time.Second), nanos, m[9], nil
}

// ParseTimestampText is shared by execution and literal predicate translation.
func ParseTimestampText(text string, unit types.TimestampUnit, aware bool) (int64, error) {
	return ParseTimestampTextInZone(text, unit, aware, "UTC")
}
func ParseTimestampTextInZone(text string, unit types.TimestampUnit, aware bool, defaultZone string) (int64, error) {
	if sign, ok := TemporalInfinity(text); ok {
		if sign < 0 {
			return math.MinInt64 + 1, nil
		}
		return math.MaxInt64, nil
	}
	tm, nanos, zone, err := calendarTimestamp(text)
	if err != nil {
		return 0, err
	}
	seconds := tm.Unix()
	if aware {
		if zone == "" {
			zone = defaultZone
		}
		if zone == "" {
			zone = "UTC"
		}
		if zone[0] == '+' || zone[0] == '-' {
			digits := strings.ReplaceAll(zone[1:], ":", "")
			number := func(s string) int64 { v, _ := strconv.ParseInt(s, 10, 64); return v }
			offset := number(digits[:2]) * 3600
			if len(digits) >= 4 {
				offset += number(digits[2:4]) * 60
			}
			if len(digits) == 6 {
				offset += number(digits[4:])
			}
			if zone[0] == '-' {
				offset = -offset
			}
			seconds -= offset
		} else if zone != "Z" && zone != "UTC" {
			loc, err := time.LoadLocation(zone)
			if err != nil {
				return 0, fmt.Errorf("cannot parse TIMESTAMP timezone %q", zone)
			}
			local := time.Date(tm.Year(), tm.Month(), tm.Day(), tm.Hour(), tm.Minute(), tm.Second(), 0, loc)
			// Prefer the later occurrence of an ambiguous local time, and normalize
			// a spring-forward gap using the pre-transition offset, as DuckDB does.
			offsets := map[int]bool{}
			for _, delta := range []time.Duration{-24 * time.Hour, 0, 24 * time.Hour} {
				_, off := local.Add(delta).Zone()
				offsets[off] = true
			}
			best := int64(math.MinInt64)
			exact := false
			for off := range offsets {
				candidate := tm.Unix() - int64(off)
				wall := time.Unix(candidate, 0).In(loc)
				matches := wall.Year() == tm.Year() && wall.Month() == tm.Month() && wall.Day() == tm.Day() && wall.Hour() == tm.Hour() && wall.Minute() == tm.Minute() && wall.Second() == tm.Second()
				if matches && !exact {
					best = candidate
					exact = true
				} else if matches == exact && candidate > best {
					best = candidate
				}
			}
			seconds = best
		}
	}
	units, err := timestampUnitsPerSecond(unit)
	if err != nil {
		return 0, err
	}
	raw := new(big.Int).Mul(big.NewInt(seconds), big.NewInt(units))
	raw.Add(raw, big.NewInt(int64(nanos)/(1_000_000_000/units)))
	if !raw.IsInt64() || TimestampInfinity(raw.Int64()) != 0 {
		return 0, fmt.Errorf("cannot parse TIMESTAMP: value exceeds representable range")
	}
	return raw.Int64(), nil
}

func FormatCalendarDate(tm time.Time) string {
	year := tm.Year()
	suffix := ""
	if year <= 0 {
		year = 1 - year
		suffix = " (BC)"
	}
	return fmt.Sprintf("%04d-%02d-%02d%s", year, tm.Month(), tm.Day(), suffix)
}
func FormatTimestamp(raw int64, info types.TimestampInfo, zone string) (string, error) {
	if sign := TimestampInfinity(raw); sign != 0 {
		return InfinityText(sign), nil
	}
	units, err := timestampUnitsPerSecond(info.Unit)
	if err != nil {
		return "", err
	}
	tm := time.Unix(raw/units, (raw%units)*(1_000_000_000/units)).UTC()
	if info.AdjustedToUTC && zone != "" && zone != "UTC" {
		loc, err := time.LoadLocation(zone)
		if err != nil {
			return "", err
		}
		tm = tm.In(loc)
	}
	result := FormatCalendarDate(tm) + tm.Format(" 15:04:05")
	if tm.Nanosecond() != 0 {
		result += "." + strings.TrimRight(fmt.Sprintf("%09d", tm.Nanosecond()), "0")
	}
	if info.AdjustedToUTC {
		_, offset := tm.Zone()
		sign := "+"
		if offset < 0 {
			sign = "-"
			offset = -offset
		}
		result += fmt.Sprintf("%s%02d", sign, offset/3600)
		if offset%3600 != 0 {
			result += fmt.Sprintf(":%02d", offset%3600/60)
			if offset%60 != 0 {
				result += fmt.Sprintf(":%02d", offset%60)
			}
		}
	}
	return result, nil
}

package semantics

import (
	"fmt"
	"strings"
	"time"
)

// ParseTimeFormat covers the calendar/clock directives shared by CSV and SQL
// strptime. Unknown directives fail instead of being treated as literal text.
func TimeFormatLayout(format string) (string, error) {
	layout := strings.Builder{}
	tokens := map[string]string{"Y": "2006", "y": "06", "m": "01", "d": "02", "H": "15", "M": "04", "S": "05", "I": "03", "p": "PM", "b": "Jan", "B": "January", "a": "Mon", "A": "Monday", "j": "002", "z": "-0700", "Z": "MST", "f": "999999999", "g": "999", "%": "%", "-m": "1", "-d": "2", "-H": "15", "-I": "3", "-j": "2"}
	for i := 0; i < len(format); i++ {
		if format[i] != '%' {
			layout.WriteByte(format[i])
			continue
		}
		i++
		if i >= len(format) {
			return "", fmt.Errorf("incomplete date format")
		}
		key := string(format[i])
		if format[i] == '-' {
			i++
			if i >= len(format) {
				return "", fmt.Errorf("incomplete date format")
			}
			key += "" + string(format[i])
		}
		s, ok := tokens[key]
		if !ok {
			return "", fmt.Errorf("unsupported date format directive %%%s", key)
		}
		layout.WriteString(s)
	}
	return layout.String(), nil
}
func ParseTimeFormat(text, format string) (time.Time, error) {
	layout, err := TimeFormatLayout(format)
	if err != nil {
		return time.Time{}, err
	}
	value, err := time.ParseInLocation(layout, text, time.UTC)
	if err != nil {
		return time.Time{}, fmt.Errorf("cannot parse date/time format: %w", err)
	}
	if strings.Contains(format, "%Z") {
		name, _ := value.Zone()
		if name != "UTC" && name != "GMT" {
			return time.Time{}, fmt.Errorf("date format timezone abbreviation %q is unsupported", name)
		}
	}
	return value, nil
}

func TimestampTextHasZone(text string) bool {
	m := timestampText.FindStringSubmatch(strings.TrimSpace(text))
	return m != nil && m[9] != ""
}
func TimestampTextHasClock(text string) bool {
	m := timestampText.FindStringSubmatch(strings.TrimSpace(text))
	return m != nil && m[5] != ""
}

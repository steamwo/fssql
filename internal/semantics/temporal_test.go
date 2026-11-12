package semantics

import (
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/types"
)

func TestParseTemporalLiterals(t *testing.T) {
	wantTime := time.Date(2024, 7, 1, 2, 3, 4, 500000000, time.UTC)
	days, err := ParseDateLiteral("2024-07-01")
	if err != nil || days != time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC).Unix()/86400 {
		t.Fatalf("date=%d err=%v", days, err)
	}
	for _, tc := range []struct {
		unit types.TimestampUnit
		want int64
	}{
		{types.TimestampMicros, wantTime.UnixMicro()},
		{types.TimestampMillis, wantTime.UnixMilli()},
		{types.TimestampNanos, wantTime.UnixNano()},
	} {
		got, err := ParseTimestampLiteral("2024-07-01 02:03:04.5", tc.unit)
		if err != nil || got != tc.want {
			t.Fatalf("unit=%d got=%d want=%d err=%v", tc.unit, got, tc.want, err)
		}
	}
	if _, err := ParseDateLiteral("2024-13-01"); err == nil {
		t.Fatal("accepted invalid DATE")
	}
	if _, err := ParseTimestampLiteral("not-a-time", types.TimestampMicros); err == nil {
		t.Fatal("accepted invalid TIMESTAMP")
	}
}

func TestConvertTimestampUnit(t *testing.T) {
	if got, err := ConvertTimestampUnit(1_500_000, types.TimestampMicros, types.TimestampMillis); err != nil || got != 1500 {
		t.Fatalf("micros->millis=%d err=%v", got, err)
	}
	if got, err := ConvertTimestampUnit(2, types.TimestampMillis, types.TimestampMicros); err != nil || got != 2000 {
		t.Fatalf("millis->micros=%d err=%v", got, err)
	}
	if got, err := ConvertTimestampUnit(-1500, types.TimestampMicros, types.TimestampMillis); err != nil || got != -2 {
		t.Fatalf("negative floor=%d err=%v", got, err)
	}
	if _, err := ConvertTimestampUnit(1, types.TimestampUnit(99), types.TimestampMicros); err == nil {
		t.Fatal("accepted invalid source unit")
	}
}

func TestParseTimestampTextOffsetsAndBounds(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 123456789, time.UTC)
	for _, tc := range []struct {
		text   string
		offset int64
	}{
		{"2024-01-02T03:04:05.123456789Z", 0},
		{"2024-01-02 03:04:05.123456789+08", 8 * 3600},
		{"2024-01-02 03:04:05.123456789-0530", -5*3600 - 30*60},
		{"2024-01-02 03:04:05.123456789+08:30:15", 8*3600 + 30*60 + 15},
		{"2024-01-02 03:04:05.123456789 UTC", 0},
	} {
		for _, aware := range []bool{false, true} {
			want := base.UnixNano()
			if aware {
				want -= tc.offset * 1_000_000_000
			}
			got, err := ParseTimestampText(tc.text, types.TimestampNanos, aware)
			if err != nil || got != want {
				t.Fatalf("%q aware=%v: got=%d want=%d err=%v", tc.text, aware, got, want, err)
			}
		}
	}
	for _, text := range []string{"2263-01-01", "1670-01-01", "2262-04-11 23:47:16.854775807", "1677-09-21 00:12:43.145224193", "2024-02-30", "2024-01-02T24:00:00.0001Z", "2024-01-02 03:04:05+080030", "2024-01-02t03:04:05z"} {
		if _, err := ParseTimestampText(text, types.TimestampNanos, true); err == nil {
			t.Fatalf("accepted %q", text)
		}
	}
	for _, tc := range []struct {
		text string
		want int64
	}{
		{"1969-12-31T23:59:59.999999999999Z", -1},
		{"2024-01-02", time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC).UnixMicro()},
		{"2024-01-02T03:04Z", time.Date(2024, 1, 2, 3, 4, 0, 0, time.UTC).UnixMicro()},
		{"2024-01-02 24:00:00", time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC).UnixMicro()},
	} {
		got, err := ParseTimestampText(tc.text, types.TimestampMicros, false)
		if err != nil || got != tc.want {
			t.Fatalf("%q: got=%d want=%d err=%v", tc.text, got, tc.want, err)
		}
	}
}

func TestParseDateTimestampText(t *testing.T) {
	want := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC).Unix() / 86400
	for _, text := range []string{"2024-01-02T03:04:05.123456789+08:30", "2024-01-02 24:00:00"} {
		got, err := ParseDateLiteral(text)
		if err != nil || got != want {
			t.Fatalf("%s: %d %v", text, got, err)
		}
	}
	if _, err := ParseDateLiteral("2024-01-02 bad"); err == nil {
		t.Fatal("accepted invalid clock")
	}
}

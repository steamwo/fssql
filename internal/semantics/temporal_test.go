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

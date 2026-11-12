package csv

import (
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/semantics"
)

type inferenceCandidate struct {
	typ    types.LogicalType
	format string
}

func inferenceCandidates(opts InferenceOptions) []inferenceCandidate {
	ts := opts.Candidates
	if ts == nil {
		ts = []types.LogicalType{{ID: types.Boolean}, {ID: types.BigInt}, {ID: types.Double}, {ID: types.DateType}, types.TimestampTZType(), {ID: types.TimestampType}}
	}
	ts = append(append([]types.LogicalType(nil), ts...), types.LogicalType{ID: types.Varchar})
	rank := func(t types.LogicalType) int {
		switch t.ID {
		case types.Boolean:
			return 0
		case types.TinyInt:
			return 1
		case types.SmallInt:
			return 2
		case types.Integer:
			return 3
		case types.BigInt:
			return 4
		case types.HugeInt:
			return 5
		case types.Float:
			return 6
		case types.Double:
			return 7
		case types.Decimal:
			return 8
		case types.DateType:
			return 9
		case types.TimestampType:
			if t.Timestamp.AdjustedToUTC {
				return 10
			}
			return 11
		default:
			return 100
		}
	}
	sort.SliceStable(ts, func(i, j int) bool { return rank(ts[i]) < rank(ts[j]) })
	out := []inferenceCandidate{}
	for _, t := range ts {
		formats := []string{""}
		if t.ID == types.DateType {
			if opts.DateFormat != "" {
				formats = []string{opts.DateFormat}

			}
		}
		if t.ID == types.TimestampType {
			if opts.TimestampFormat != "" {
				formats = []string{opts.TimestampFormat}

			}
		}
		for _, f := range formats {
			out = append(out, inferenceCandidate{t, f})
		}
	}
	return out
}
func candidateAccepts(raw []byte, c inferenceCandidate) bool {
	s := strings.TrimSpace(string(raw))
	t := c.typ
	if t.ID == types.Varchar {
		return true
	}
	if t.ID == types.DateType || t.ID == types.TimestampType {
		if c.format != "" {
			if t.ID == types.TimestampType && t.Timestamp.AdjustedToUTC && !strings.Contains(c.format, "%z") && !strings.Contains(c.format, "%Z") && !strings.HasSuffix(c.format, "Z") {
				return false
			}
			_, err := semantics.ParseTimeFormat(s, c.format)
			return err == nil
		}
		if t.ID == types.DateType {
			if semantics.TimestampTextHasClock(s) {
				return false
			}
			_, err := semantics.ParseDateLiteral(s)
			return err == nil
		}
		if t.Timestamp.AdjustedToUTC && !semantics.TimestampTextHasZone(s) {
			return false
		}
		_, err := semantics.ParseTimestampText(s, t.Timestamp.Unit, t.Timestamp.AdjustedToUTC)
		return err == nil
	}
	if t.ID == types.Boolean {
		_, ok := parseBool(raw)
		return ok
	}
	if t.ID == types.Decimal {
		_, err := types.ParseDecimalText(s, t.Decimal.Width, t.Decimal.Scale)
		return err == nil
	}
	if t.ID == types.HugeInt {
		n, ok := new(big.Int).SetString(s, 10)
		if !ok {
			return false
		}
		_, ok = types.Int128FromBig(n)
		return ok
	}
	if t.ID == types.Float || t.ID == types.Double {
		bits := 64
		if t.ID == types.Float {
			bits = 32
		}
		v, err := strconv.ParseFloat(s, bits)
		return err == nil && !math.IsInf(v, 0)
	}
	bits := 64
	switch t.ID {
	case types.TinyInt:
		bits = 8
	case types.SmallInt:
		bits = 16
	case types.Integer:
		bits = 32
	}
	if t.IsUnsignedInteger() {
		_, err := strconv.ParseUint(s, 10, bits)
		return err == nil
	}
	_, err := strconv.ParseInt(s, 10, bits)
	return err == nil
}

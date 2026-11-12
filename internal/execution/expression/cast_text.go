package expression

import (
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

var decimalIntegerText = regexp.MustCompile(`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$`)

// Parse exact decimal/exponent text, rounding halves away from zero without
// routing wide integers through float64. The big.Int payload stays <=40 digits.
func parseSQLIntegerText(text string) (*big.Int, error) {
	text = strings.TrimSpace(text)
	if len(text) > 2 && text[0] == '0' && (text[1] == 'x' || text[1] == 'X' || text[1] == 'b' || text[1] == 'B') {
		base := 16
		if text[1] == 'b' || text[1] == 'B' {
			base = 2
		}
		digits := text[2:]
		if strings.HasPrefix(digits, "_") || strings.HasSuffix(digits, "_") || strings.Contains(digits, "__") {
			return nil, fmt.Errorf("cannot parse integer text")
		}
		digits = strings.ReplaceAll(digits, "_", "")
		if digits == "" {
			return nil, fmt.Errorf("cannot parse integer text")
		}
		for _, c := range digits {
			valid := c >= '0' && c <= '9'
			if base == 2 {
				valid = c == '0' || c == '1'
			} else {
				valid = valid || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
			}
			if !valid {
				return nil, fmt.Errorf("cannot parse integer text")
			}
		}
		digits = strings.TrimLeft(digits, "0")
		if digits == "" {
			return new(big.Int), nil
		}
		limit := 32
		if base == 2 {
			limit = 128
		}
		if len(digits) > limit {
			return nil, fmt.Errorf("CAST integer overflow")
		}
		n, ok := new(big.Int).SetString(digits, base)
		if !ok || n.Sign() < 0 {
			return nil, fmt.Errorf("cannot parse integer text")
		}
		if n.BitLen() > 128 {
			return nil, fmt.Errorf("CAST integer overflow")
		}
		return n, nil
	}
	if strings.Contains(text, "_") {
		for i, c := range text {
			if c == '_' && (i == 0 || i+1 == len(text) || text[i-1] < '0' || text[i-1] > '9' || text[i+1] < '0' || text[i+1] > '9') {
				return nil, fmt.Errorf("cannot parse integer text")
			}
		}
		text = strings.ReplaceAll(text, "_", "")
	}
	if !decimalIntegerText.MatchString(text) {
		return nil, fmt.Errorf("cannot parse integer text")
	}
	negative := text[0] == '-'
	if text[0] == '-' || text[0] == '+' {
		text = text[1:]
	}
	mantissa, exponentText := text, ""
	if i := strings.IndexAny(text, "eE"); i >= 0 {
		mantissa, exponentText = text[:i], text[i+1:]
	}
	fraction := int64(0)
	if i := strings.IndexByte(mantissa, '.'); i >= 0 {
		fraction = int64(len(mantissa) - i - 1)
		mantissa = mantissa[:i] + mantissa[i+1:]
	}
	digits := strings.TrimLeft(mantissa, "0")
	var exponent int64
	if exponentText != "" {
		var err error
		exponent, err = strconv.ParseInt(exponentText, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("cannot parse integer exponent")
		}
	}
	if digits == "" {
		return new(big.Int), nil
	}
	if exponentText != "" {
		bound := int64(len(text)) + 40
		if exponent > bound {
			return nil, fmt.Errorf("CAST integer overflow")
		}
		if exponent < -bound {
			return new(big.Int), nil
		}
	}
	cut := int64(len(digits)) - fraction + exponent
	if cut > 39 {
		return nil, fmt.Errorf("CAST integer overflow")
	}
	if cut < 0 {
		return new(big.Int), nil
	}
	n := new(big.Int)
	if cut > 0 {
		if cut >= int64(len(digits)) {
			n.SetString(digits+strings.Repeat("0", int(cut)-len(digits)), 10)
		} else {
			n.SetString(digits[:int(cut)], 10)
		}
	}
	if cut < int64(len(digits)) && digits[int(cut)] >= '5' {
		n.Add(n, big.NewInt(1))
	}
	if negative {
		n.Neg(n)
	}
	return n, nil
}
